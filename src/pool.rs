//! The thread pool behind `par()` and `prefetch()`.
//!
//! It replaces a Python `ThreadPoolExecutor`, whose per-task Future, Condition
//! and work item cost ~70,000 instructions per `par()` branch. Workers stay
//! attached to the interpreter and only detach while waiting for work, so a
//! job costs a queue push, a wake-up and a GIL hand-off.
//!
//! A task is claimed exactly once, by a worker or by the thread that submitted
//! it: a waiter first runs its own tasks that no worker has started, so nested
//! `par()`/`prefetch()` can't exhaust the pool (a waiter only sleeps on tasks
//! that are already running). It never runs other threads' tasks: it may be
//! inside a node's update, and a foreign task reaching that node would fail
//! its borrow instead of waiting for the lock.
//! Jobs run in a copy of the submitting thread's `contextvars` context.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use pyo3::prelude::*;

/// A unit of work, run on a worker (or its submitter) with the GIL held.
pub(crate) type Job = Box<dyn FnOnce(Python<'_>) + Send>;

/// Stacks, as (lowest, highest) address, of threads waiting for a task.
type Stacks = Arc<[(usize, usize)]>;

/// The threads waiting for a task: the one that submitted it, and those
/// waiting in turn for that thread's own task (nested par()/prefetch()),
/// each by its stack.
#[derive(Clone, Default)]
struct Waiting {
    submitter: (usize, usize),
    /// None unless the submitter itself runs a task for another thread.
    outer: Option<Stacks>,
}

impl Waiting {
    fn contains(&self, address: usize) -> bool {
        let within = |&(low, high): &(usize, usize)| low <= address && address < high;
        within(&self.submitter) || self.outer.as_deref().is_some_and(|o| o.iter().any(within))
    }
}

thread_local! {
    /// While a worker runs a task: the threads waiting for it.
    static RUNNING_FOR: std::cell::RefCell<Option<Waiting>> =
        const { std::cell::RefCell::new(None) };
}

/// Whether `address` is on the stack of a thread that waits for the task
/// this thread is running (see `Emitting::check_not_waited_on`).
pub(crate) fn waits_for_this_thread(address: usize) -> bool {
    RUNNING_FOR.with(|r| r.borrow().as_ref().is_some_and(|w| w.contains(address)))
}

/// A queued job that runs at most once.
pub(crate) struct Task {
    job: Mutex<Option<Job>>,
    waiting: Waiting,
}

impl Task {
    fn claim(&self) -> Option<Job> {
        self.job
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn is_claimed(&self) -> bool {
        self.job
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    }

    /// Run the job here, on the thread that submitted it, if no worker has
    /// started it.
    pub(crate) fn run_if_unclaimed(&self, py: Python) {
        if let Some(job) = self.claim() {
            job(py);
        }
    }

    /// Run the job on a worker, if its submitter hasn't, with the threads
    /// waiting for it on record.
    fn run_for_submitter(&self, py: Python) {
        /// Puts back what the worker was running for before.
        struct Restore(Option<Waiting>);
        impl Drop for Restore {
            fn drop(&mut self) {
                let outer = self.0.take();
                RUNNING_FOR.with(|r| *r.borrow_mut() = outer);
            }
        }
        if let Some(job) = self.claim() {
            let outer = RUNNING_FOR.with(|r| r.borrow_mut().replace(self.waiting.clone()));
            let _restore = Restore(outer);
            job(py);
        }
    }
}

/// Who waits for a task submitted now, from this thread.
fn waiting_for_new_task() -> Waiting {
    let outer = RUNNING_FOR.with(|r| {
        r.borrow().as_ref().map(|w| {
            // This thread runs a task: its waiters wait for this one too.
            let mut stacks = vec![w.submitter];
            stacks.extend(w.outer.as_deref().unwrap_or_default());
            Stacks::from(stacks)
        })
    });
    Waiting {
        submitter: crate::stack::range(),
        outer,
    }
}

struct Queue {
    tasks: VecDeque<Arc<Task>>,
    idle: usize,
    threads: usize,
    /// Tasks a worker is running right now.
    running: usize,
    /// The interpreter is exiting: workers take no new tasks.
    shutdown: bool,
}

struct Pool {
    generation: usize,
    queue: Mutex<Queue>,
    available: Condvar,
    /// Signalled when `running` drops to zero.
    finished: Condvar,
    max_threads: usize,
}

/// How long a waiter sleeps before re-checking for signals (Ctrl+C).
const SIGNAL_POLL: Duration = Duration::from_millis(50);

/// Bumped in a forked child: the parent's worker threads don't exist there.
static FORK_GENERATION: AtomicUsize = AtomicUsize::new(0);
/// The current pool, leaked so references stay valid: one per process, plus
/// one per fork generation in a child. A lock-free lookup, so a fork can't
/// leave it locked by a thread that doesn't exist in the child.
static POOL: AtomicPtr<Pool> = AtomicPtr::new(std::ptr::null_mut());

fn pool() -> &'static Pool {
    let generation = FORK_GENERATION.load(Ordering::Acquire);
    loop {
        let current = POOL.load(Ordering::Acquire);
        // SAFETY: pools are leaked, never freed.
        if let Some(pool) = unsafe { current.as_ref() }
            && pool.generation == generation
        {
            return pool;
        }
        let fresh = Box::into_raw(Box::new(Pool {
            generation,
            queue: Mutex::new(Queue {
                tasks: VecDeque::new(),
                idle: 0,
                threads: 0,
                running: 0,
                shutdown: false,
            }),
            available: Condvar::new(),
            finished: Condvar::new(),
            max_threads: std::thread::available_parallelism().map_or(4, usize::from),
        }));
        if POOL
            .compare_exchange(current, fresh, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // SAFETY: just leaked above.
            return unsafe { &*fresh };
        }
        // Another thread installed one first.
        // SAFETY: `fresh` was never shared.
        drop(unsafe { Box::from_raw(fresh) });
    }
}

/// Called in a forked child (registered with `os.register_at_fork`).
#[pyfunction]
pub(crate) fn _reset_pool_after_fork() {
    FORK_GENERATION.fetch_add(1, Ordering::AcqRel);
}

/// At interpreter exit (registered with `threading._register_atexit`, like
/// `concurrent.futures`): let tasks already running on workers finish, as
/// `ThreadPoolExecutor` did, and start no new ones. A thread still waiting on
/// its own queued tasks runs them itself.
#[pyfunction]
pub(crate) fn _shutdown_pool(py: Python) {
    let pool = pool();
    py.detach(|| {
        let mut q = pool.queue.lock().unwrap_or_else(PoisonError::into_inner);
        q.shutdown = true;
        while q.running > 0 {
            q = pool
                .finished
                .wait(q)
                .unwrap_or_else(PoisonError::into_inner);
        }
    });
}

/// Queue `job`, starting a worker if none is idle and the pool isn't full.
pub(crate) fn submit(job: Job) -> Arc<Task> {
    let waiting = waiting_for_new_task();
    let task = Arc::new(Task {
        job: Mutex::new(Some(job)),
        waiting,
    });
    let pool = pool();
    let mut q = pool.queue.lock().unwrap_or_else(PoisonError::into_inner);
    q.tasks.push_back(Arc::clone(&task));
    if q.idle == 0 && q.threads < pool.max_threads {
        q.threads += 1;
        let spawned = std::thread::Builder::new()
            .name("akayu-worker".into())
            // As deep as the main thread's (Rust's default is 2 MiB): par()
            // and prefetch() branches run whole subgraphs.
            .stack_size(8 * 1024 * 1024)
            .spawn(move || worker(pool));
        if spawned.is_err() {
            q.threads -= 1; // The submitter runs the task itself instead.
        }
    }
    drop(q);
    pool.available.notify_one();
    task
}

fn worker(pool: &Pool) {
    // A worker started while the interpreter shuts down can't attach: it just
    // ends. Its tasks are run by whoever waits for them (`wait_for`).
    Python::try_attach(|py| {
        loop {
            let task = py.detach(|| {
                let mut q = pool.queue.lock().unwrap_or_else(PoisonError::into_inner);
                q.idle += 1;
                loop {
                    if !q.shutdown
                        && let Some(task) = q.tasks.pop_front()
                    {
                        // A task its submitter already ran holds no Python
                        // objects: drop it here, without the GIL. Taking the
                        // GIL for each, one at a time, let them pile up while
                        // threads kept submitting. (An unclaimed one is still
                        // claimed only once the GIL is back, so its submitter
                        // can run it meanwhile, which is cheaper.)
                        if task.is_claimed() {
                            continue;
                        }
                        q.idle -= 1;
                        q.running += 1;
                        return task;
                    }
                    q = pool
                        .available
                        .wait(q)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            });
            task.run_for_submitter(py);
            let mut q = pool.queue.lock().unwrap_or_else(PoisonError::into_inner);
            q.running -= 1;
            if q.running == 0 {
                pool.finished.notify_all();
            }
        }
    });
}

/// The submitting thread's context, copied: a job must not share a context
/// object with another thread (entering one twice raises).
pub(crate) fn copy_context(py: Python) -> PyResult<Py<PyAny>> {
    // SAFETY: GIL held; returns a new reference or NULL with an exception set.
    unsafe { Bound::from_owned_ptr_or_err(py, pyo3::ffi::PyContext_CopyCurrent()) }
        .map(Bound::unbind)
}

/// Run `f` inside `ctx` (a context object no other thread is in).
pub(crate) fn run_in_context<T>(
    py: Python,
    ctx: &Py<PyAny>,
    f: impl FnOnce() -> PyResult<T>,
) -> PyResult<T> {
    // SAFETY: GIL held; ctx is a context object (from copy_context).
    if unsafe { pyo3::ffi::PyContext_Enter(ctx.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    let result = f();
    // SAFETY: as above; exits the context entered just before.
    if unsafe { pyo3::ffi::PyContext_Exit(ctx.as_ptr()) } != 0 {
        return Err(PyErr::fetch(py));
    }
    result
}

/// Something a thread can wait on: becomes ready exactly once.
pub(crate) trait Waitable {
    fn is_ready(&self) -> bool;
    /// Sleep until ready or `timeout` (GIL released by the caller).
    fn wait_timeout(&self, timeout: Duration);
}

/// Wait for `target`, first running any of `own` (tasks this thread
/// submitted) that no worker has started. Signals (Ctrl+C) interrupt the
/// wait with their error.
pub(crate) fn wait_for(
    py: Python,
    target: &(impl Waitable + Sync),
    own: &[Arc<Task>],
) -> PyResult<()> {
    for task in own {
        if target.is_ready() {
            return Ok(());
        }
        task.run_if_unclaimed(py);
    }
    while !target.is_ready() {
        py.detach(|| target.wait_timeout(SIGNAL_POLL));
        py.check_signals()?;
    }
    Ok(())
}

/// Run `f`, turning a panic into a Python exception: a job must always report
/// back, or its waiter would wait forever.
pub(crate) fn catch_panic<T>(f: impl FnOnce() -> PyResult<T>) -> PyResult<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or_else(|payload| {
        let msg = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panic in akayu worker".to_string());
        Err(pyo3::panic::PanicException::new_err(msg))
    })
}

/// A result filled in by one job.
pub(crate) struct Slot<T> {
    value: Mutex<Option<PyResult<T>>>,
    done: Condvar,
}

impl<T> Slot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            done: Condvar::new(),
        })
    }

    pub(crate) fn set(&self, value: PyResult<T>) {
        *self.value.lock().unwrap_or_else(PoisonError::into_inner) = Some(value);
        self.done.notify_all();
    }

    /// The result, once `wait_helping` has returned for this slot.
    pub(crate) fn take(&self) -> PyResult<T> {
        self.value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .expect("slot taken before it was filled")
    }
}

impl<T> Waitable for Slot<T> {
    fn is_ready(&self) -> bool {
        self.value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn wait_timeout(&self, timeout: Duration) {
        let guard = self.value.lock().unwrap_or_else(PoisonError::into_inner);
        if guard.is_none() {
            drop(self.done.wait_timeout(guard, timeout));
        }
    }
}

/// Counts down outstanding jobs; keeps the error of the lowest-numbered job.
pub(crate) struct Latch {
    state: Mutex<(usize, Option<(usize, PyErr)>)>,
    done: Condvar,
}

impl Latch {
    pub(crate) fn new(count: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((count, None)),
            done: Condvar::new(),
        })
    }

    /// Job `index` finished with `result`.
    pub(crate) fn count_down(&self, index: usize, result: PyResult<()>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.0 -= 1;
        if let Err(e) = result
            && state.1.as_ref().is_none_or(|(first, _)| index < *first)
        {
            state.1 = Some((index, e));
        }
        if state.0 == 0 {
            self.done.notify_all();
        }
    }

    /// The lowest-numbered job's error, once the count has reached zero.
    pub(crate) fn take_error(&self) -> Option<PyErr> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.1.take().map(|(_, e)| e)
    }
}

impl Waitable for Latch {
    fn is_ready(&self) -> bool {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).0 == 0
    }

    fn wait_timeout(&self, timeout: Duration) {
        let guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if guard.0 != 0 {
            drop(self.done.wait_timeout(guard, timeout));
        }
    }
}
