//! Native stack checks for deep graphs.
//!
//! Every node an item passes through adds native frames to the emitting
//! thread's stack. A graph thousands of nodes deep, or an ordinary graph on a
//! thread with a small stack (musl's 128 KiB default, `threading.stack_size()`)
//! would overflow it and crash the interpreter. Instead, propagation raises
//! `RecursionError` once the stack is nearly used up.

use pyo3::prelude::*;
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// The main thread's usable stack range, (floor, top), once known: a stack
/// address in it can only be the main thread's, so the main thread (where
/// most graphs run) is checked without the thread-local lookup.
static MAIN_FLOOR: AtomicUsize = AtomicUsize::new(usize::MAX);
static MAIN_TOP: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The lowest stack address this thread may reach before raising; 0
    /// until first looked up, 1 where the stack bounds are unknown.
    static FLOOR: Cell<usize> = const { Cell::new(0) };
}

/// Raise `RecursionError` if the current thread's native stack is nearly
/// used up. It runs at every node an item passes, so it is cheap where most
/// graphs run: on the main thread it compares against the main thread's
/// cached range (no thread-local lookup); other threads look up their own.
#[inline(always)]
pub(crate) fn check() -> PyResult<()> {
    let here = 0u8;
    let here = std::ptr::addr_of!(here) as usize;
    if here > MAIN_FLOOR.load(Relaxed) && here < MAIN_TOP.load(Relaxed) {
        return Ok(());
    }
    check_slow(here)
}

/// Whether there is still room on the stack, for walks that can stop early
/// instead of failing (compile()'s analysis).
pub(crate) fn has_room() -> bool {
    let here = 0u8;
    check_slow(std::ptr::addr_of!(here) as usize).is_ok()
}

#[inline(never)]
fn check_slow(here: usize) -> PyResult<()> {
    let mut floor = FLOOR.with(Cell::get);
    if floor == 0 {
        floor = match stack_bounds() {
            Some((low, size)) => {
                // Keep a reserve for the frames of one more node and for
                // Python code running in callbacks: a quarter of the stack,
                // within limits.
                let floor = low + (size / 4).clamp(32 * 1024, 256 * 1024);
                if is_main_thread() {
                    // Written once; a half-written pair only fails the test.
                    MAIN_TOP.store(low + size, Relaxed);
                    MAIN_FLOOR.store(floor, Relaxed);
                }
                floor
            }
            None => 1,
        };
        FLOOR.with(|f| f.set(floor));
    }
    if here > floor {
        return Ok(());
    }
    Err(pyo3::exceptions::PyRecursionError::new_err(
        "maximum graph depth exceeded: the thread's native stack is nearly used up \
         (a graph thousands of nodes deep, a long chain of sources feeding each other, \
         or a thread with a small stack)",
    ))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn is_main_thread() -> bool {
    // SAFETY: plain queries.
    unsafe { libc::gettid() == libc::getpid() }
}

#[cfg(target_os = "macos")]
fn is_main_thread() -> bool {
    // SAFETY: a plain query.
    unsafe { libc::pthread_main_np() != 0 }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
fn is_main_thread() -> bool {
    false
}

/// (lowest address, size) of the current thread's stack.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn stack_bounds() -> Option<(usize, usize)> {
    // SAFETY: the attr is initialised by pthread_getattr_np before use and
    // destroyed after; the out-pointers are valid locals.
    unsafe {
        let mut attr: libc::pthread_attr_t = std::mem::zeroed();
        if libc::pthread_getattr_np(libc::pthread_self(), &raw mut attr) != 0 {
            return None;
        }
        let mut addr: *mut libc::c_void = std::ptr::null_mut();
        let mut size: libc::size_t = 0;
        let ok = libc::pthread_attr_getstack(&raw const attr, &raw mut addr, &raw mut size) == 0;
        libc::pthread_attr_destroy(&raw mut attr);
        ok.then_some((addr as usize, size))
    }
}

#[cfg(target_os = "macos")]
fn stack_bounds() -> Option<(usize, usize)> {
    // SAFETY: plain queries about the calling thread.
    unsafe {
        let thread = libc::pthread_self();
        let top = libc::pthread_get_stackaddr_np(thread) as usize;
        let size = libc::pthread_get_stacksize_np(thread);
        Some((top.checked_sub(size)?, size))
    }
}

#[cfg(windows)]
fn stack_bounds() -> Option<(usize, usize)> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentThreadStackLimits(low: *mut usize, high: *mut usize);
    }
    let (mut low, mut high) = (0usize, 0usize);
    // SAFETY: two valid out-pointers.
    unsafe { GetCurrentThreadStackLimits(&raw mut low, &raw mut high) };
    (high > low).then_some((low, high - low))
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    windows
)))]
fn stack_bounds() -> Option<(usize, usize)> {
    None
}
