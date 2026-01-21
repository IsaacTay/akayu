use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use std::collections::VecDeque;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

static IS_AWAITABLE: OnceLock<Py<PyAny>> = OnceLock::new();
static PROCESS_ASYNC: OnceLock<Py<PyAny>> = OnceLock::new();
static GATHER: OnceLock<Py<PyAny>> = OnceLock::new();
static FILTER_ASYNC: OnceLock<Py<PyAny>> = OnceLock::new();

fn get_is_awaitable() -> PyResult<&'static Py<PyAny>> {
    IS_AWAITABLE.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_process_async() -> PyResult<&'static Py<PyAny>> {
    PROCESS_ASYNC.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_gather() -> PyResult<&'static Py<PyAny>> {
    GATHER.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn get_filter_async() -> PyResult<&'static Py<PyAny>> {
    FILTER_ASYNC.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("rstreamz not initialized")
    })
}

fn extract_stream_name(
    py: Python,
    kwargs: &Option<Py<PyDict>>,
    default_suffix: &str,
    parent_name: &str,
) -> PyResult<String> {
    if let Some(k) = kwargs {
        let k_bound = k.bind(py);
        if let Some(item) = k_bound.get_item("stream_name")? {
            let name = item.extract::<String>()?;
            k_bound.del_item("stream_name")?;
            return Ok(name);
        }
    }
    Ok(format!("{}.{}", parent_name, default_suffix))
}

fn wrap_error<T>(py: Python, res: PyResult<T>, node_name: &str) -> PyResult<T> {
    res.map_err(|e| {
        let ctx_msg = format!("Stream operation failed at node '{}'", node_name);
        let new_err = pyo3::exceptions::PyRuntimeError::new_err(ctx_msg);
        new_err.set_cause(py, Some(e));
        new_err
    })
}

#[pyfunction]
#[pyo3(signature = (data, path))]
fn to_text_file(py: Python, data: Py<PyAny>, path: String) -> PyResult<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    let s = data.bind(py).str()?;
    let output = s.to_string();

    writeln!(file, "{}", output)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (path, interval=None))]
fn from_text_file(py: Python, path: String, interval: Option<f64>) -> PyResult<Py<Stream>> {
    let stream = Py::new(py, Stream::new(Some("file_source".to_string()), None))?;
    let stream_clone = stream.clone_ref(py);

    thread::spawn(move || {
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Error opening file {}: {}", path, e);
                return;
            }
        };
        let reader = BufReader::new(file);

        for text in reader.lines().map_while(Result::ok) {
            Python::attach(|py| {
                if let Ok(mut s) = stream_clone.try_borrow_mut(py) {
                    let py_str = PyString::new(py, &text);
                    let _ = s.emit(py, py_str.into());
                }
            });

            if let Some(delay) = interval {
                thread::sleep(Duration::from_secs_f64(delay));
            }
        }
    });

    Ok(stream)
}

enum NodeLogic {
    Source,
    Map {
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    },
    Starmap {
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    },
    Filter {
        predicate: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    },
    Flatten,
    Collect {
        buffer: Vec<Py<PyAny>>,
    },
    Sink {
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    },
    Accumulate {
        func: Py<PyAny>,
        state: Py<PyAny>,
    },
    Tag {
        index: usize,
    },
    CombineLatest {
        state: Vec<Option<Py<PyAny>>>,
    },
    Zip {
        buffers: Vec<VecDeque<Py<PyAny>>>,
    },
}

#[pyclass(subclass)]
struct Stream {
    logic: NodeLogic,
    downstreams: Vec<Py<Stream>>,
    name: String,
    asynchronous: Option<bool>,
}

const HELPERS: &str = r#"
import asyncio
import inspect

async def _process_async(awaitable, downstreams):
    val = await awaitable
    results = []
    for d in downstreams:
        res = d.update(val)
        if res is not None:
            results.append(res)
    if results:
        await asyncio.gather(*results)

async def _gather(futures):
    await asyncio.gather(*futures)

async def _filter_async(coro, x, downstreams):
    allowed = await coro
    if allowed:
        results = []
        for d in downstreams:
            res = d.update(x)
            if res is not None:
                results.append(res)
        if results:
            await asyncio.gather(*results)
"#;

#[pymethods]
impl Stream {
    #[new]
    #[pyo3(signature = (name=None, asynchronous=None))]
    fn new(name: Option<String>, asynchronous: Option<bool>) -> Self {
        Stream {
            logic: NodeLogic::Source,
            downstreams: Vec::new(),
            name: name.unwrap_or_else(|| "source".to_string()),
            asynchronous,
        }
    }

    #[pyo3(signature = (func, *args, **kwargs))]
    fn map(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Stream>> {
        let name = extract_stream_name(py, &kwargs, "map", &self.name)?;
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Map { func, args, kwargs },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    #[pyo3(signature = (func, *args, **kwargs))]
    fn starmap(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Stream>> {
        let name = extract_stream_name(py, &kwargs, "starmap", &self.name)?;
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Starmap { func, args, kwargs },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    #[pyo3(signature = (predicate, *args, **kwargs))]
    fn filter(
        &mut self,
        py: Python,
        predicate: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Stream>> {
        let name = extract_stream_name(py, &kwargs, "filter", &self.name)?;
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Filter {
                    predicate,
                    args,
                    kwargs,
                },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    fn flatten(&mut self, py: Python) -> PyResult<Py<Stream>> {
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Flatten,
                downstreams: Vec::new(),
                name: format!("{}.flatten", self.name),
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    fn collect(&mut self, py: Python) -> PyResult<Py<Stream>> {
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Collect { buffer: Vec::new() },
                downstreams: Vec::new(),
                name: format!("{}.collect", self.name),
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    fn flush(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match &mut self.logic {
            NodeLogic::Collect { buffer } => {
                let items = std::mem::take(buffer);
                let tuple = PyTuple::new(py, items)?;
                self.propagate(py, tuple.into())
            }
            _ => Ok(None),
        }
    }

    #[pyo3(signature = (func, *args, **kwargs))]
    fn sink(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
    ) -> PyResult<Py<Stream>> {
        let name = extract_stream_name(py, &kwargs, "sink", &self.name)?;
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Sink { func, args, kwargs },
                downstreams: Vec::new(),
                name,
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    fn accumulate(
        &mut self,
        py: Python,
        func: Py<PyAny>,
        start: Py<PyAny>,
    ) -> PyResult<Py<Stream>> {
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Accumulate { func, state: start },
                downstreams: Vec::new(),
                name: format!("{}.accumulate", self.name),
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));
        Ok(node)
    }

    #[pyo3(signature = (*others))]
    fn union(&mut self, py: Python, others: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Source,
                downstreams: Vec::new(),
                name: format!("{}.union", self.name),
                asynchronous: self.asynchronous,
            },
        )?;

        self.downstreams.push(node.clone_ref(py));

        for other in others {
            let mut other_ref = other.borrow_mut(py);
            other_ref.downstreams.push(node.clone_ref(py));
        }

        Ok(node)
    }

    #[pyo3(signature = (*others))]
    fn combine_latest(&mut self, py: Python, others: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
        let total_sources = 1 + others.len();
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::CombineLatest {
                    state: (0..total_sources).map(|_| None).collect(),
                },
                downstreams: Vec::new(),
                name: format!("{}.combine_latest", self.name),
                asynchronous: self.asynchronous,
            },
        )?;

        self.add_tag_node(py, 0, &node)?;
        for (i, other) in others.iter().enumerate() {
            let mut other_ref = other.borrow_mut(py);
            other_ref.add_tag_node(py, i + 1, &node)?;
        }

        Ok(node)
    }

    #[pyo3(signature = (*others))]
    fn zip(&mut self, py: Python, others: Vec<Py<Stream>>) -> PyResult<Py<Stream>> {
        let total_sources = 1 + others.len();
        let node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Zip {
                    buffers: (0..total_sources).map(|_| VecDeque::new()).collect(),
                },
                downstreams: Vec::new(),
                name: format!("{}.zip", self.name),
                asynchronous: self.asynchronous,
            },
        )?;

        self.add_tag_node(py, 0, &node)?;
        for (i, other) in others.iter().enumerate() {
            let mut other_ref = other.borrow_mut(py);
            other_ref.add_tag_node(py, i + 1, &node)?;
        }

        Ok(node)
    }

    fn emit(&mut self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        self.update(py, x)
    }

    fn emit_batch(&mut self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        self.update_batch(py, items)
    }

    fn update_batch(&mut self, py: Python, items: Vec<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        let _skip_async_check = self.asynchronous == Some(false);
        let mut output_batch = Vec::with_capacity(items.len());

        match &mut self.logic {
            NodeLogic::Source => {
                output_batch = items;
            }
            NodeLogic::Map { func, args, kwargs } => {
                let args_ref = args.bind(py);
                let kwargs_ref = kwargs.as_ref().map(|k| k.bind(py));
                let args_empty = args_ref.is_empty();
                for x in items {
                    let res = if args_empty {
                        wrap_error(py, func.call1(py, (x,)), &self.name)?
                    } else {
                        let x_tuple = PyTuple::new(py, &[x.clone_ref(py)])?;
                        let full_args = x_tuple.add(args_ref)?;
                        let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                        wrap_error(py, func.call(py, full_args_tuple, kwargs_ref), &self.name)?
                    };
                    output_batch.push(res);
                }
            }
            NodeLogic::Starmap { func, args, kwargs } => {
                let args_ref = args.bind(py);
                let kwargs_ref = kwargs.as_ref().map(|k| k.bind(py));
                for x in items {
                    let x_bound = x.bind(py);
                    let x_tuple: pyo3::Bound<'_, PyTuple> = if x_bound.is_instance_of::<PyTuple>() {
                        x_bound.clone().cast_into::<PyTuple>()?
                    } else if x_bound.is_instance_of::<PyList>() {
                        let list = x_bound.cast::<PyList>()?;
                        list.to_tuple()
                    } else {
                        let iter = x_bound.try_iter()?;
                        let elements: Result<Vec<_>, _> = iter.collect();
                        PyTuple::new(py, elements?)?
                    };
                    let full_args = x_tuple.add(args_ref)?;
                    let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                    let res =
                        wrap_error(py, func.call(py, full_args_tuple, kwargs_ref), &self.name)?;
                    output_batch.push(res);
                }
            }
            NodeLogic::Filter {
                predicate,
                args,
                kwargs,
            } => {
                let args_ref = args.bind(py);
                let kwargs_ref = kwargs.as_ref().map(|k| k.bind(py));
                let args_empty = args_ref.is_empty();
                for x in items {
                    let res = if args_empty {
                        wrap_error(
                            py,
                            predicate.call1(py, (x.clone_ref(py),)),
                            &self.name,
                        )?
                    } else {
                        let x_clone = x.clone_ref(py);
                        let x_tuple = PyTuple::new(py, &[x_clone])?;
                        let full_args = x_tuple.add(args_ref)?;
                        let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                        wrap_error(
                            py,
                            predicate.call(py, full_args_tuple, kwargs_ref),
                            &self.name,
                        )?
                    };
                    if res.is_truthy(py)? {
                        output_batch.push(x);
                    }
                }
            }
            NodeLogic::Flatten => {
                for x in items {
                    let x_bound = x.bind(py);
                    let iter = x_bound.try_iter()?;
                    for item_res in iter {
                        output_batch.push(item_res?.into());
                    }
                }
            }
            NodeLogic::Collect { buffer } => {
                buffer.extend(items);
                return Ok(None);
            }
            NodeLogic::Sink { func, args, kwargs } => {
                let args_ref = args.bind(py);
                let kwargs_ref = kwargs.as_ref().map(|k| k.bind(py));
                let args_empty = args_ref.is_empty();
                for x in items {
                    if args_empty {
                        wrap_error(py, func.call1(py, (x,)), &self.name)?;
                    } else {
                        let x_tuple = PyTuple::new(py, &[x.clone_ref(py)])?;
                        let full_args = x_tuple.add(args_ref)?;
                        let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                        wrap_error(py, func.call(py, full_args_tuple, kwargs_ref), &self.name)?;
                    }
                }
            }
            NodeLogic::Accumulate { func, state } => {
                for x in items {
                    let new_state =
                        wrap_error(py, func.call1(py, (state.clone_ref(py), x)), &self.name)?;
                    *state = new_state.clone_ref(py);
                    output_batch.push(new_state);
                }
            }
            NodeLogic::Tag { index } => {
                let idx_obj: Py<PyAny> = (*index).into_pyobject(py)?.into();
                for x in items {
                    let tuple = PyTuple::new(py, &[idx_obj.clone_ref(py), x])?;
                    output_batch.push(tuple.into());
                }
            }
            NodeLogic::CombineLatest { state } => {
                for x in items {
                    let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                    let (idx, val) = tuple;
                    if idx < state.len() {
                        state[idx] = Some(val);
                    }
                    if state.iter().all(|s| s.is_some()) {
                        let values: Vec<Py<PyAny>> = state
                            .iter()
                            .map(|s| s.as_ref().unwrap().clone_ref(py))
                            .collect();
                        let tuple_out = PyTuple::new(py, values)?;
                        output_batch.push(tuple_out.into());
                    }
                }
            }
            NodeLogic::Zip { buffers } => {
                for x in items {
                    let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                    let (idx, val) = tuple;
                    if idx < buffers.len() {
                        buffers[idx].push_back(val);
                    }
                }
                while buffers.iter().all(|buf| !buf.is_empty()) {
                    let mut values = Vec::with_capacity(buffers.len());
                    for buf in buffers.iter_mut() {
                        if let Some(v) = buf.pop_front() {
                            values.push(v);
                        }
                    }
                    let tuple_out = PyTuple::new(py, values)?;
                    output_batch.push(tuple_out.into());
                }
            }
        }

        self.propagate_batch(py, output_batch)
    }

    fn propagate_batch(
        &self,
        py: Python,
        output_batch: Vec<Py<PyAny>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        if output_batch.is_empty() {
            return Ok(None);
        }
        if self.downstreams.len() == 1 {
            let mut child_ref = self.downstreams[0].borrow_mut(py);
            return child_ref.update_batch(py, output_batch);
        } else if !self.downstreams.is_empty() {
            let mut futures = Vec::new();
            let len = self.downstreams.len();

            let mut child_refs = Vec::with_capacity(len);
            for child in &self.downstreams {
                child_refs.push(child.borrow_mut(py));
            }

            for val in output_batch {
                for child in child_refs.iter_mut().take(len - 1) {
                    let res = child.update(py, val.clone_ref(py))?;
                    if let Some(f) = res {
                        futures.push(f);
                    }
                }
                if len > 0 {
                    let res = child_refs[len - 1].update(py, val)?;
                    if let Some(f) = res {
                        futures.push(f);
                    }
                }
            }
            if !futures.is_empty() {
                let gather = get_gather()?;
                let gathered = gather.call1(py, (futures,))?;
                return Ok(Some(gathered));
            }
        }
        Ok(None)
    }

    fn propagate(&self, py: Python, val: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let skip_async_check = self.asynchronous == Some(false);
        if !skip_async_check {
            let is_awaitable_fn = get_is_awaitable()?;
            let is_awaitable = is_awaitable_fn.call1(py, (&val,))?.is_truthy(py)?;
            if is_awaitable {
                let process_async = get_process_async()?;
                let downstreams_list: Vec<Py<Stream>> =
                    self.downstreams.iter().map(|s| s.clone_ref(py)).collect();
                let dl = PyList::new(py, downstreams_list)?;
                let coro = process_async.call1(py, (val, dl))?;
                return Ok(Some(coro));
            }
        }
        let mut futures = Vec::new();
        for child in &self.downstreams {
            let mut child_ref = child.borrow_mut(py);
            let res = child_ref.update(py, val.clone_ref(py))?;
            if let Some(f) = res {
                futures.push(f);
            }
        }
        if !futures.is_empty() {
            let gather = get_gather()?;
            let gathered = gather.call1(py, (futures,))?;
            return Ok(Some(gathered));
        }
        Ok(None)
    }

    fn update(&mut self, py: Python, x: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let skip_async_check = self.asynchronous == Some(false);
        let result = match &mut self.logic {
            NodeLogic::Source => Some(x),
            NodeLogic::Map { func, args, kwargs } => {
                let args_ref = args.bind(py);
                let x_tuple = PyTuple::new(py, vec![x])?;
                let full_args = x_tuple.add(args_ref)?;
                let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                Some(wrap_error(
                    py,
                    func.call(py, full_args_tuple, kwargs.as_ref().map(|k| k.bind(py))),
                    &self.name,
                )?)
            }
            NodeLogic::Starmap { func, args, kwargs } => {
                let args_ref = args.bind(py);
                let x_bound = x.bind(py);
                let x_tuple: pyo3::Bound<'_, PyTuple> = if x_bound.is_instance_of::<PyTuple>() {
                    x_bound.clone().cast_into::<PyTuple>()?
                } else if x_bound.is_instance_of::<PyList>() {
                    let list = x_bound.cast::<PyList>()?;
                    list.to_tuple()
                } else {
                    let iter = x_bound.try_iter()?;
                    let elements: Result<Vec<_>, _> = iter.collect();
                    PyTuple::new(py, elements?)?
                };
                let full_args = x_tuple.add(args_ref)?;
                let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                Some(wrap_error(
                    py,
                    func.call(py, full_args_tuple, kwargs.as_ref().map(|k| k.bind(py))),
                    &self.name,
                )?)
            }
            NodeLogic::Filter {
                predicate,
                args,
                kwargs,
            } => {
                let args_ref = args.bind(py);
                let x_clone = x.clone_ref(py);
                let x_tuple = PyTuple::new(py, vec![x_clone])?;
                let full_args = x_tuple.add(args_ref)?;
                let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                let res = wrap_error(
                    py,
                    predicate.call(py, full_args_tuple, kwargs.as_ref().map(|k| k.bind(py))),
                    &self.name,
                )?;
                if !skip_async_check {
                    let is_awaitable_fn = get_is_awaitable()?;
                    let is_awaitable = is_awaitable_fn.call1(py, (&res,))?.is_truthy(py)?;
                    if is_awaitable {
                        let filter_async = get_filter_async()?;
                        let downstreams_list: Vec<Py<Stream>> =
                            self.downstreams.iter().map(|s| s.clone_ref(py)).collect();
                        let dl = PyList::new(py, downstreams_list)?;
                        let coro = filter_async.call1(py, (res, x, dl))?;
                        return Ok(Some(coro));
                    }
                }
                if res.is_truthy(py)? { Some(x) } else { None }
            }
            NodeLogic::Flatten => {
                let x_bound = x.bind(py);
                let iter = x_bound.try_iter()?;
                let mut batch = Vec::new();
                for item_res in iter {
                    let item = item_res?;
                    batch.push(item.into());
                }
                return self.propagate_batch(py, batch);
            }
            NodeLogic::Collect { buffer } => {
                buffer.push(x);
                return Ok(None);
            }
            NodeLogic::Sink { func, args, kwargs } => {
                let args_ref = args.bind(py);
                let x_tuple = PyTuple::new(py, vec![x])?;
                let full_args = x_tuple.add(args_ref)?;
                let full_args_tuple = full_args.cast_into::<PyTuple>()?;
                wrap_error(
                    py,
                    func.call(py, full_args_tuple, kwargs.as_ref().map(|k| k.bind(py))),
                    &self.name,
                )?;
                None
            }
            NodeLogic::Accumulate { func, state } => {
                let new_state =
                    wrap_error(py, func.call1(py, (state.clone_ref(py), x)), &self.name)?;
                *state = new_state.clone_ref(py);
                Some(new_state)
            }
            NodeLogic::Tag { index } => {
                let idx_obj: Py<PyAny> = (*index).into_pyobject(py)?.into();
                let elements = vec![idx_obj, x];
                let tuple = PyTuple::new(py, elements)?;
                Some(tuple.into())
            }
            NodeLogic::CombineLatest { state } => {
                let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                let (idx, val) = tuple;
                if idx < state.len() {
                    state[idx] = Some(val);
                }
                if state.iter().all(|s| s.is_some()) {
                    let values: Vec<Py<PyAny>> = state
                        .iter()
                        .map(|s| s.as_ref().unwrap().clone_ref(py))
                        .collect();
                    let tuple_out = PyTuple::new(py, values)?;
                    Some(tuple_out.into())
                } else {
                    None
                }
            }
            NodeLogic::Zip { buffers } => {
                let tuple: (usize, Py<PyAny>) = x.extract(py)?;
                let (idx, val) = tuple;
                if idx < buffers.len() {
                    buffers[idx].push_back(val);
                }
                if buffers.iter().all(|buf| !buf.is_empty()) {
                    let mut values = Vec::with_capacity(buffers.len());
                    for buf in buffers.iter_mut() {
                        if let Some(v) = buf.pop_front() {
                            values.push(v);
                        }
                    }
                    let tuple_out = PyTuple::new(py, values)?;
                    Some(tuple_out.into())
                } else {
                    None
                }
            }
        };
        if let Some(val) = result {
            return self.propagate(py, val);
        }
        Ok(None)
    }
}

impl Stream {
    fn add_tag_node(&mut self, py: Python, index: usize, target: &Py<Stream>) -> PyResult<()> {
        let tag_node = Py::new(
            py,
            Stream {
                logic: NodeLogic::Tag { index },
                downstreams: vec![target.clone_ref(py)],
                name: format!("{}.tag({})", self.name, index),
                asynchronous: self.asynchronous,
            },
        )?;
        self.downstreams.push(tag_node);
        Ok(())
    }
}

#[pymodule]
fn rstreamz(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Stream>()?;
    m.add_function(wrap_pyfunction!(to_text_file, m)?)?;
    m.add_function(wrap_pyfunction!(from_text_file, m)?)?;

    let code = CString::new(HELPERS).unwrap();
    let fname = CString::new("helpers.py").unwrap();
    let mname = CString::new("helpers").unwrap();
    let helpers_module = PyModule::from_code(py, &code, &fname, &mname)?;

    let process_async = helpers_module.getattr("_process_async")?;
    let gather = helpers_module.getattr("_gather")?;
    let filter_async = helpers_module.getattr("_filter_async")?;

    let _ = PROCESS_ASYNC.set(process_async.unbind());
    let _ = GATHER.set(gather.unbind());
    let _ = FILTER_ASYNC.set(filter_async.unbind());

    let inspect = py.import("inspect")?;
    let is_awaitable = inspect.getattr("isawaitable")?;
    let _ = IS_AWAITABLE.set(is_awaitable.unbind());

    Ok(())
}