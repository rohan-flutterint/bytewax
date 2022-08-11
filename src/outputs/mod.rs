//! Internal code for output.
//!
//! For a user-centric version of output, read the `bytewax.output`
//! Python module docstring. Read that first.
//!
//! Architecture
//! ------------
//!
//! Output is based around the core trait of [`OutputWriter`].  The
//! [`crate::dataflow::Dataflow::capture`] operator delegates to impls
//! of that trait for actual writing.
//!
//! This system follows our standard pattern of having parallel Python
//! config objects and Rust impl structs for each trait of behavior we
//! want. E.g. [`StdOutputConfig`] represents a token in Python for
//! how to create a [`StdOutputWriter`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    with_traceback,
};
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
    routing, Extension, Router,
};
use pyo3::{exceptions::PyValueError, prelude::*};
use tokio::runtime::{self, Runtime};

/// Base class for an output config.
///
/// These define how a certain stream of data should be output.
///
/// Ues a specific subclass of this that matches the output
/// destination you'd like to write to.
#[pyclass(module = "bytewax.outputs", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct OutputConfig;

impl OutputConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, OutputConfig {}).unwrap().into()
    }
}

#[pymethods]
impl OutputConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("OutputConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("OutputConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for OutputConfig: {state:?}"
            )))
        }
    }
}

/// Call a Python callback function with each output item.
///
/// Args:
///
///     output_builder: `output_builder(worker_index: int,
///         worker_count: int) => output_handler(item: Any)` Builder
///         function which returns a handler function for each worker
///         thread, called with `item` whenever an item passes by this
///         capture operator on this worker.
///
/// Returns:
///
///     Config object. Pass this to the
///     `bytewax.dataflow.Dataflow.capture` operator.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(output_builder)")]
pub(crate) struct ManualOutputConfig {
    output_builder: TdPyCallable,
}

#[pymethods]
impl ManualOutputConfig {
    #[new]
    #[args(output_builder)]
    fn new(output_builder: TdPyCallable) -> (Self, OutputConfig) {
        (Self { output_builder }, OutputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, TdPyCallable) {
        ("ManualOutputConfig", self.output_builder.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualOutputConfig", output_builder)) = state.extract() {
            self.output_builder = output_builder;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualOutputConfig: {state:?}"
            )))
        }
    }
}

/// Call a Python callback function with each output epoch and item.
///
/// You probably want to use `ManualOutputConfig` unless you know you
/// need specific epoch assignments for deep integration work.
///
/// Args:
///
///     output_builder: `output_builder(worker_index: int,
///         worker_count: int) => output_handler(epoch_item:
///         Tuple[int, Any])` Builder function which returns a handler
///         function for each worker thread, called with `(epoch,
///         item)` whenever an item passes by this capture operator on
///         this worker.
///
/// Returns:
///
///     Config object. Pass this to the
///     `bytewax.dataflow.Dataflow.capture` operator.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(output_builder)")]
pub(crate) struct ManualEpochOutputConfig {
    output_builder: TdPyCallable,
}

#[pymethods]
impl ManualEpochOutputConfig {
    #[new]
    #[args(output_builder)]
    fn new(output_builder: TdPyCallable) -> (Self, OutputConfig) {
        (Self { output_builder }, OutputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, TdPyCallable) {
        ("ManualEpochOutputConfig", self.output_builder.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualEpochOutputConfig", output_builder)) = state.extract() {
            self.output_builder = output_builder;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualEpochOutputConfig: {state:?}"
            )))
        }
    }
}

/// Write the output items to standard out.
///
/// Items must have a valid `__str__`. If not, map the items into a
/// string before capture.
///
/// Returns:
///
///     Config object. Pass this to the
///     `bytewax.dataflow.Dataflow.capture` operator.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "()")]
pub(crate) struct StdOutputConfig {}

#[pymethods]
impl StdOutputConfig {
    #[new]
    #[args()]
    fn new() -> (Self, OutputConfig) {
        (Self {}, OutputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("StdOutputConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("StdOutputConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for StdOutputConfig: {state:?}"
            )))
        }
    }
}

///
#[derive(Clone)]
struct PySocketAddr(SocketAddr);

impl FromPyObject<'_> for PySocketAddr {
    fn extract<'source>(ob: &'source PyAny) -> PyResult<Self> {
        let s = ob.str()?.to_str()?;
        Ok(PySocketAddr(s.parse().map_err(|_err| {
            PyValueError::new_err(format!(
                "wanted `\"address:port\"` string; got `{s:?}` instead"
            ))
        })?))
    }
}

impl IntoPy<PyObject> for PySocketAddr {
    fn into_py(self, py: Python<'_>) -> PyObject {
        format!("{}", self.0).into_py(py)
    }
}

/// Start a HTTP server and broadcast all captured output items to all
/// websocket clients.
///
/// If a client disconnects and reconnects, they'll miss output.
///
/// Items must have a valid `__str__`. If not, map the items into a
/// string before capture.
///
/// Args:
///
///     socket_addr (str): Local `"address:port"` to listen
///         on. E.g. `"127.0.0.1:3000"`.
///
///     route (str): HTTP route to accept websocket connections
///         on. Defaults to `"/"`.
///
/// Returns:
///
///     Config object. Pass this to the
///     `bytewax.dataflow.Dataflow.capture` operator.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(socket_addr, *, route)")]
pub(crate) struct WebSocketOutputConfig {
    #[pyo3(get)]
    socket_addr: PySocketAddr,
    #[pyo3(get)]
    route: String,
}

#[pymethods]
impl WebSocketOutputConfig {
    #[new]
    #[args(socket_addr, route = "\"/\".to_string()")]
    fn new(socket_addr: PySocketAddr, route: String) -> (Self, OutputConfig) {
        (Self { socket_addr, route }, OutputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, PySocketAddr, String) {
        (
            "WebSocketOutputConfig",
            self.socket_addr.clone(),
            self.route.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (PySocketAddr, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (
            PySocketAddr(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0)),
            s,
        )
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("WebSocketOutputConfig", socket_addr, route)) = state.extract() {
            self.socket_addr = socket_addr;
            self.route = route;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for WebSocketOutputConfig: {state:?}"
            )))
        }
    }
}

/// Defines how output of the dataflow is written.
pub(crate) trait OutputWriter<T, D> {
    /// Write a single output item.
    fn push(&mut self, epoch: T, item: D);
}

pub(crate) fn build_output_writer(
    py: Python,
    config: Py<OutputConfig>,
    worker_index: usize,
    worker_count: usize,
) -> Result<Box<dyn OutputWriter<u64, TdPyAny>>, String> {
    // See comment in [`crate::recovery::build_recovery_writers`]
    // about releasing the GIL during IO class building.
    let config = config.as_ref(py);

    if let Ok(config) = config.downcast::<PyCell<ManualOutputConfig>>() {
        let config = config.borrow();

        let output_builder = config.output_builder.clone();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let writer = ManualOutput::new(py, output_builder, worker_index, worker_count);

        Ok(Box::new(writer))
    } else if let Ok(config) = config.downcast::<PyCell<ManualEpochOutputConfig>>() {
        let config = config.borrow();

        let output_builder = config.output_builder.clone();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let writer = ManualEpochOutput::new(py, output_builder, worker_index, worker_count);

        Ok(Box::new(writer))
    } else if let Ok(config) = config.downcast::<PyCell<StdOutputConfig>>() {
        let _config = config.borrow();

        let writer = py.allow_threads(|| StdOutput::new());

        Ok(Box::new(writer))
    } else if let Ok(config) = config.downcast::<PyCell<WebSocketOutputConfig>>() {
        let config = config.borrow();

        let socket_addr = config.socket_addr.0;
        let route = config.route.clone();

        let writer = py.allow_threads(|| WebSocketOutput::new(socket_addr, route));

        Ok(Box::new(writer))
    } else {
        let pytype = config.get_type();
        Err(format!("Unknown output_config type: {pytype}"))
    }
}

/// Call a Python callback function on each item of output.
struct ManualOutput {
    pyfunc: TdPyCallable,
}

impl ManualOutput {
    fn new(
        py: Python,
        output_builder: TdPyCallable,
        worker_index: usize,
        worker_count: usize,
    ) -> Self {
        let pyfunc: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        Self { pyfunc }
    }
}

impl OutputWriter<u64, TdPyAny> for ManualOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| with_traceback!(py, self.pyfunc.call1(py, (item,))));
    }
}

/// Call a Python callback function on each item of output with its
/// epoch.
struct ManualEpochOutput {
    pyfunc: TdPyCallable,
}

impl ManualEpochOutput {
    fn new(
        py: Python,
        output_builder: TdPyCallable,
        worker_index: usize,
        worker_count: usize,
    ) -> Self {
        let pyfunc: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        Self { pyfunc }
    }
}

impl OutputWriter<u64, TdPyAny> for ManualEpochOutput {
    fn push(&mut self, epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| {
            let epoch_item_pytuple: Py<PyAny> = (epoch, item).into_py(py);
            with_traceback!(py, self.pyfunc.call1(py, (epoch_item_pytuple,)))
        });
    }
}

/// Print output to standard out.
struct StdOutput {}

impl StdOutput {
    fn new() -> Self {
        Self {}
    }
}

impl OutputWriter<u64, TdPyAny> for StdOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| {
            let item = item.as_ref(py);
            let item_str: &str = item
                .str()
                .expect("Items written to std out need to implement `__str__`")
                .extract()
                .unwrap();
            println!("{item_str}");
        });
    }
}

/// Start a HTTP server in a background thread and accept websocket
/// connections and broadcast all output to every websocket
/// connection.
struct WebSocketOutput {
    _rt: Runtime,
    tx: tokio::sync::watch::Sender<String>,
}

impl WebSocketOutput {
    fn new(socket_addr: SocketAddr, route: String) -> Self {
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let (tx, rx) = tokio::sync::watch::channel("".into());

        rt.spawn(async move {
            let app = Router::new()
                .route(&route, routing::get(Self::handle_http_request))
                .layer(Extension(rx));

            axum::Server::bind(&socket_addr)
                .serve(app.into_make_service())
                .await
                .unwrap();
        });

        WebSocketOutput { _rt: rt, tx }
    }

    async fn handle_http_request(
        request: WebSocketUpgrade,
        Extension(rx): Extension<tokio::sync::watch::Receiver<String>>,
    ) -> Response {
        request.on_upgrade(|socket| Self::socket_main(socket, rx))
    }

    async fn socket_main(mut socket: WebSocket, mut rx: tokio::sync::watch::Receiver<String>) {
        while rx.changed().await.is_ok() {
            let item_str = rx.borrow().clone();
            let msg = Message::Text(item_str);
            if socket.send(msg).await.is_err() {
                return;
            }
        }
    }
}

impl OutputWriter<u64, TdPyAny> for WebSocketOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| {
            let item = item.as_ref(py);
            let item_str: &str = item
                .str()
                .expect("Items written to websockets need to implement `__str__`")
                .extract()
                .unwrap();
            self.tx
                .send(item_str.to_string())
                .expect("Error sending output item to websockets");
        });
    }
}

pub(crate) fn capture(
    writer: &mut Box<dyn OutputWriter<u64, TdPyAny>>,
    epoch: &u64,
    item: &TdPyAny,
) {
    writer.push(*epoch, item.clone());
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<OutputConfig>()?;
    m.add_class::<ManualOutputConfig>()?;
    m.add_class::<ManualEpochOutputConfig>()?;
    m.add_class::<StdOutputConfig>()?;
    m.add_class::<WebSocketOutputConfig>()?;
    Ok(())
}
