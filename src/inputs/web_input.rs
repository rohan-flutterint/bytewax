use std::{sync::Mutex, task::Poll};

use axum::{
    body::Bytes, extract::Extension, http::HeaderMap, response::IntoResponse, routing::get, Router,
};
use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict, PyTuple},
};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};
use tokio::runtime::Runtime;

use crate::{
    common::{pickle_extract, StringResult},
    execution::WorkerIndex,
    pyo3_extensions::{TdPyAny, TdPyCallable},
    window::StateBytes,
};

use super::{InputBuilder, InputConfig, InputReader};

struct State {
    handler: TdPyCallable,
    requests: Arc<Mutex<VecDeque<TdPyAny>>>,
}

/// Use a user-defined function as a handler for an http webserver
///
/// Args:
///
///   handler: (fn): A Python function to be called as the handler for
///       the webserver.
///
/// Returns:
///
///   Config object. Pass this as the `input_config` argument of the
///   `bytewax.dataflow.Dataflow.input` operator.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(handler)")]
#[derive(Clone)]
pub(crate) struct WebServerInputConfig {
    #[pyo3(get)]
    pub(crate) handler_builder: TdPyCallable,
}

#[pymethods]
impl WebServerInputConfig {
    #[new]
    #[args(input_builder)]
    pub(crate) fn new(handler_builder: TdPyCallable) -> (Self, InputConfig) {
        (Self { handler_builder }, InputConfig {})
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> std::collections::HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "WebServerInputConfig".into_py(py)),
                ("input_builder", self.handler_builder.clone().into_py(py)),
            ])
        })
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from a PyDict of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        self.handler_builder = pickle_extract(dict, "handler_builder")?;
        Ok(())
    }
}

impl InputBuilder for WebServerInputConfig {
    fn build(
        &self,
        py: Python,
        worker_index: WorkerIndex,
        worker_count: usize,
        _resume_snapshot: Option<StateBytes>,
    ) -> StringResult<Box<dyn InputReader<TdPyAny>>> {
        let requests = Arc::new(Mutex::new(VecDeque::new()));

        let handler: TdPyCallable = self
            .handler_builder
            .call1(py, (worker_index, worker_count, py.None()))
            .unwrap()
            .extract(py)
            .unwrap();

        let shared_state = Arc::new(State {
            handler: handler.clone(),
            requests: requests.clone(),
        });

        let app = Router::new()
            .route("/", get(run_handler))
            .layer(Extension(shared_state));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("webserver-input-threads")
            .enable_all()
            .build()
            .unwrap();

        let port = std::env::var("BYTEWAX_INPUT_PORT")
            .map(|var| var.parse().expect("Unable to parse BYTEWAX_INPUT_PORT"))
            .unwrap_or(4040);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        rt.spawn(async move {
            tracing::info!("Starting input webserver on port {port:?}");
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .unwrap()
        });

        tracing::info!("Started input webserver on port {port:?}");
        Ok(Box::new(WebServerInput::new(requests.clone(), rt)))
    }
}

pub(crate) struct WebServerInput {
    requests: Arc<Mutex<VecDeque<TdPyAny>>>,
    _rt: Runtime,
}

impl WebServerInput {
    pub(crate) fn new(requests: Arc<Mutex<VecDeque<TdPyAny>>>, rt: Runtime) -> Self {
        Self { requests, _rt: rt }
    }
}

impl InputReader<TdPyAny> for WebServerInput {
    #[tracing::instrument(name = "manual_input", level = "trace", skip_all)]
    fn next(&mut self) -> Poll<Option<TdPyAny>> {
        match self.requests.lock().unwrap().pop_front() {
            Some(item) => Poll::Ready(item.into()),
            None => Poll::Pending,
        }
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes(vec![])
    }
}

async fn run_handler(
    Extension(state): Extension<Arc<State>>,
    body: Bytes,
    headers: HeaderMap,
) -> impl IntoResponse {
    Python::with_gil(|py| {
        let mut header_hashmap = HashMap::new();
        for (k, v) in headers {
            let k = k.expect("Error, no header name").as_str().to_owned();
            let v = String::from_utf8_lossy(v.as_bytes()).into_owned();
            header_hashmap.entry(k).or_insert_with(Vec::new).push(v)
        }
        let res = state
            .handler
            .call1(py, (header_hashmap.into_py(py), body.into_py(py)))
            .unwrap();
        let pytuple: &PyTuple = res.as_ref(py).downcast().unwrap();
        let (http_response, dataflow_response): (&PyAny, &PyAny) = pytuple.extract().unwrap();
        // Push the dataflow response on to our queue for processing by the dataflow
        state
            .requests
            .lock()
            .unwrap()
            .push_back(dataflow_response.into());
        // Return the first argument from the handler function as an http response
        // by turning into bytes
        let http_response: &PyBytes = http_response.downcast().unwrap();
        http_response.extract::<Vec<u8>>().unwrap()
    })
}
