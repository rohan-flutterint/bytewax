//! Internal code for dataflow execution.
//!
//! For a user-centric version of how to execute dataflows, read the
//! the `bytewax.execution` Python module docstring. Read that first.
//!
//! [`worker_main()`] for the root of all the internal action here.
//!
//! Dataflow Building
//! -----------------
//!
//! The "blueprint" of a dataflow in [`crate::dataflow::Dataflow`] is
//! compiled into a Timely dataflow in [`build_production_dataflow`].
//!
//! See [`crate::recovery`] for a description of the recovery
//! components added to the Timely dataflow.

use crate::dataflow::{Dataflow, Step};
use crate::errors::{
    err_chain_thread, err_msg_thread, plain_tderr, pyerr_chain, pyerr_chain_thread,
    pyerr_msg_thread, tderr, StackRaiser, TdError, TdResult,
};
use crate::inputs::*;
use crate::operators::collect_window::CollectWindowLogic;
use crate::operators::fold_window::FoldWindowLogic;
use crate::operators::reduce::ReduceLogic;
use crate::operators::reduce_window::ReduceWindowLogic;
use crate::operators::stateful_map::StatefulMapLogic;
use crate::operators::stateful_unary::StatefulUnary;
use crate::operators::*;
use crate::outputs::*;
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair};
use crate::recovery::dataflows::*;
use crate::recovery::model::*;
use crate::recovery::python::*;
use crate::recovery::store::in_mem::{InMemProgress, StoreSummary};
use crate::webserver::run_webserver;
use crate::window::WindowBuilder;
use crate::window::{clock::ClockBuilder, StatefulWindowUnary};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io::Write;
use std::panic::{panic_any, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use timely::communication::Allocate;
use timely::dataflow::operators::*;
use timely::dataflow::ProbeHandle;
use timely::progress::Timestamp;
use timely::worker::Worker;
use tracing::span::EnteredSpan;

/// Integer representing the index of a worker in a cluster.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct WorkerIndex(pub(crate) usize);

impl IntoPy<Py<PyAny>> for WorkerIndex {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

/// Integer representing the number of workers in a cluster.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerCount(pub(crate) usize);

impl WorkerCount {
    /// Iterate through all workers in this cluster.
    pub(crate) fn iter(&self) -> impl Iterator<Item = WorkerIndex> {
        (0..self.0).map(WorkerIndex)
    }
}

impl IntoPy<Py<PyAny>> for WorkerCount {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

#[test]
fn worker_count_iter_works() {
    let count = WorkerCount(3);
    let found: Vec<_> = count.iter().collect();
    let expected = vec![WorkerIndex(0), WorkerIndex(1), WorkerIndex(2)];
    assert_eq!(found, expected);
}

/// Turn the abstract blueprint for a dataflow into a Timely dataflow
/// so it can be executed.
///
/// This is more complicated than a 1:1 translation of Bytewax
/// concepts to Timely, as we are using Timely as a basis to implement
/// more-complicated Bytewax features like input builders and
/// recovery.
#[allow(clippy::too_many_arguments)]
fn build_production_dataflow<A, PW, SW>(
    py: Python,
    worker: &mut Worker<A>,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    resume_from: ResumeFrom,
    mut resume_state: FlowStateBytes,
    mut resume_progress: InMemProgress,
    store_summary: StoreSummary,
    mut progress_writer: PW,
    state_writer: SW,
    interrupt_flag: &'static AtomicBool,
) -> TdResult<ProbeHandle<u64>>
where
    A: Allocate,
    PW: ProgressWriter + 'static,
    SW: StateWriter + 'static,
{
    let ResumeFrom(ex, resume_epoch) = resume_from;

    let worker_index = WorkerIndex(worker.index());
    let worker_count = WorkerCount(worker.peers());

    let worker_key = WorkerKey(ex, worker_index);

    let progress_init = KChange(
        worker_key,
        Change::Upsert(ProgressMsg::Init(worker_count, resume_epoch)),
    );
    resume_progress.write(progress_init.clone());
    progress_writer.write(progress_init);

    // Remember! Never build different numbers of Timely operators on
    // different workers! Timely does not like that and you'll see a
    // mysterious `failed to correctly cast channel` panic. You must
    // build asymmetry within each operator.
    worker.dataflow(|scope| {
        let flow = flow.as_ref(py).borrow();

        let mut probe = ProbeHandle::new();

        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut step_changes = Vec::new();

        // Start with an "empty" stream. We might overwrite it with
        // input later.
        let mut stream = None.to_stream(scope);

        for step in &flow.steps {
            // All these closure lifetimes are static, so tell
            // Python's GC that there's another pointer to the
            // mapping function that's going to hang around
            // for a while when it's moved into the closure.
            let step = step.clone();
            match step {
                Step::CollectWindow {
                    step_id,
                    clock_config,
                    window_config,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let clock_builder = clock_config
                        .build(py)
                        .raises::<PyRuntimeError>("can't build the clock config")?;
                    let windower_builder = window_config
                        .build(py)
                        .raises::<PyRuntimeError>("can't build the window config")?;

                    let (output, changes) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        CollectWindowLogic::builder(),
                        step_resume_state,
                    );

                    stream = output
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::Input {
                    step_id,
                    input,
                } => {
                    if let Ok(input) = input.extract::<PartitionedInput>(py) {
                        let step_resume_state = resume_state.remove(&step_id);

                        let (output, changes) = input.partitioned_input(
                            py,
                            scope,
                            step_id.clone(),
                            epoch_interval.clone(),
                            worker_index,
                            worker_count,
                            &probe,
                            resume_epoch,
                            step_resume_state,
                        ).raises::<PyRuntimeError>("error building partitioned input")?;

                        inputs.push(output.clone());
                        stream = output;
                        step_changes.push(changes);
                    } else if let Ok(input) = input.extract::<DynamicInput>(py) {
                        let output = input.dynamic_input(
                            py,
                            scope,
                            step_id.clone(),
                            epoch_interval.clone(),
                            worker_index,
                            worker_count,
                            &probe,
                            resume_epoch,
                        ).raises::<PyRuntimeError>("error building dynamic input")?;

                        inputs.push(output.clone());
                        stream = output;
                    } else {
                        return Err(tderr::<PyTypeError>("unknown input type"))
                    }
                }
                Step::Map { mapper } => {
                    stream = stream.map(move |item| map(&mapper, item));
                }
                Step::FlatMap { mapper } => {
                    stream = stream.flat_map(move |item| flat_map(&mapper, item));
                }
                Step::Filter { predicate } => {
                    stream = stream.filter(move |item| filter(&predicate, item));
                }
                Step::FilterMap { mapper } => {
                    stream = stream
                        .map(move |item| map(&mapper, item))
                        .filter(move |item| Python::with_gil(|py| !item.is_none(py)));
                }
                Step::FoldWindow {
                    step_id,
                    clock_config,
                    window_config,
                    builder,
                    folder,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let clock_builder = clock_config.build(py).raises::<PyRuntimeError>("error building clock")?;
                    let windower_builder = window_config.build(py).raises::<PyRuntimeError>("error building window")?;

                    let (output, changes) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        FoldWindowLogic::new(builder, folder),
                        step_resume_state,
                    );

                    stream = output
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::Inspect { inspector } => {
                    stream = stream.inspect(move |item| inspect(&inspector, item));
                }
                Step::InspectEpoch { inspector } => {
                    stream = stream
                        .inspect_time(move |epoch, item| inspect_epoch(&inspector, epoch, item));
                }
                Step::Reduce {
                    step_id,
                    reducer,
                    is_complete,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let (output, changes) = stream.map(extract_state_pair).stateful_unary(
                        step_id,
                        ReduceLogic::builder(reducer, is_complete),
                        step_resume_state,
                    );
                    stream = output.map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::ReduceWindow {
                    step_id,
                    clock_config,
                    window_config,
                    reducer,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let clock_builder = clock_config.build(py).raises::<PyRuntimeError>("error building clock")?;
                    let windower_builder = window_config.build(py).raises::<PyRuntimeError>("error building window")?;

                    let (output, changes) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        ReduceWindowLogic::builder(reducer),
                        step_resume_state,
                    );

                    stream = output
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let (output, changes) = stream.map(extract_state_pair).stateful_unary(
                        step_id,
                        StatefulMapLogic::builder(builder, mapper),
                        step_resume_state,
                    );
                    stream = output.map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::Output { step_id, output } => {
                    if let Ok(output) = output.extract(py) {
                        let step_resume_state = resume_state.remove(&step_id);

                        let (output, changes) =
                            stream.partitioned_output(
                                py,
                                step_id,
                                output,
                                worker_index,
                                worker_count,
                                step_resume_state,
                            ).raises::<PyRuntimeError>("error building partitioned output")?;
                        let clock = output.map(|_| ());

                        outputs.push(clock.clone());
                        step_changes.push(changes);
                        stream = output;
                    } else if let Ok(output) = output.extract(py) {
                        let output =
                            stream.dynamic_output(
                                py,
                                step_id,
                                output,
                                worker_index,
                                worker_count,
                            ).raises::<PyRuntimeError>("error building dynamic output")?;
                        let clock = output.map(|_| ());

                        outputs.push(clock.clone());
                        stream = output;
                    } else {
                        return Err(tderr::<PyTypeError>("unknown output type"));
                    }
                }
            }
        }

        if inputs.is_empty() {
            return Err(tderr::<PyValueError>("Dataflow needs to contain at least one input"));
        }
        if outputs.is_empty() {
            return Err(tderr::<PyValueError>("Dataflow needs to contain at least one output"));
        }
        if !resume_state.is_empty() {
            tracing::warn!(
                "Resume state exists for unknown steps {:?}; did you delete or rename a step and forget to remove or migrate state data?",
                resume_state.keys(),
            );
        }

        attach_recovery_to_dataflow(
            &mut probe,
            worker_key,
            resume_progress,
            store_summary,
            progress_writer,
            state_writer,
            scope.concatenate(step_changes),
            scope.concatenate(outputs),
        );

        Ok(probe)
    })
}

// Struct used to handle a span that is closed and reopened periodically.
struct PeriodicSpan {
    span: Option<EnteredSpan>,
    length: Duration,
    // State
    last_open: Instant,
    counter: u64,
}

impl PeriodicSpan {
    pub fn new(length: Duration) -> Self {
        Self {
            span: Some(tracing::trace_span!("Periodic", counter = 0).entered()),
            length,
            last_open: Instant::now(),
            counter: 0,
        }
    }

    pub fn update(&mut self) {
        if self.last_open.elapsed() > self.length {
            if let Some(span) = self.span.take() {
                span.exit();
            }
            self.counter += 1;
            self.span = Some(tracing::trace_span!("Periodic", counter = self.counter).entered());
            self.last_open = Instant::now();
        }
    }
}

/// Run a dataflow which uses sources until complete.
fn run_until_done<A: Allocate, T: Timestamp>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    probe: ProbeHandle<T>,
) -> TdResult<()> {
    let mut span = PeriodicSpan::new(Duration::from_secs(10));
    while !FLAG.load(Ordering::Relaxed) && !probe.done() {
        println!("Step {} {}", worker.index(), FLAG.load(Ordering::Relaxed));
        let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
            worker.step();
        }));
        if let Err(err) = res {
            let err = err.downcast::<TdError>().unwrap();
            eprintln!("CIAO {err}");
            interrupt_flag.store(true, Ordering::Relaxed);
            return Err(*err);
        }
        span.update();
        Python::with_gil(|py| {
            Python::check_signals(py).map_err(|err| {
                interrupt_flag.store(true, Ordering::Relaxed);
                pyerr_chain_thread::<PyRuntimeError>(py, "Error in worker", &err.into())
            })
        })?;
    }
    Ok(())
}

fn build_and_run_progress_loading_dataflow<A, R>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    progress_reader: R,
) -> TdResult<InMemProgress>
where
    A: Allocate,
    R: ProgressReader + 'static,
{
    let (probe, progress_store) = build_progress_loading_dataflow(worker, progress_reader);

    run_until_done(worker, interrupt_flag, probe)?;

    let resume_progress = Rc::try_unwrap(progress_store)
        .expect("Resume epoch dataflow still has reference to progress_store")
        .into_inner();

    Ok(resume_progress)
}

fn build_and_run_state_loading_dataflow<A, R>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    resume_epoch: ResumeEpoch,
    state_reader: R,
) -> TdResult<(FlowStateBytes, StoreSummary)>
where
    A: Allocate,
    R: StateReader + 'static,
{
    let (probe, resume_state, summary) =
        build_state_loading_dataflow(worker, state_reader, resume_epoch);

    run_until_done(worker, interrupt_flag, probe)?;

    Ok((
        Rc::try_unwrap(resume_state)
            .expect("State loading dataflow still has reference to resume_state")
            .into_inner(),
        Rc::try_unwrap(summary)
            .expect("State loading dataflow still has reference to summary")
            .into_inner(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_and_run_production_dataflow<A, PW, SW>(
    worker: &mut Worker<A>,
    interrupt_flag: &'static AtomicBool,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    resume_from: ResumeFrom,
    resume_state: FlowStateBytes,
    resume_progress: InMemProgress,
    store_summary: StoreSummary,
    progress_writer: PW,
    state_writer: SW,
) -> TdResult<()>
where
    A: Allocate,
    PW: ProgressWriter + 'static,
    SW: StateWriter + 'static,
{
    let span = tracing::trace_span!("Building dataflow").entered();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("webserver-threads")
        .enable_all()
        .build()
        .unwrap();

    let probe = Python::with_gil(|py| {
        let df: Dataflow = flow.extract(py).unwrap();
        if worker.index() == 0 && std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
            rt.spawn(run_webserver(df));
        }

        build_production_dataflow(
            py,
            worker,
            flow,
            epoch_interval,
            resume_from,
            resume_state,
            resume_progress,
            store_summary,
            progress_writer,
            state_writer,
            interrupt_flag,
        )
        .raises::<PyRuntimeError>("error building dataflow")
    })?;
    span.exit();

    run_until_done(worker, interrupt_flag, probe)?;

    Ok(())
}

/// Terminate all dataflows in this worker.
///
/// We need this because otherwise all of Timely's entry points
/// (e.g. [`timely::execute::execute_from`]) wait until all work is
/// complete and we will hang.
fn shutdown_worker<A: Allocate>(worker: &mut Worker<A>) {
    for dataflow_id in worker.installed_dataflows() {
        worker.drop_dataflow(dataflow_id);
    }
}

/// What a worker thread should do during its lifetime.
fn worker_main<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &'static AtomicBool,
    flow: Py<Dataflow>,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> TdResult<()> {
    // Modify traceback to specify it's a python error
    // XXX: Sure this is a good idea?
    //     Python::with_gil(|py| {
    //         py.run(
    //             r#"
    // import traceback
    // import sys
    //
    // def exception_handler(*exc_info):
    //     # All your trace are belong to us!
    //     print("".join(traceback.format_exception(*exc_info)))
    //     # text = "".join(traceback.format_exception(*exc_info)).split("\n")
    //     # text = "".join(f"<pythread>\t{i}\n" for i in text)
    //     # print(text)
    //
    // sys.excepthook = exception_handler
    //         "#,
    //             None,
    //             None,
    //         )
    //     })
    //     .stack::<PyRuntimeError>("error running custom exception handler hook")?;

    let worker_index = worker.index();
    let worker_count = worker.peers();

    tracing::info!("Worker {worker_index:?} of {worker_count:?} starting up");

    let epoch_interval = epoch_interval.unwrap_or_default();
    tracing::info!("Using epoch interval of {epoch_interval:?}");

    let recovery_config = recovery_config.unwrap_or_else(default_recovery_config);

    let (progress_reader, state_reader) = Python::with_gil(|py| {
        build_recovery_readers(py, worker_index, worker_count, recovery_config.clone())
    })
    .raises::<PyRuntimeError>("error building recovery readers")?;
    let (progress_writer, state_writer) = Python::with_gil(|py| {
        build_recovery_writers(py, worker_index, worker_count, recovery_config)
    })
    .raises::<PyRuntimeError>("error building recovery writers")?;

    let span = tracing::trace_span!("Resume epoch").entered();
    let resume_progress =
        build_and_run_progress_loading_dataflow(worker, interrupt_flag, progress_reader)?;
    span.exit();
    let resume_from = resume_progress.resume_from();
    tracing::info!("Calculated {resume_from:?}");

    let span = tracing::trace_span!("State loading").entered();
    let ResumeFrom(_ex, resume_epoch) = resume_from;
    let (resume_state, store_summary) =
        build_and_run_state_loading_dataflow(worker, interrupt_flag, resume_epoch, state_reader)?;
    span.exit();

    // let res = build_and_run_production_dataflow(
    build_and_run_production_dataflow(
        worker,
        interrupt_flag,
        flow,
        epoch_interval,
        resume_from,
        resume_state,
        resume_progress,
        store_summary,
        progress_writer,
        state_writer,
    )?;
    // res.unwrap()_or_else(|err| panic_any(err));

    shutdown_worker(worker);

    tracing::info!("Worker {worker_index:?} of {worker_count:?} shut down");

    Ok(())
}

// TODO: pytest --doctest-modules does not find doctests in PyO3 code.
/// Execute a dataflow in the current thread.
///
/// Blocks until execution is complete.
///
/// You'd commonly use this for prototyping custom input and output
/// builders with a single worker before using them in a cluster
/// setting.
///
/// >>> from bytewax.dataflow import Dataflow
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> run_main(flow)
/// 0
/// 1
/// 2
///
/// See `bytewax.spawn_cluster()` for starting a cluster on this
/// machine with full control over inputs and outputs.
///
/// Args:
///
///   flow: Dataflow to run.
///
///   epoch_interval (datetime.timedelta): System time length of each
///       epoch. Defaults to 10 seconds.
///
///   recovery_config: State recovery config. See
///       `bytewax.recovery`. If `None`, state will not be
///       persisted.

static FLAG: AtomicBool = AtomicBool::new(false);

#[pyfunction(flow, "*", epoch_interval = "None", recovery_config = "None")]
#[pyo3(text_signature = "(flow, *, epoch_interval, recovery_config)")]
pub(crate) fn run_main(
    py: Python,
    flow: Py<Dataflow>,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    // let interrupt_flag = AtomicBool::new(false);
    let result = py.allow_threads(move || {
        std::panic::catch_unwind(|| {
            // TODO: See if we can PR Timely to not cast result error
            // to a String. Then we could "raise" Python errors from
            // the builder directly. Probably also as part of the
            // panic recast issue below.
            timely::execute::execute_directly::<TdResult<()>, _>(move |worker| {
                worker_main(worker, &FLAG, flow, epoch_interval, recovery_config)
            })
        })
    });

    match result {
        Ok(Ok(_)) => PyResult::Ok(()),
        Ok(err_result) => err_result
            .raises::<PyValueError>("error executing Dataflow")
            .as_pyresult::<PyValueError>(),
        Err(panic_err) => {
            // Here the worker either crashed with a PyErr or panicked in Rust.
            // We can differentiate the 2 situations by trying to downcast the panic
            // to a PyErr. If that doesn't work, we assume the worker crashed
            // on the Rust side of the moon.
            let err = if let Some(err) = panic_err.downcast_ref::<TdError>() {
                err.clone_ref(py)
            } else if let Some(err) = panic_err.downcast_ref::<String>() {
                tderr::<PyRuntimeError>(err)
            } else {
                tderr::<PyRuntimeError>("unknown error")
            };
            TdResult::Err(err)
                .raises::<PyRuntimeError>("Worker crashed")
                .as_pyresult::<PyRuntimeError>()
        }
    }
}

/// Execute a dataflow in the current process as part of a cluster.
///
/// You have to coordinate starting up all the processes in the
/// cluster and ensuring they each are assigned a unique ID and know
/// the addresses of other processes. You'd commonly use this for
/// starting processes as part of a Kubernetes cluster.
///
/// Blocks until execution is complete.
///
/// >>> from bytewax.dataflow import Dataflow
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> addresses = []  # In a real example, you'd find the "host:port" of all other Bytewax workers.
/// >>> proc_id = 0  # In a real example, you'd assign each worker a distinct ID from 0..proc_count.
/// >>> cluster_main(flow, addresses, proc_id)
/// 0
/// 1
/// 2
///
/// See `bytewax.run_main()` for a way to test input and output
/// builders without the complexity of starting a cluster.
///
/// See `bytewax.spawn_cluster()` for starting a simple cluster
/// locally on one machine.
///
/// Args:
///
///   flow: Dataflow to run.
///
///   addresses: List of host/port addresses for all processes in
///       this cluster (including this one).
///
///   proc_id: Index of this process in cluster; starts from 0.
///
///   epoch_interval (datetime.timedelta): System time length of each
///       epoch. Defaults to 10 seconds.
///
///   recovery_config: State recovery config. See
///       `bytewax.recovery`. If `None`, state will not be
///       persisted.
///
///   worker_count_per_proc: Number of worker threads to start on
///       each process.
#[pyfunction(
    flow,
    addresses,
    proc_id,
    "*",
    epoch_interval = "None",
    recovery_config = "None",
    worker_count_per_proc = "1"
)]
#[pyo3(
    text_signature = "(flow, addresses, proc_id, *, epoch_interval, recovery_config, worker_count_per_proc)"
)]
pub(crate) fn cluster_main(
    py: Python,
    flow: Py<Dataflow>,
    addresses: Option<Vec<String>>,
    proc_id: usize,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
    worker_count_per_proc: usize,
) -> PyResult<()> {
    py.allow_threads(move || {
        let addresses = addresses.unwrap_or_default();
        let (builders, other) = if addresses.is_empty() {
            timely::CommunicationConfig::Process(worker_count_per_proc)
        } else {
            timely::CommunicationConfig::Cluster {
                threads: worker_count_per_proc,
                process: proc_id,
                addresses,
                report: false,
                log_fn: Box::new(|_| None),
            }
        }
        .try_build()
        .raises::<PyRuntimeError>("Error building cluster")
        .as_pyresult::<PyRuntimeError>()?;

        // let should_shutdown = Arc::new(AtomicBool::new(false));
        // let should_shutdown_w = should_shutdown.clone();
        // let should_shutdown_p = should_shutdown.clone();
        // let should_shutdown_3 = should_shutdown.clone();

        // Set a custom panic_hook.
        // Here we try to downcast the payload to various
        // types to offer better info, and default to the original info.
        std::panic::set_hook(Box::new(move |info| {
            FLAG.store(true, Ordering::Relaxed);
            // Acquire stdout lock and write the string as bytes,
            // so we avoid interleaving outputs from different threads.
            let mut stderr = std::io::stderr().lock();
            let msg = if let Some(pyerr) = info.payload().downcast_ref::<PyErr>() {
                Python::with_gil(|py| pyerr_msg_thread(py, pyerr))
            } else if let Some(err) = info.payload().downcast_ref::<TdError>() {
                Python::with_gil(|py| pyerr_msg_thread(py, &err.as_pyerr_ref()))
            } else if let Some(err) = info.payload().downcast_ref::<&str>() {
                err_msg_thread(err)
            } else if let Some(err) = info.payload().downcast_ref::<String>() {
                err_msg_thread(err)
            } else {
                format!("{}\n", info)
            };
            stderr
                .write_all(format!("{msg}\n").as_bytes())
                .unwrap_or_else(|err| eprintln!("Error printing error (that's not good): {err}"));
        }));
        let guards = timely::execute::execute_from::<_, TdResult<()>, _>(
            builders,
            other,
            timely::WorkerConfig::default(),
            move |worker| {
                worker_main(
                    worker,
                    &FLAG,
                    flow.clone(),
                    epoch_interval.clone(),
                    recovery_config.clone(),
                )
            },
        )
        .raises::<PyRuntimeError>("Error while running the cluster")
        .as_pyresult::<PyRuntimeError>()?;
        // .map_err(|err| {
        //     err_chain_thread::<PyRuntimeError>("Error while running cluster", &err).as_pyerr()
        // })?;

        // Recreating what Python does in Thread.join() to "block"
        // but also check interrupt handlers.
        // https://github.com/python/cpython/blob/204946986feee7bc80b233350377d24d20fcb1b8/Modules/_threadmodule.c#L81
        while guards
            .guards()
            .iter()
            .any(|worker_thread| !worker_thread.is_finished())
        {
            thread::sleep(Duration::from_millis(1));
            Python::with_gil(|py| Python::check_signals(py)).map_err(|err| {
                FLAG.store(true, Ordering::Relaxed);
                err
            })?;
        }
        for maybe_worker_panic in guards.join() {
            // TODO: See if we can PR Timely to not cast panic info to
            // String. Then we could re-raise Python exception in main
            // thread and not need to print in panic::set_hook above,
            // although we still need it to tell the other workers to
            // do graceful shutdown.
            match maybe_worker_panic {
                Ok(Ok(ok)) => Ok(ok),
                Ok(err) => err.raises::<PyRuntimeError>("Error building Dataflow"),
                // Here we ignore _panic_err since it's an Any
                // without a useful representation.
                Err(_panic_err) => Err(tderr::<PyRuntimeError>(
                    "Worker thread panicked, see error above",
                )),
            }
            .map_err(|err| err.as_pyerr())?;
        }

        Ok(())
    })
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    Ok(())
}
