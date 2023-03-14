//! Internal code for input.
//!
//! For a user-centric version of input, read the `bytewax.inputs`
//! Python module docstring. Read that first.

use crate::errors::{raise, UnwrapAny, StackRaiser, TdResult};
use crate::execution::{WorkerCount, WorkerIndex};
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::model::*;
use crate::recovery::operators::{FlowChangeStream, Route};
use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyTypeError, PyValueError};
use pyo3::prelude::*;
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::task::Poll;
use std::time::{Duration, Instant};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Exchange;
use timely::dataflow::{ProbeHandle, Scope, Stream};

/// Length of epoch.
#[derive(Debug, Clone)]
pub(crate) struct EpochInterval(Duration);

impl<'source> FromPyObject<'source> for EpochInterval {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        match ob.extract::<chrono::Duration>()?.to_std() {
            Err(err) => Err(PyValueError::new_err(format!(
                "invalid epoch interval: {err}"
            ))),
            Ok(dur) => Ok(Self(dur)),
        }
    }
}

impl Default for EpochInterval {
    fn default() -> Self {
        Self(Duration::from_secs(10))
    }
}

/// Represents a `bytewax.inputs.Input` from Python.
#[derive(Clone, Debug)]
pub(crate) struct Input(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for Input {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("Input")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "input must derive from `bytewax.inputs.Input`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl IntoPy<Py<PyAny>> for Input {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl Input {
    pub(crate) fn extract<'p, D>(&'p self, py: Python<'p>) -> PyResult<D>
    where
        D: FromPyObject<'p>,
    {
        self.0.extract(py)
    }
}

/// Represents a `bytewax.inputs.PartitionedInput` from Python.
#[derive(Clone)]
pub(crate) struct PartitionedInput(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for PartitionedInput {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("PartitionedInput")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "partitioned input must derive from `bytewax.inputs.PartitionedInput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl PartitionedInput {
    /// Build all partitions for this input for this worker.
    fn build(
        &self,
        py: Python,
        step_id: StepId,
        index: WorkerIndex,
        worker_count: WorkerCount,
        mut resume_state: StepStateBytes,
    ) -> TdResult<HashMap<StateKey, StatefulSource>> {
        let keys: BTreeSet<StateKey> = self
            .0
            .call_method0(py, "list_parts")
            .raises::<PyRuntimeError>("error calling 'list_parts'")?
            .extract(py)
            .raises::<PyValueError>("'list_parts' should return a set of strings (not a list)")?;

        let parts = keys
            .into_iter()
            // We are using the [`StateKey`] routing hash as the way to
            // divvy up partitions to workers. This is kinda an abuse of
            // behavior, but also means we don't have to find a way to
            // propogate the correct partition:worker mappings into the
            // restore system, which would be more difficult as we have to
            // find a way to treat this kind of state key differently. I
            // might regret this.
            .filter(|key| key.is_local(index, worker_count))
            .flat_map(|key| {
                let state = resume_state
                    .remove(&key)
                    .map(StateBytes::de::<TdPyAny>);
                tracing::info!("{index:?} building input {step_id:?} source {key:?} with resume state {state:?}");
                match self.0.call_method1(py, "build_part", (key.clone(), state)).and_then(|part| part.extract(py)) {
                    Err(err) => Some(Err(err)),
                    Ok(None) => None,
                    Ok(Some(part)) => Some(Ok((key, part))),
                }
            })
            .collect::<PyResult<HashMap<StateKey, StatefulSource>>>()
            .raises::<PyRuntimeError>("error building input parts")?;

        if !resume_state.is_empty() {
            tracing::warn!(
                "Resume state exists for {step_id:?} \
                for unknown partitions {:?}; changing partition \
                counts? recovery state routing bug?",
                resume_state.keys()
            );
        }

        Ok(parts)
    }

    /// Read items from a partitioned input.
    ///
    /// This is a stateful operator, so the change stream must be
    /// incorporated into the recovery system, and the resume state
    /// must be routed back here.
    ///
    /// Will manage automatically distributing partition sources. All
    /// you have to do is pass in the definition.
    pub(crate) fn partitioned_input<S>(
        self,
        py: Python,
        scope: &S,
        step_id: StepId,
        epoch_interval: EpochInterval,
        index: WorkerIndex,
        count: WorkerCount,
        probe: &ProbeHandle<u64>,
        start_at: ResumeEpoch,
        resume_state: StepStateBytes,
    ) -> TdResult<(Stream<S, TdPyAny>, FlowChangeStream<S>)>
    where
        S: Scope<Timestamp = u64>,
    {
        let mut parts = self
            .build(py, step_id.clone(), index, count, resume_state)
            .raises::<PyRuntimeError>("error building parts")?;
        let bundle_size = parts.len();
        let mut op_builder = OperatorBuilder::new(step_id.0.clone(), scope.clone());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut change_wrapper, change_stream) = op_builder.new_output();

        let probe = probe.clone();
        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);

        let step_id_op = step_id.clone();
        op_builder.build(move |mut init_caps| {
            let change_cap = init_caps.pop().map(|cap| cap.delayed(&start_at.0)).unwrap();
            let output_cap = init_caps.pop().map(|cap| cap.delayed(&start_at.0)).unwrap();

            let mut caps = Some((output_cap, change_cap));
            let mut epoch_started = Instant::now();
            let mut emit_keys_buffer: HashSet<StateKey> = HashSet::new();
            let mut eofd_keys_buffer: HashSet<StateKey> = HashSet::new();
            let mut snapshot_keys_buffer: HashSet<StateKey> = HashSet::new();

            move |_input_frontiers| {
                caps = caps.take().and_then(|(output_cap, change_cap)| {
                    assert!(output_cap.time() == change_cap.time());
                    let epoch = output_cap.time();

                    if !probe.less_than(epoch) {
                        let mut output_handle = output_wrapper.activate();
                        let mut output_session = output_handle.session(&output_cap);
                        for (key, part) in parts.iter() {
                            match Python::with_gil(|py| part.next(py)).unwrap_any() {
                                Poll::Pending => {}
                                Poll::Ready(None) => {
                                    tracing::trace!(
                                        "Input {step_id_op:?} partition {key:?} reached EOF"
                                    );
                                    eofd_keys_buffer.insert(key.clone());
                                }
                                Poll::Ready(Some(item)) => {
                                    output_session.give(item);
                                    emit_keys_buffer.insert(key.clone());
                                }
                            }
                        }
                    }
                    // Don't allow progress unless we've caught up,
                    // otherwise you can get cascading advancement and
                    // never poll input.
                    let advance =
                        !probe.less_than(epoch) && epoch_started.elapsed() > epoch_interval.0;

                    // If the the current epoch will be over, snapshot
                    // to get "end of the epoch state".
                    if advance {
                        snapshot_keys_buffer.extend(emit_keys_buffer.drain());
                    }
                    snapshot_keys_buffer.extend(eofd_keys_buffer.clone());

                    if !snapshot_keys_buffer.is_empty() {
                        let kchanges = snapshot_keys_buffer
                            .drain()
                            .map(|state_key| {
                                let part = parts
                                    .get(&state_key)
                                    .expect("Unknown partition {state_key:?} to snapshot");
                                let snap = Python::with_gil(|py| part.snapshot(py)).unwrap_any();
                                (state_key, snap)
                            })
                            .map(|(state_key, snap)| (FlowKey(step_id_op.clone(), state_key), snap))
                            .map(|(flow_key, snap)| KChange(flow_key, Change::Upsert(snap)));
                        change_wrapper
                            .activate()
                            .session(&change_cap)
                            .give_iterator(kchanges);
                    }

                    for key in eofd_keys_buffer.drain() {
                        let part = parts
                            .remove(&key)
                            .expect("Unknown partition {key:?} marked as EOF");
                        Python::with_gil(|py| part.close(py)).unwrap_any();
                    }

                    if parts.is_empty() {
                        tracing::trace!("Input {step_id_op:?} reached EOF");
                        None
                    } else if advance {
                        let next_epoch = epoch + 1;
                        epoch_started = Instant::now();
                        tracing::trace!("Input {step_id_op:?} advancing to epoch {next_epoch:?}");
                        Some((
                            output_cap.delayed(&next_epoch),
                            change_cap.delayed(&next_epoch),
                        ))
                    } else {
                        Some((output_cap, change_cap))
                    }
                });

                // Wake up constantly, because we never know when
                // input will have new data.
                if caps.is_some() {
                    activator.activate();
                }
            }
        });

        // Re-balance input if we have a small number of
        // partitions. This will be a slight performance
        // penalty if there are no CPU-heavy tasks, but it
        // seems more intuitive to have all workers
        // contribute.
        let output_stream = if bundle_size < count.0 {
            tracing::info!("Worker count < partition count; activating random load-balancing for input {step_id:?}");
            // TODO: Could do this via `let mut counter =
            // 0` when PR to make this FnMut lands in
            // Timely stable.
            let counter = Cell::new(0 as u64);
            output_stream.exchange(move |_item| {
                let next = counter.get().wrapping_add(1);
                counter.set(next);
                next
            })
        } else {
            output_stream
        };

        Ok((output_stream, change_stream))
    }
}

/// Represents a `bytewax.inputs.StatefulSource` in Python.
struct StatefulSource(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for StatefulSource {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("StatefulSource")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "stateful source is not a subclass of `bytewax.inputs.StatefulSource`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatefulSource {
    fn next(&self, py: Python) -> PyResult<Poll<Option<TdPyAny>>> {
        match self.0.call_method0(py, "next") {
            Err(stop_ex) if stop_ex.is_instance_of::<PyStopIteration>(py) => Ok(Poll::Ready(None)),
            Err(err) => Err(err),
            Ok(none) if none.is_none(py) => Ok(Poll::Pending),
            Ok(item) => Ok(Poll::Ready(Some(item.into()))),
        }
    }

    fn snapshot(&self, py: Python) -> PyResult<StateBytes> {
        let state = self.0.call_method0(py, "snapshot")?.into();
        Ok(StateBytes::ser::<TdPyAny>(&state))
    }

    fn close(self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

/// Represents a `bytewax.inputs.DynamicInput` from Python.
#[derive(Clone)]
pub(crate) struct DynamicInput(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for DynamicInput {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("DynamicInput")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "dynamic input must derive from `bytewax.inputs.DynamicInput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl DynamicInput {
    fn build(
        &self,
        py: Python,
        index: WorkerIndex,
        count: WorkerCount,
    ) -> TdResult<StatelessSource> {
        self.0
            .call_method1(py, "build", (index, count))
            .raises::<PyRuntimeError>("error while calling 'DynamicInput.build'")?
            .extract(py)
            .raises::<PyValueError>("'DynamicInput.build' did not return a 'StatelessSource'")
    }

    /// Read items from a dynamic output.
    ///
    /// Will manage automatically building sinks. All you have to do
    /// is pass in the definition.
    pub(crate) fn dynamic_input<S>(
        self,
        py: Python,
        scope: &S,
        step_id: StepId,
        epoch_interval: EpochInterval,
        index: WorkerIndex,
        count: WorkerCount,
        probe: &ProbeHandle<u64>,
        start_at: ResumeEpoch,
    ) -> TdResult<Stream<S, TdPyAny>>
    where
        S: Scope<Timestamp = u64>,
    {
        let source = self.build(py, index, count)?;

        let mut op_builder = OperatorBuilder::new(step_id.0.to_string(), scope.clone());

        let (mut output_wrapper, output_stream) = op_builder.new_output();

        let probe = probe.clone();
        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);

        op_builder.build(move |mut init_caps| {
            let output_cap = init_caps.pop().map(|cap| cap.delayed(&start_at.0)).unwrap();

            let mut cap_src = Some((output_cap, source));
            let mut epoch_started = Instant::now();

            move |_input_frontiers| {
                cap_src = cap_src.take().and_then(|(cap, source)| {
                    let epoch = cap.time();

                    let mut eof = false;

                    if !probe.less_than(epoch) {
                        match Python::with_gil(|py| source.next(py))
                            .raises::<PyValueError>("error getting input")
                            .unwrap_any()
                        {
                            Poll::Pending => {}
                            Poll::Ready(None) => {
                                eof = true;
                            }
                            Poll::Ready(Some(item)) => {
                                output_wrapper.activate().session(&cap).give(item);
                            }
                        }
                    }
                    // Don't allow progress unless we've caught up,
                    // otherwise you can get cascading advancement and
                    // never poll input.
                    let advance =
                        !probe.less_than(epoch) && epoch_started.elapsed() > epoch_interval.0;

                    if eof {
                        tracing::trace!("Input {step_id:?} reached EOF");
                        Python::with_gil(|py| source.close(py)).unwrap_any();
                        None
                    } else if advance {
                        let next_epoch = epoch + 1;
                        epoch_started = Instant::now();
                        tracing::trace!("Input {step_id:?} advancing to epoch {next_epoch:?}");
                        Some((cap.delayed(&next_epoch), source))
                    } else {
                        Some((cap, source))
                    }
                });

                // Wake up constantly, because we never know when
                // input will have new data.
                if cap_src.is_some() {
                    activator.activate();
                }
            }
        });

        Ok(output_stream)
    }
}

/// Represents a `bytewax.inputs.StatelessSource` in Python.
struct StatelessSource(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for StatelessSource {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("StatelessSource")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(raise::<PyTypeError>(
                "stateless source must derive from `bytewax.inputs.StatelessSource`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatelessSource {
    fn next(&self, py: Python) -> PyResult<Poll<Option<TdPyAny>>> {
        match self.0.call_method0(py, "next") {
            Err(stop_ex) if stop_ex.is_instance_of::<PyStopIteration>(py) => Ok(Poll::Ready(None)),
            Err(err) => Err(err),
            Ok(none) if none.is_none(py) => Ok(Poll::Pending),
            Ok(item) => Ok(Poll::Ready(Some(item.into()))),
        }
    }

    fn close(self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}
