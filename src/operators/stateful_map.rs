use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyTypeError, prelude::*};
use tracing::field::debug;

use super::stateful_unary::*;
use crate::{
    errors::UnwrapAny,
    pyo3_extensions::{TdPyAny, TdPyCallable},
    try_unwrap,
};

/// Implements the stateful map operator.
///
/// Map incoming values, having access to a persistent shared state
/// for each key.
pub(crate) struct StatefulMapLogic {
    builder: TdPyCallable,
    mapper: TdPyCallable,
    state: Option<TdPyAny>,
}

impl StatefulMapLogic {
    /// Returns a function that can deserialize the result of
    /// [`Self::snapshot`].
    pub(crate) fn builder(
        builder: TdPyCallable,
        mapper: TdPyCallable,
    ) -> impl Fn(Option<StateBytes>) -> Self {
        move |resume_snapshot| {
            let state = resume_snapshot
                .map(StateBytes::de::<Option<TdPyAny>>)
                .unwrap_or_else(|| {
                    Python::with_gil(|py| {
                        let initial_state: TdPyAny = builder.call1(py, ()).unwrap_any().into();
                        tracing::debug!(
                            builder = ?builder,
                            initial_state = ?initial_state,
                            "stateful_map_builder",
                        );
                        Some(initial_state)
                    })
                });

            Python::with_gil(|py| Self {
                builder: builder.clone_ref(py),
                mapper: mapper.clone_ref(py),
                state,
            })
        }
    }
}

impl StatefulLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for StatefulMapLogic {
    #[tracing::instrument(
        name = "stateful_map",
        level = "trace",
        skip(self),
        fields(self.builder, self.mapper, self.state, updated_state, updated_value)
    )]
    fn on_awake(&mut self, next_value: Poll<Option<TdPyAny>>) -> Option<TdPyAny> {
        if let Poll::Ready(Some(value)) = next_value {
            Python::with_gil(|py| {
                let state = self.state.get_or_insert_with(|| {
                    tracing::trace!("Calling python builder");
                    self.builder.call1(py, ()).unwrap_any().into()
                });
                let (updated_state, updated_value): (Option<TdPyAny>, TdPyAny) = try_unwrap!({
                    let updated_state_value_pytuple: TdPyAny = self
                        .mapper
                        .call1(py, (state.clone_ref(py), value.clone_ref(py)))?
                        .into();
                    updated_state_value_pytuple.extract(py).map_err(|_err| {
                        PyTypeError::new_err(format!(
                            "return value of `mapper` in stateful \
                                        map operator must be a 2-tuple of \
                                        `(updated_state, updated_value)`; \
                                        got `{updated_state_value_pytuple:?}` instead"
                        ))
                    })
                });
                tracing::Span::current().record("updated_state", debug(&updated_state));
                tracing::Span::current().record("updated_value", debug(&updated_value));
                self.state = updated_state;
                Some(updated_value)
            })
        } else {
            None
        }
    }

    fn fate(&self) -> LogicFate {
        if self.state.is_none() {
            LogicFate::Discard
        } else {
            LogicFate::Retain
        }
    }

    fn next_awake(&self) -> Option<DateTime<Utc>> {
        None
    }

    #[tracing::instrument(name = "stateful_map_snapshot", level = "trace", skip_all)]
    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<Option<TdPyAny>>(&self.state)
    }
}
