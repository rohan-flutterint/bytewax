use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use tracing::field::debug;

use crate::{pyo3_extensions::TdPyAny, window::*};

/// Implements the collect window operator.
///
/// Append values in a window onto a list. Emit the list when the
/// window closes.
pub(crate) struct CollectWindowLogic {
    acc: Vec<(TdPyAny, DateTime<Utc>)>,
}

impl CollectWindowLogic {
    pub(crate) fn builder() -> impl Fn(Option<StateBytes>) -> Self {
        move |resume_snapshot| {
            let acc = resume_snapshot
                .map(StateBytes::de::<Vec<(TdPyAny, DateTime<Utc>)>>)
                .unwrap_or_default();
            Self { acc }
        }
    }
}

impl WindowLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for CollectWindowLogic {
    #[tracing::instrument(
        name = "collect_window",
        level = "trace",
        skip(self),
        fields(self.acc)
    )]
    fn with_next(&mut self, next_value: Option<(TdPyAny, DateTime<Utc>)>) -> Option<TdPyAny> {
        match next_value {
            Some((value, item_time)) => {
                self.acc.push((value, item_time));
                tracing::Span::current().record("self.acc", debug(&self.acc));
                None
            }
            // Emit in item time order at end of window.
            None => {
                self.acc
                    .sort_by_key(|(_value, item_time)| item_time.clone());
                let out_values: Vec<TdPyAny> = self
                    .acc
                    .drain(..)
                    .map(|(value, _item_time)| value)
                    .collect();
                Python::with_gil(|py| Some(out_values.into_py(py).into()))
            }
        }
    }

    #[tracing::instrument(name = "collect_window_snapshot", level = "trace", skip_all)]
    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<Vec<(TdPyAny, DateTime<Utc>)>>(&self.acc)
    }
}