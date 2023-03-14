/// This module contains functions to help create a proper error message
/// when something goes wrong, either in our or our users' code.
///
/// We make use of the #[track_caller] attribute to get the location
/// of the function that called the error builder function.
/// We then use the location to print file, line and column of where
/// the error happened.
use std::{error::Error, fmt::Display, panic::Location, thread};

use pyo3::{PyErr, PyResult, PyTypeInfo, Python};

// Custom error type that wraps PyErr.
#[derive(Debug)]
pub(crate) struct TdError(PyErr);

impl TdError {
    /// Call PyErr::clone_ref and build a new cloned TdError
    pub fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    /// Return the message string with the thread
    /// name as prefix to each line.
    pub fn msg_with_tname(&self) -> String {
        prepend_tname(self.to_string())
    }
}

impl Display for TdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for TdError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

// Custom Result type (more or less a wrapped PyResult)
pub(crate) type TdResult<T> = Result<T, TdError>;

pub(crate) trait StackRaiser<T> {
    /// This function converts self (which should be a Result) to PyResult
    fn as_pyresult<PyErrType: PyTypeInfo>(self) -> PyResult<T>;

    /// Call this when you want to add this error
    /// to the stack trace and bubble it up.
    fn raises<PyErrType: PyTypeInfo>(self, msg: &str) -> TdResult<T>;

    /// Only call this when you want to raise an error
    /// directly to the python interpreter.
    /// This should only be used on functions that will be directly
    /// called from Python.
    fn raise_pyerr<PyErrType: PyTypeInfo>(self, msg: &str) -> PyResult<T>
    where
        Self: Sized,
    {
        self.raises::<PyErrType>(msg).as_pyresult::<PyErrType>()
    }

    fn _raise<PyErrType: PyTypeInfo>(self, msg: &str, caller: &Location) -> TdResult<T>
    where
        Self: Sized,
    {
        self.as_pyresult::<PyErrType>().map_err(|err| {
            Python::with_gil(|py| {
                let msg: String = format!("({caller}) {msg}");
                let err_msg: String = if let Some(tb) = get_traceback(py, &err) {
                    format!("{err}\n{tb}")
                } else {
                    format!("{err}")
                };
                err_chain::<PyErrType>(&msg, &err_msg)
            })
        })
    }
}

impl<T> StackRaiser<T> for Result<T, String> {
    fn as_pyresult<PyErrType: PyTypeInfo>(self) -> Result<T, PyErr> {
        self.map_err(|err| PyErr::new::<PyErrType, _>(err))
    }

    #[track_caller]
    fn raises<PyErrType: PyTypeInfo>(self, msg: &str) -> TdResult<T> {
        let caller = std::panic::Location::caller();
        self._raise::<PyErrType>(msg, caller)
    }
}

impl<T> StackRaiser<T> for PyResult<T> {
    fn as_pyresult<PyErrType: PyTypeInfo>(self) -> Result<T, PyErr> {
        self
    }

    #[track_caller]
    fn raises<PyErrType: PyTypeInfo>(self, msg: &str) -> TdResult<T> {
        let caller = std::panic::Location::caller();
        self._raise::<PyErrType>(msg, caller)
    }
}

impl<T> StackRaiser<T> for TdResult<T> {
    fn as_pyresult<PyErrType: PyTypeInfo>(self) -> Result<T, PyErr> {
        self.map_err(|err| err.0)
    }

    #[track_caller]
    fn raises<PyErrType: PyTypeInfo>(self, msg: &str) -> TdResult<T> {
        let caller = std::panic::Location::caller();
        self._raise::<PyErrType>(msg, caller)
    }
}

/// Creates a ByteError of the given Python type tracking the caller
#[track_caller]
pub(crate) fn tderr<T: PyTypeInfo>(msg: &str) -> TdError {
    let caller = std::panic::Location::caller();
    TdError(PyErr::new::<T, _>(format!("({caller}) {msg}")))
}

/// Creates a PyErr of the given Python type tracking the caller
#[track_caller]
pub(crate) fn raise<T: PyTypeInfo>(msg: &str) -> PyErr {
    let caller = std::panic::Location::caller();
    PyErr::new::<T, _>(format!("({caller}) {msg}"))
}

/// Prepend the name of the current thread to each line,
/// if present.
fn prepend_tname(msg: String) -> String {
    let tname = thread::current()
        .name()
        .unwrap_or("unnamed-thread")
        .to_string();
    msg.split("\n")
        .map(|line| format!("<{tname}> {line}\n"))
        .collect()
}

fn get_traceback(py: Python, err: &PyErr) -> Option<String> {
    err.traceback(py).map(|tb| {
        tb.format()
            .unwrap_or_else(|_| "Unable to print traceback".to_string())
    })
}

pub(crate) fn pyerr_msg_thread(py: Python, err: &PyErr) -> String {
    if let Some(tb) = get_traceback(py, err) {
        prepend_tname(format!("{err}\n{tb}"))
    } else {
        prepend_tname(err.to_string())
    }
}

pub(crate) fn err_msg_thread(err: &str) -> String {
    prepend_tname(err.to_string())
}

/// Chain an error msg with another string
fn err_chain<T: PyTypeInfo>(msg: &str, err: &str) -> TdError {
    TdError(PyErr::new::<T, _>(format!("{msg}\nCaused by => {err}")))
}
