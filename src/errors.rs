/// This module contains functions to help create a proper error message
/// when something goes wrong, either in our or our users' code.
///
/// We make use of the #[track_caller] attribute to get the location
/// of the function that called the error builder function.
/// We then use the location to print file, line and column of where
/// the error happened.
use std::{any::Any, error::Error, fmt::Display, thread};

use pyo3::{PyErr, PyResult, PyTypeInfo, Python};

pub(crate) trait ByteErr<T> {
    fn pyerr<PyErrType: PyTypeInfo>(self, msg: &str) -> ByteResult<T>;
    fn pyerr_thread<PyErrType: PyTypeInfo>(self, msg: &str) -> PyResult<T>;
}

impl<T> ByteErr<T> for Result<T, String> {
    #[track_caller]
    fn pyerr<PyErrType: PyTypeInfo>(self, msg: &str) -> ByteResult<T> {
        let caller = std::panic::Location::caller();
        self.map_err(|err| {
            Python::with_gil(|py| {
                let err_msg: String =
                    if let Some(tb) = get_traceback(py, &pyerr::<PyErrType>(&err).as_pyerr()) {
                        format!("({caller}) {err}\n{tb}")
                    } else {
                        format!("({caller}) {err}")
                    };
                err_chain::<PyErrType>(msg, &err_msg)
            })
        })
    }

    #[track_caller]
    fn pyerr_thread<PyErrType: PyTypeInfo>(self, msg: &str) -> PyResult<T> {
        let caller = std::panic::Location::caller();
        self.map_err(|err| {
            Python::with_gil(|py| {
                let err = &pyerr::<PyErrType>(&err).as_pyerr();
                let err_msg: String = if let Some(tb) = get_traceback(py, err) {
                    prepend_tname(format!("({caller}) {err}\n{tb}"))
                } else {
                    prepend_tname(format!("({caller}) {err}"))
                };
                err_chain::<PyErrType>(msg, &err_msg).as_pyerr()
            })
        })
    }
}

impl<T> ByteErr<T> for PyResult<T> {
    #[track_caller]
    fn pyerr<PyErrType: PyTypeInfo>(self, msg: &str) -> ByteResult<T> {
        let caller = std::panic::Location::caller();
        self.map_err(|err| {
            Python::with_gil(|py| {
                let err = &err;
                let err_msg: String = if let Some(tb) = get_traceback(py, err) {
                    format!("({caller}) {err}\n{tb}")
                } else {
                    format!("({caller}) {err}")
                };
                err_chain::<PyErrType>(msg, &err_msg)
            })
        })
    }

    #[track_caller]
    fn pyerr_thread<PyErrType: PyTypeInfo>(self, msg: &str) -> PyResult<T> {
        let caller = std::panic::Location::caller();
        self.map_err(|err| {
            Python::with_gil(|py| {
                {
                    let err = &err;
                    let err_msg: String = if let Some(tb) = get_traceback(py, err) {
                        prepend_tname(format!("({caller}) {err}\n{tb}"))
                    } else {
                        prepend_tname(format!("({caller}) {err}"))
                    };
                    err_chain::<PyErrType>(msg, &err_msg)
                }
                .as_pyerr()
            })
        })
    }
}

impl<T> ByteErr<T> for ByteResult<T> {
    #[track_caller]
    fn pyerr<PyErrType: PyTypeInfo>(self, msg: &str) -> ByteResult<T> {
        let caller = std::panic::Location::caller();
        self.map_err(|err| {
            Python::with_gil(|py| {
                let err = &err.0;
                let err_msg: String = if let Some(tb) = get_traceback(py, err) {
                    format!("({caller}) {err}\n{tb}")
                } else {
                    format!("({caller}) {err}")
                };
                err_chain::<PyErrType>(msg, &err_msg)
            })
        })
    }

    #[track_caller]
    fn pyerr_thread<PyErrType: PyTypeInfo>(self, msg: &str) -> PyResult<T> {
        let caller = std::panic::Location::caller();
        self.map_err(|err| {
            Python::with_gil(|py| {
                {
                    let err = &err.0;
                    let err_msg: String = if let Some(tb) = get_traceback(py, err) {
                        prepend_tname(format!("({caller}) {err}\n{tb}"))
                    } else {
                        prepend_tname(format!("({caller}) {err}"))
                    };
                    err_chain::<PyErrType>(msg, &err_msg)
                }
                .as_pyerr()
            })
        })
    }
}

// impl Deref for ByteError {
//     type Target = PyErr;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

pub(crate) type ByteResult<T> = Result<T, ByteError>;

// impl From<PyErr> for ByteError {
//     fn from(value: PyErr) -> Self {
//         Self(value)
//     }
// }

// impl From<&PyErr> for ByteError {
//     fn from(value: &PyErr) -> Self {
//         Self(value)
//     }
// }
// impl From<ByteError> for PyErr {
//     fn from(value: ByteError) -> Self {
//         value.0
//     }
// }

#[derive(Debug)]
pub struct ByteError(pub(crate) PyErr);

impl ByteError {
    pub fn as_pyerr(self) -> PyErr {
        self.0
    }
}

impl Display for ByteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for ByteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

/// Converts anything that can be converted to a string to a PyErr of the given type
pub(crate) fn pyerr<T: PyTypeInfo>(msg: &str) -> ByteError {
    let caller = std::panic::Location::caller();
    ByteError(PyErr::new::<T, _>(format!("{caller} {msg}")))
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
fn err_chain<T: PyTypeInfo>(msg: &str, err: &str) -> ByteError {
    ByteError(PyErr::new::<T, _>(format!("{msg}\nCaused by => {err}")))
}

/// Chain an error msg with another string, adding track_caller info and thread name.
#[track_caller]
pub(crate) fn err_chain_thread<T: PyTypeInfo>(msg: &str, err: &str) -> ByteError {
    let caller = std::panic::Location::caller();
    let err_msg = prepend_tname(format!("\n({caller}) {msg}"));
    err_chain::<T>(&err_msg, err)
}

/// Chain an error msg with a PyErr, adding track_caller info and the traceback if present.
#[track_caller]
pub(crate) fn pyerr_chain<T: PyTypeInfo>(py: Python, msg: &str, err: &PyErr) -> ByteError {
    let caller = std::panic::Location::caller();
    let err_msg: String = if let Some(tb) = get_traceback(py, err) {
        format!("({caller}) {err}\n{tb}")
    } else {
        format!("({caller}) {err}")
    };
    err_chain::<T>(msg, &err_msg)
}

/// Chain an error msg with a PyErr, adding track_caller info, traceback and thread name.
#[track_caller]
pub(crate) fn pyerr_chain_thread<T: PyTypeInfo>(py: Python, msg: &str, err: &PyErr) -> ByteError {
    let caller = std::panic::Location::caller();
    let err_msg: String = if let Some(tb) = get_traceback(py, err) {
        prepend_tname(format!("({caller}) {err}\n{tb}"))
    } else {
        prepend_tname(format!("({caller}) {err}"))
    };
    err_chain::<T>(msg, &err_msg)
}
