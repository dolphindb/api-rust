//! DolphinDB Client SDK error messages.
//!
//! See [DolphinDB docs](https://docs.dolphindb.cn/zh/rustdoc/chap1_quickstart_landingpage.html) for more information.

use std::string::FromUtf8Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("type {from} cannot be converted to {to}")]
    InvalidConvert { from: String, to: String },
    #[error("invalid data, expect {expect} but gets {actual}")]
    InvalidData { expect: String, actual: String },
    #[error("{0}")]
    InvalidNumeric(String),
    #[error("string is not valid utf8: {0}")]
    InvalidUtf8Encoding(#[from] FromUtf8Error),
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("timeout expired")]
    TimedOut,
    #[error("unexpected EOF")]
    UnexpectedEof,
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
    #[error("unsupported type {data_form}<{data_type}>")]
    Unsupported {
        data_form: String,
        data_type: String,
    },
    #[error("violation: {0}")]
    ConstraintsViolated(String),
    #[error("{0}")]
    ChannelClosed(String),
    #[error("0")]
    StreamSubscriptionError(String),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::ChannelClosed(value.to_string())
    }
}
