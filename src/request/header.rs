use std::fmt::{self, Display, Formatter};

use bytes::BufMut;

use super::super::error::Result;
use super::Serialize;

#[derive(Debug)]
pub(crate) enum ApiType {
    API1,
    API2,
}

impl Display for ApiType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ApiType::API1 => write!(f, "API"),
            ApiType::API2 => write!(f, "API2"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RequestHeader {
    api: ApiType,
    session_id: Vec<u8>, // unspecified length in API document
}

impl RequestHeader {
    pub(crate) fn new(api: ApiType, session_id: Vec<u8>) -> Self {
        Self { api, session_id }
    }
}

impl Serialize for RequestHeader {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        buffer.put(self.api.to_string().as_bytes());
        buffer.put_u8(b' ');

        buffer.put(&self.session_id[..]);

        buffer.put_u8(b' ');

        Ok(0)
    }
}
