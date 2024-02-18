mod body;
mod header;

use self::body::{FunctionRequest, ScriptRequest, UploadRequest};

use super::client::RequestInfo;
use crate::{
    request::{body::ConnectRequest, header::ApiType},
    Serialize,
};
use body::RequestBody;
use bytes::BufMut;
use header::RequestHeader;
use std::io::Write;

#[derive(Debug)]
pub struct BehaviorOptions {
    priority: i32,
    parallelism: i32,
    fetch_size: i32,
}

impl Default for BehaviorOptions {
    fn default() -> Self {
        Self {
            priority: 4,
            parallelism: 2,
            fetch_size: 0,
        }
    }
}

impl Serialize for BehaviorOptions {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: BufMut,
    {
        let mut writer = buffer.writer();
        write!(
            &mut writer,
            " / {}_1_{}_{}",
            self.special_flag(),
            self.priority,
            self.parallelism
        )
        .unwrap(); // memory write is infallible

        if self.fetch_size > 0 {
            write!(&mut writer, "__{}", self.fetch_size).unwrap();
        }

        Ok(0)
    }
}

impl BehaviorOptions {
    #[allow(unused)]
    pub fn set_priority(&mut self, priority: i32) {
        self.priority = priority;
    }

    // generate flag for subscriber.
    pub fn special_flag(&self) -> i32 {
        0
    }
}

#[derive(Debug)]
pub(crate) struct Request {
    header: RequestHeader,
    option: BehaviorOptions,
    body: RequestBody,
}

impl Request {
    pub(crate) fn new(session_id: Vec<u8>, info: RequestInfo) -> Self {
        use RequestInfo::*;

        let api = match info {
            RequestInfo::Connect(_) => ApiType::API1,
            _ => ApiType::API2,
        };

        let header = RequestHeader::new(api, session_id);

        let body = match info {
            Connect(info) => RequestBody::Connect(ConnectRequest::new(info.auth, info.ssl)),
            Script(info) => RequestBody::Script(ScriptRequest::new(info.script)),
            Function(info) => {
                RequestBody::Function(FunctionRequest::new(info.function, info.args, info.endian))
            }
            Upload(info) => RequestBody::Upload(UploadRequest::new(info.variables, info.endian)),
        };

        Self {
            header,
            option: BehaviorOptions::default(),
            body,
        }
    }
}

impl Serialize for Request {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: BufMut,
    {
        self.header.serialize(buffer)?;

        let mut payload = bytes::BytesMut::new();

        // It's strange that payload length is encoded in `String`.
        let len = self.body.serialize(&mut payload)?;
        if len > 0 {
            buffer.put(len.to_string().as_bytes());
        } else {
            buffer.put(payload.len().to_string().as_bytes());
        }

        // 😿
        match self.body {
            RequestBody::Upload(_) => 0,
            _ => self.option.serialize(buffer)?,
        };

        buffer.put_u8(b'\n');

        buffer.put(&payload[..]);

        Ok(0)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize, ()>
    where
        B: BufMut,
    {
        self.header.serialize_le(buffer)?;

        let mut payload = bytes::BytesMut::new();

        // It's strange that payload length is encoded in `String`.
        let len = self.body.serialize_le(&mut payload)?;
        if len > 0 {
            buffer.put(len.to_string().as_bytes());
        } else {
            buffer.put(payload.len().to_string().as_bytes());
        }

        // 😿
        match self.body {
            RequestBody::Upload(_) => 0,
            _ => self.option.serialize_le(buffer)?,
        };

        buffer.put_u8(b'\n');

        buffer.put(&payload[..]);

        Ok(0)
    }
}
