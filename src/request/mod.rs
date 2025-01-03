mod body;
mod header;

use body::{FunctionRequest, ScriptRequest, UploadRequest};

use crate::{
    client::RequestInfo,
    error::Result,
    request::{body::ConnectRequest, header::ApiType},
    Serialize,
};
use body::RequestBody;
use bytes::BufMut;
use header::RequestHeader;
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub struct BehaviorOptions {
    priority: i32,
    parallelism: i32,
    fetch_size: i32,

    is_subscribe: bool,
}

impl Default for BehaviorOptions {
    fn default() -> Self {
        Self {
            priority: 4,
            parallelism: 64,
            fetch_size: 0,
            is_subscribe: false,
        }
    }
}

impl BehaviorOptions {
    pub fn with_priority(&mut self, priority: i32) -> &mut Self {
        self.priority = priority;
        self
    }

    pub fn with_parallelism(&mut self, parallelism: i32) -> &mut Self {
        self.parallelism = parallelism;
        self
    }

    pub fn with_fetch_size(&mut self, fetch_size: i32) -> &mut Self {
        self.fetch_size = fetch_size;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn is_subscribe(&mut self, subscribe: bool) -> &mut Self {
        self.is_subscribe = subscribe;
        self
    }

    // generate flag for subscriber.
    pub fn special_flag(&self) -> i32 {
        if self.is_subscribe {
            131072
        } else {
            0
        }
    }
}

impl Serialize for BehaviorOptions {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
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

#[derive(Debug)]
pub(crate) struct Request<'a> {
    header: RequestHeader,
    option: BehaviorOptions,
    body: RequestBody<'a>,
}

impl<'a> Request<'a> {
    pub(crate) fn new(
        session_id: Vec<u8>,
        info: RequestInfo<'a>,
        option: &BehaviorOptions,
    ) -> Self {
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
            option: option.clone(),
            body,
        }
    }
}

impl Serialize for Request<'_> {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
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

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
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
