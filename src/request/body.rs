use bytes::BufMut;
use std::collections::HashMap;

use crate::{types::ConstantKind, Endian, Serialize};

#[derive(Debug)]
pub(crate) enum RequestBody {
    Connect(ConnectRequest),
    Function(FunctionRequest),
    Script(ScriptRequest),
    Upload(UploadRequest),
}

impl Serialize for RequestBody {
    fn serialize<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        match self {
            RequestBody::Connect(request) => request.serialize(buffer),
            RequestBody::Function(request) => request.serialize(buffer),
            RequestBody::Script(request) => request.serialize(buffer),
            RequestBody::Upload(request) => request.serialize(buffer),
        }
    }

    fn serialize_le<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        match self {
            RequestBody::Connect(request) => request.serialize_le(buffer),
            RequestBody::Function(request) => request.serialize_le(buffer),
            RequestBody::Script(request) => request.serialize_le(buffer),
            RequestBody::Upload(request) => request.serialize_le(buffer),
        }
    }
}

#[derive(Default, Debug)]
pub(super) struct ConnectRequest {
    login_info: Option<(String, String)>,
    ssl: bool,
}

impl ConnectRequest {
    pub(super) fn new(login_info: Option<(String, String)>, ssl: bool) -> Self {
        Self { login_info, ssl }
    }
}

impl Serialize for ConnectRequest {
    fn serialize<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        buffer.put(&b"connect\n"[..]);
        if self.ssl {
            return Ok(0);
        }

        if let Some((ref user, ref pwd)) = self.login_info {
            buffer.put(format!("login\n{}\n{}\nfalse", user, pwd).as_bytes());
        } // TODO: else?
        Ok(0)
    }
}

#[derive(Default, Debug)]
pub(super) struct ScriptRequest {
    script: String,
}

impl ScriptRequest {
    pub(super) fn new(script: String) -> Self {
        Self { script }
    }
}

impl Serialize for ScriptRequest {
    fn serialize<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        buffer.put(&b"script\n"[..]);
        buffer.put(self.script.as_bytes());
        Ok(0)
    }
}

#[derive(Default, Debug)]
pub(super) struct FunctionRequest {
    function: String,
    args: Vec<ConstantKind>,
    endian: Endian,
}

impl FunctionRequest {
    pub(super) fn new(function: String, args: Vec<ConstantKind>, endian: Endian) -> Self {
        Self {
            function,
            args,
            endian,
        }
    }

    fn serialize_command<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        buffer.put(&b"function\n"[..]);
        buffer.put(self.function.as_bytes());
        buffer.put_u8(b'\n');

        buffer.put(self.args.len().to_string().as_bytes());
        buffer.put_u8(b'\n');

        self.endian.serialize(buffer)?;
        buffer.put_u8(b'\n');

        Ok("function\n".len() + self.function.len() + self.args.len().to_string().len() + 4)
    }
}

impl Serialize for FunctionRequest {
    fn serialize<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        let res = self.serialize_command(buffer)?;
        for arg in self.args.iter() {
            arg.serialize(buffer)?;
        }
        Ok(res)
    }

    fn serialize_le<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        let res = self.serialize_command(buffer)?;
        for arg in self.args.iter() {
            arg.serialize_le(buffer)?;
        }
        Ok(res)
    }
}

#[derive(Default, Debug)]
pub(super) struct UploadRequest {
    variables: HashMap<String, ConstantKind>,
    endian: Endian,
}

impl UploadRequest {
    pub(super) fn new(variables: HashMap<String, ConstantKind>, endian: Endian) -> Self {
        Self { variables, endian }
    }

    // split hash-base variables into comma-split name string and Constants in Vec.
    fn split_variables(&self) -> (String, Vec<ConstantKind>) {
        // TODO: use keys and values instead of iter
        // TODO: advance split so no need split everytime
        let maps = self
            .variables
            .iter()
            .map(|variable| (variable.0, variable.1.clone()))
            .collect::<Vec<_>>();

        let names = maps
            .iter()
            .map(|variable| variable.0.as_str())
            .collect::<Vec<_>>()
            .join(",");

        let variables = maps
            .into_iter()
            .map(|variable| variable.1)
            .collect::<Vec<_>>();
        (names, variables)
    }

    fn serialize_command<B: BufMut>(
        &self,
        names: String,
        len: usize,
        buffer: &mut B,
    ) -> Result<usize, ()> {
        buffer.put(&b"variable\n"[..]);
        buffer.put(names.as_bytes());
        buffer.put_u8(b'\n');

        buffer.put(len.to_string().as_bytes());
        buffer.put_u8(b'\n');

        self.endian.serialize(buffer)?;
        buffer.put_u8(b'\n');
        Ok("variable\n".len() + names.len() + len.to_string().len() + 4)
    }
}

impl Serialize for UploadRequest {
    fn serialize<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        let (names, variables) = self.split_variables();
        let res = self.serialize_command(names, variables.len(), buffer)?;
        for arg in variables {
            arg.serialize(buffer)?;
        }
        Ok(res)
    }

    fn serialize_le<B: BufMut>(&self, buffer: &mut B) -> Result<usize, ()> {
        let (names, variables) = self.split_variables();
        let res = self.serialize_command(names, variables.len(), buffer)?;
        for arg in variables {
            arg.serialize_le(buffer)?;
        }
        Ok(res)
    }
}
