use crate::{types::ConstantImpl, Endian, Serialize};

use super::super::error::Result;
use bytes::BufMut;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) enum RequestBody<'a> {
    Connect(ConnectRequest<'a>),
    Function(FunctionRequest<'a>),
    Script(ScriptRequest<'a>),
    Upload(UploadRequest<'a>),
}

impl Serialize for RequestBody<'_> {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        match self {
            RequestBody::Connect(request) => request.serialize(buffer),
            RequestBody::Function(request) => request.serialize(buffer),
            RequestBody::Script(request) => request.serialize(buffer),
            RequestBody::Upload(request) => request.serialize(buffer),
        }
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        match self {
            RequestBody::Connect(request) => request.serialize_le(buffer),
            RequestBody::Function(request) => request.serialize_le(buffer),
            RequestBody::Script(request) => request.serialize_le(buffer),
            RequestBody::Upload(request) => request.serialize_le(buffer),
        }
    }
}

#[derive(Default, Debug)]
pub(super) struct ConnectRequest<'a> {
    login_info: Option<(&'a str, &'a str)>,
    ssl: bool,
}

impl<'a> ConnectRequest<'a> {
    pub(super) fn new(login_info: Option<(&'a str, &'a str)>, ssl: bool) -> Self {
        Self { login_info, ssl }
    }
}

impl Serialize for ConnectRequest<'_> {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        buffer.put(&b"connect\n"[..]);

        if self.ssl {
            return Ok(0);
        }

        if let Some((ref user, ref pwd)) = self.login_info {
            let login_string = format!("login\n{}\n{}\nfalse", user, pwd);
            buffer.put(login_string.as_bytes());
        }

        Ok(0)
    }
}

#[derive(Default, Debug)]
pub(super) struct ScriptRequest<'a> {
    script: &'a str,
}

impl<'a> ScriptRequest<'a> {
    pub(super) fn new(script: &'a str) -> Self {
        Self { script }
    }
}

impl Serialize for ScriptRequest<'_> {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        buffer.put(&b"script\n"[..]);

        buffer.put(self.script.as_bytes());

        Ok(0)
    }
}

#[derive(Debug)]
pub(super) struct FunctionRequest<'a> {
    function: &'a str,
    args: &'a [ConstantImpl],
    endian: Endian,
}

impl<'a> FunctionRequest<'a> {
    pub(super) fn new(function: &'a str, args: &'a [ConstantImpl], endian: Endian) -> Self {
        Self {
            function,
            args,
            endian,
        }
    }

    // serialize command information that don't care about endianness.
    fn serialize_command<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        buffer.put(&b"function\n"[..]);

        buffer.put(self.function.as_bytes());
        buffer.put_u8(b'\n');

        buffer.put(self.args.len().to_string().as_bytes());
        buffer.put_u8(b'\n');

        self.endian.serialize(buffer)?;
        buffer.put_u8(b'\n');

        Ok("function\n".len()
            + self.function.len()
            + 1
            + self.args.len().to_string().len()
            + 1
            + 1 // endian
            + 1)
    }
}

impl Serialize for FunctionRequest<'_> {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        let res = self.serialize_command(buffer)?;

        for arg in self.args.iter() {
            arg.serialize(buffer)?;
        }

        Ok(res)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        let res = self.serialize_command(buffer)?;

        for arg in self.args.iter() {
            arg.serialize_le(buffer)?;
        }

        Ok(res)
    }
}

#[derive(Debug)]
pub(super) struct UploadRequest<'a> {
    variables: &'a HashMap<String, ConstantImpl>,
    endian: Endian,
}

impl<'a> UploadRequest<'a> {
    pub(super) fn new(variables: &'a HashMap<String, ConstantImpl>, endian: Endian) -> Self {
        Self { variables, endian }
    }

    // split hash-base variables into comma-split name string and Constants in Vec.
    fn split_variables(&self) -> (String, Vec<ConstantImpl>) {
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

    // serialize command information that don't care about endianness.
    fn serialize_command<B>(&self, names: String, len: usize, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        buffer.put(&b"variable\n"[..]);

        buffer.put(names.as_bytes());
        buffer.put_u8(b'\n');

        buffer.put(len.to_string().as_bytes());
        buffer.put_u8(b'\n');

        self.endian.serialize(buffer)?;
        buffer.put_u8(b'\n');

        Ok("variable\n".len() + names.len() + 1 + len.to_string().len() + 1 + 1 + 1)
    }
}

impl Serialize for UploadRequest<'_> {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        let (names, variables) = self.split_variables();

        let res = self.serialize_command(names, variables.len(), buffer)?;

        for arg in variables {
            arg.serialize(buffer)?;
        }

        Ok(res)
    }

    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        let (names, variables) = self.split_variables();

        let res = self.serialize_command(names, variables.len(), buffer)?;

        for arg in variables {
            arg.serialize_le(buffer)?;
        }

        Ok(res)
    }
}
