use std::collections::HashMap;

use crate::{types::ConstantImpl, Endian};

#[derive(Debug)]
pub(crate) enum RequestInfo<'a> {
    Connect(ConnectInfo<'a>),
    Script(ScriptInfo<'a>),
    Function(FunctionInfo<'a>),
    Upload(UploadInfo<'a>),
}

#[derive(Debug)]
pub(crate) struct ConnectInfo<'a> {
    pub(crate) ssl: bool,
    pub(crate) auth: Option<(&'a str, &'a str)>,
}

impl<'a> ConnectInfo<'a> {
    pub(super) fn new(ssl: bool, auth: Option<(&'a str, &'a str)>) -> Self {
        Self { ssl, auth }
    }
}

#[derive(Debug)]
pub(crate) struct ScriptInfo<'a> {
    pub(crate) script: &'a str,
}

impl<'a> ScriptInfo<'a> {
    pub(super) fn new(script: &'a str) -> Self {
        Self { script }
    }
}

#[derive(Debug)]
pub(crate) struct FunctionInfo<'a> {
    pub(crate) function: &'a str,
    pub(crate) args: &'a [ConstantImpl],
    pub(crate) endian: Endian,
}

impl<'a> FunctionInfo<'a> {
    pub(super) fn new(function: &'a str, args: &'a [ConstantImpl], endian: Endian) -> Self {
        Self {
            function,
            args,
            endian,
        }
    }
}

#[derive(Debug)]
pub(crate) struct UploadInfo<'a> {
    pub(crate) variables: &'a HashMap<String, ConstantImpl>,
    pub(crate) endian: Endian,
}

impl<'a> UploadInfo<'a> {
    pub(super) fn new(variables: &'a HashMap<String, ConstantImpl>, endian: Endian) -> Self {
        Self { variables, endian }
    }
}
