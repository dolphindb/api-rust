use std::collections::HashMap;

use crate::{types::ConstantKind, Endian};

#[derive(Debug)]
pub(crate) enum RequestInfo {
    Connect(ConnectInfo),
    Script(ScriptInfo),
    Function(FunctionInfo),
    Upload(UploadInfo),
}

#[derive(Debug)]
pub(crate) struct ConnectInfo {
    pub(crate) ssl: bool,
    pub(crate) auth: Option<(String, String)>,
}

impl ConnectInfo {
    pub(super) fn new(ssl: bool, auth: Option<(String, String)>) -> Self {
        Self { ssl, auth }
    }
}

#[derive(Debug)]
pub(crate) struct ScriptInfo {
    pub(crate) script: String,
}

impl ScriptInfo {
    pub(super) fn new(script: String) -> Self {
        Self { script }
    }
}

#[derive(Debug)]
pub(crate) struct FunctionInfo {
    pub(crate) function: String,
    pub(crate) args: Vec<ConstantKind>,
    pub(crate) endian: Endian,
}

impl FunctionInfo {
    pub(super) fn new(function: String, args: Vec<ConstantKind>, endian: Endian) -> Self {
        Self {
            function,
            args,
            endian,
        }
    }
}

#[derive(Debug)]
pub(crate) struct UploadInfo {
    pub(crate) variables: HashMap<String, ConstantKind>,
    pub(crate) endian: Endian,
}

impl UploadInfo {
    pub(super) fn new(variables: HashMap<String, ConstantKind>, endian: Endian) -> Self {
        Self { variables, endian }
    }
}
