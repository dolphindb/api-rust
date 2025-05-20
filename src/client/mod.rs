//! DolphinDB connection client for executing Dlang scripts/functions or uploading variables.
//!
//! See [DolphinDB connection docs](https://docs.dolphindb.cn/zh/rustdoc/chap3_basic_operations_landingpage.html) for more information.

mod builder;
mod request_info;
mod table_writer;
use bytes::BytesMut;
pub(crate) use request_info::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub use builder::ClientBuilder;
pub use table_writer::TableWriter;

use crate::request::BehaviorOptions;
use crate::{
    error::Result, request::Request, response::Response, types::ConstantImpl, Deserialize, Endian,
    Serialize,
};

#[derive(Debug)]
pub struct Client {
    session_id: Vec<u8>,
    tx: OwnedWriteHalf,
    rx: BufReader<OwnedReadHalf>,
    endian: Endian,
    option: BehaviorOptions,
}

impl Client {
    async fn run(&mut self, req: Request<'_>) -> Result<Option<ConstantImpl>> {
        let mut buf = BytesMut::new();
        if matches!(self.endian, Endian::Big) {
            req.serialize(&mut buf)?;
        } else {
            req.serialize_le(&mut buf)?;
        }

        self.tx.write_all(&buf).await?;
        self.tx.flush().await?;

        buf.clear();

        let mut resp = Response::default();

        if matches!(self.endian, Endian::Big) {
            resp.deserialize(&mut self.rx).await?;
        } else {
            resp.deserialize_le(&mut self.rx).await?;
        }

        Ok(resp.data)
    }

    pub async fn run_script(&mut self, script: &str) -> Result<Option<ConstantImpl>> {
        let info = ScriptInfo::new(script);

        let req = Request::new(
            self.session_id.clone(),
            RequestInfo::Script(info),
            &self.option,
        );

        self.run(req).await
    }

    pub async fn run_function(
        &mut self,
        function: &str,
        args: &[ConstantImpl],
    ) -> Result<Option<ConstantImpl>> {
        let info = FunctionInfo::new(function, args, self.endian);
        let req = Request::new(
            self.session_id.clone(),
            RequestInfo::Function(info),
            &self.option,
        );

        self.run(req).await
    }

    pub async fn upload(
        &mut self,
        variables: &HashMap<String, ConstantImpl>,
    ) -> Result<Option<ConstantImpl>> {
        let info = UploadInfo::new(variables, self.endian);

        let req = Request::new(
            self.session_id.clone(),
            RequestInfo::Upload(info),
            &self.option,
        );

        self.run(req).await
    }

    pub async fn run_script_with_option(
        &mut self,
        script: &str,
        option: &BehaviorOptions,
    ) -> Result<Option<ConstantImpl>> {
        let info = ScriptInfo::new(script);
        let req = Request::new(self.session_id.clone(), RequestInfo::Script(info), option);
        self.run(req).await
    }

    pub async fn run_function_with_option(
        &mut self,
        function: &str,
        args: &[ConstantImpl],
        option: &BehaviorOptions,
    ) -> Result<Option<ConstantImpl>> {
        let info = FunctionInfo::new(function, args, self.endian);
        let req = Request::new(self.session_id.clone(), RequestInfo::Function(info), option);
        self.run(req).await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.tx.local_addr().unwrap()
    }

    pub(crate) fn rx(&mut self) -> &mut BufReader<OwnedReadHalf> {
        &mut self.rx
    }
}
