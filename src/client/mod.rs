mod builder;
mod request_info;
use bytes::BytesMut;
pub(crate) use request_info::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub use builder::ClientBuilder;

use crate::request::BehaviorOptions;
use crate::{
    error::Result, request::Request, response::Response, types::ConstantImpl, Deserialize, Endian,
    Serialize,
};

#[derive(Debug)]
pub struct Client {
    session_id: Vec<u8>,
    conn: TcpStream,
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

        self.conn.write_all(&buf).await?;
        self.conn.flush().await?;

        buf.clear();

        let mut reader = BufReader::new(&mut self.conn);

        let mut resp = Response::default();

        if matches!(self.endian, Endian::Big) {
            resp.deserialize(&mut reader).await?;
        } else {
            resp.deserialize_le(&mut reader).await?;
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
        self.conn.local_addr().unwrap()
    }

    pub fn into_inner(self) -> TcpStream {
        self.conn
    }
}
