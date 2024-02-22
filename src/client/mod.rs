mod builder;
mod request_info;
use bytes::BytesMut;
pub(crate) use request_info::*;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub use builder::ClientBuilder;

use crate::{
    request::Request, response::Response, types::ConstantImpl, Deserialize, Endian, Serialize,
};

pub struct Client {
    session_id: Vec<u8>,
    conn: TcpStream,
    endian: Endian,
}

impl Client {
    async fn run(&mut self, req: Request) -> Result<Vec<ConstantImpl>> {
        let mut buf = BytesMut::new();
        if matches!(self.endian, Endian::Big) {
            req.serialize(&mut buf)
                .map_err(|_| Error::from(ErrorKind::InvalidData))?;
        } else {
            req.serialize_le(&mut buf)
                .map_err(|_| Error::from(ErrorKind::InvalidData))?;
        }

        self.conn.write_all(&buf).await?;
        self.conn.flush().await?; // ? when to flush

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

    pub async fn run_script(&mut self, script: String) -> Result<Vec<ConstantImpl>> {
        let info = ScriptInfo::new(script);
        #[cfg(feature = "debug_pr")]
        println!("info: {:?}", info);

        let req = Request::new(self.session_id.clone(), RequestInfo::Script(info));
        #[cfg(feature = "debug_pr")]
        println!("request: {:?}", req);

        self.run(req).await
    }

    pub async fn run_function(
        &mut self,
        function: String,
        args: Vec<ConstantImpl>,
    ) -> Result<Vec<ConstantImpl>> {
        let info = FunctionInfo::new(function, args, self.endian);
        let req = Request::new(self.session_id.clone(), RequestInfo::Function(info));

        self.run(req).await
    }

    pub async fn upload(
        &mut self,
        variables: HashMap<String, ConstantImpl>,
    ) -> Result<Vec<ConstantImpl>> {
        let info = UploadInfo::new(variables, self.endian);
        #[cfg(feature = "debug_pr")]
        println!("info: {:?}", info);

        let req = Request::new(self.session_id.clone(), RequestInfo::Upload(info));
        #[cfg(feature = "debug_pr")]
        println!("request: {:?}", req);

        self.run(req).await
    }
}
