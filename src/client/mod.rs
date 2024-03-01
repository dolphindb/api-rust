mod builder;
mod request_info;

pub use builder::ClientBuilder;
pub(crate) use request_info::*;

use bytes::BytesMut;
use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{
    request::Request, response::Response, types::ConstantKind, Deserialize, Endian, Serialize,
};

/// The connected client
pub struct Client {
    session_id: Vec<u8>,
    conn: TcpStream,
    endian: Endian,
}

impl Client {
    async fn run(&mut self, req: Request) -> Result<Vec<ConstantKind>> {
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

    /// Execute the script
    pub async fn run_script(&mut self, script: String) -> Result<Vec<ConstantKind>> {
        let info = ScriptInfo::new(script);
        #[cfg(feature = "debug_pr")]
        println!("info: {:?}", info);

        let req = Request::new(self.session_id.clone(), RequestInfo::Script(info));
        #[cfg(feature = "debug_pr")]
        println!("request: {:?}", req);

        self.run(req).await
    }

    /// Execute the function
    pub async fn run_function(
        &mut self,
        function: String,
        args: Vec<ConstantKind>,
    ) -> Result<Vec<ConstantKind>> {
        let info = FunctionInfo::new(function, args, self.endian);
        let req = Request::new(self.session_id.clone(), RequestInfo::Function(info));

        self.run(req).await
    }

    /// Upload data to server
    pub async fn upload(
        &mut self,
        variables: HashMap<String, ConstantKind>,
    ) -> Result<Vec<ConstantKind>> {
        let info = UploadInfo::new(variables, self.endian);
        #[cfg(feature = "debug_pr")]
        println!("info: {:?}", info);

        let req = Request::new(self.session_id.clone(), RequestInfo::Upload(info));
        #[cfg(feature = "debug_pr")]
        println!("request: {:?}", req);

        self.run(req).await
    }
}
