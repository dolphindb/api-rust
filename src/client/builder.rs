use super::request_info::{ConnectInfo, RequestInfo};
use super::Client;
use crate::request::Request;
use crate::response::Response;
use crate::{Deserialize, Serialize};
use bytes::BytesMut;
use std::io::ErrorKind;
use std::time::Duration;
use std::{
    borrow::Borrow,
    io::{Error, Result},
};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};

pub struct ClientBuilder<A: ToSocketAddrs> {
    host: A,
    ssl: bool,
    auth: Option<(String, String)>,
}

impl<A: ToSocketAddrs> ClientBuilder<A> {
    pub fn new(host: A) -> Result<Self> {
        Ok(Self {
            host,
            ssl: false,
            auth: None,
        })
    }

    pub fn with_auth(&mut self, auth: (impl Into<String>, impl Into<String>)) {
        self.auth = Some((auth.0.into(), auth.1.into()));
    }

    pub fn with_ssl(&mut self, ssl: bool) {
        self.ssl = ssl;
    }

    pub async fn connect(self) -> Result<Client> {
        let mut conn = TcpStream::connect(self.host.borrow()).await?;

        {
            let socket_ref = socket2::SockRef::from(&conn);

            let keepalive = socket2::TcpKeepalive::new()
                .with_time(Duration::from_secs(5))
                .with_interval(Duration::from_secs(1));

            let _ = socket_ref.set_tcp_keepalive(&keepalive);
        }

        let info = ConnectInfo::new(self.ssl, self.auth.clone());
        let request = Request::new(vec![b'0'], RequestInfo::Connect(info));

        let mut buf = BytesMut::new();
        request
            .serialize(&mut buf)
            .map_err(|_| Error::from(ErrorKind::InvalidData))?;

        conn.write_all(&buf).await?;

        buf.clear();

        let mut reader = BufReader::new(&mut conn);

        let mut resp = Response::default();
        resp.deserialize(&mut reader).await?;

        Ok(Client {
            session_id: resp.header.session_id,
            conn,
            endian: resp.header.endian,
        })
    }
}
