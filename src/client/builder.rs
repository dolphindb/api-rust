use super::request_info::{ConnectInfo, RequestInfo};
use super::Client;
use crate::{
    error::Result,
    request::{BehaviorOptions, Request},
    response::Response,
    Deserialize, Serialize,
};

use bytes::BytesMut;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};

pub struct ClientBuilder<'a, A: ToSocketAddrs> {
    addr: A,
    ssl: bool,
    auth: Option<(&'a str, &'a str)>,
    option: BehaviorOptions,
}

impl<'a, A: ToSocketAddrs> ClientBuilder<'a, A> {
    pub fn new(addr: A) -> Self {
        Self {
            addr,
            ssl: false,
            auth: None,
            option: BehaviorOptions::default(),
        }
    }

    pub fn with_auth(&mut self, auth: (impl Into<&'a str>, impl Into<&'a str>)) -> &mut Self {
        self.auth = Some((auth.0.into(), auth.1.into()));
        self
    }

    #[allow(dead_code)]
    fn with_ssl(&mut self, ssl: bool) -> &mut Self {
        self.ssl = ssl;
        self
    }

    pub fn with_option(&mut self, option: BehaviorOptions) -> &mut Self {
        self.option = option;
        self
    }

    pub async fn connect(mut self) -> Result<Client> {
        let mut conn = TcpStream::connect(&self.addr).await?;

        {
            let socket_ref = socket2::SockRef::from(&conn);

            let keepalive = socket2::TcpKeepalive::new()
                .with_time(Duration::from_secs(5))
                .with_interval(Duration::from_secs(1));

            let _ = socket_ref.set_tcp_keepalive(&keepalive);
        }

        let info = ConnectInfo::new(self.ssl, self.auth.take());
        let request = Request::new(vec![b'0'], RequestInfo::Connect(info), &self.option);

        let mut buf = BytesMut::new();
        request.serialize(&mut buf)?;

        conn.write_all(&buf).await?;

        buf.clear();

        let mut reader = BufReader::new(&mut conn);

        let mut resp = Response::default();
        resp.deserialize(&mut reader).await?;

        Ok(Client {
            session_id: resp.header.session_id,
            conn,
            endian: resp.header.endian,
            option: self.option,
        })
    }
}
