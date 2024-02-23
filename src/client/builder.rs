use bytes::BytesMut;
use std::{borrow::Borrow, io, time::Duration};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpStream, ToSocketAddrs},
};

use super::{
    request_info::{ConnectInfo, RequestInfo},
    Client,
};
use crate::{request::Request, response::Response, Deserialize, Serialize};

/// Used to construct a Client
pub struct ClientBuilder<A: ToSocketAddrs> {
    host: A,
    ssl: bool,
    auth: Option<(String, String)>,
}

impl<A: ToSocketAddrs> ClientBuilder<A> {
    /// Construct a ClientBuilder
    pub fn new(host: A) -> Self {
        Self {
            host,
            ssl: false,
            auth: None,
        }
    }

    /// Set authentication information
    pub fn with_auth<U: Into<String>, P: Into<String>>(&mut self, auth: (U, P)) -> &mut Self {
        self.auth = Some((auth.0.into(), auth.1.into()));
        self
    }

    /// Set ssl config
    pub fn with_ssl(&mut self, ssl: bool) -> &mut Self {
        self.ssl = ssl;
        self
    }

    /// Establish a connection
    pub async fn connect(&self) -> io::Result<Client> {
        let mut conn = TcpStream::connect(self.host.borrow()).await?;
        socket2::SockRef::from(&conn).set_tcp_keepalive(
            &socket2::TcpKeepalive::new()
                .with_time(Duration::from_secs(5))
                .with_interval(Duration::from_secs(1)),
        )?;

        let mut buf = BytesMut::new();
        Request::new(
            vec![b'0'],
            RequestInfo::Connect(ConnectInfo::new(self.ssl, self.auth.clone())),
        )
        .serialize(&mut buf)
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        conn.write_all(&buf).await?;
        // TODO：modify serialize to return io::Error type

        let mut resp = Response::default();
        let mut reader = BufReader::new(&mut conn);
        resp.deserialize(&mut reader).await?;
        Ok(Client {
            session_id: resp.header.session_id,
            conn,
            endian: resp.header.endian,
        })
    }
}
