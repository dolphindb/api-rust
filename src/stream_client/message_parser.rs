use std::sync::Arc;

use crate::{
    error::{Error, Result},
    types::{Any, Constant, ConstantImpl, Vector, VectorImpl},
    Deserialize, Endian,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt},
    sync::mpsc::UnboundedSender,
};

use crate::client::Client;

use super::message::Message;

pub(crate) struct MessageParser {
    msg_id: u64,
    topic: Option<String>,
    sender: UnboundedSender<Arc<Message>>,
}

impl MessageParser {
    pub(crate) fn new(sender: UnboundedSender<Arc<Message>>) -> Self {
        Self {
            msg_id: 0,
            topic: None,
            sender,
        }
    }

    // panic if no topic exist.
    fn pack_messages(&mut self, v: Vector<Any>) -> Result<Vec<Arc<Message>>> {
        // todo: array vector
        if matches!(v.first().unwrap().get(), ConstantImpl::Scalar(_)) {
            return Ok(vec![Arc::new(Message::new(
                self.msg_id as i64,
                self.topic.take().unwrap(),
                VectorImpl::Any(v),
            ))]);
        }

        if v.iter()
            .any(|v| !matches!(v.get(), ConstantImpl::Vector(_)))
        {
            return Err(Error::BadResponse("mismatched stream data from".into()));
        }

        let content_len = v.first().unwrap().get().len();
        let mut res = Vec::with_capacity(v.first().unwrap().get().len());

        let msg_id = self.msg_id + 1 - content_len as u64;
        let topic = self.topic.take().unwrap();

        for i in 0..content_len {
            let v = v
                .iter()
                .map(|v| v.get().get(i).unwrap().into())
                .collect::<Vector<Any>>();

            res.push(Arc::new(Message::new(
                (msg_id as i64) + (i as i64) as i64,
                topic.clone(),
                VectorImpl::Any(v),
            )));
        }

        Ok(res)
    }

    pub(crate) async fn run(&mut self, mut client: Client) -> Result<()> {
        let reader = client.rx();
        loop {
            let endian = self.parse_endian(reader).await?;
            let mut c = ConstantImpl::default();

            if matches!(endian, Endian::Big) {
                self.parse_header(reader).await?;
                c.deserialize(reader).await?;
            } else {
                self.parse_header_le(reader).await?;
                c.deserialize_le(reader).await?;
            }

            self.dispatch_data(c)?;
        }
    }
}

impl MessageParser {
    async fn parse_endian<R>(&mut self, reader: &mut R) -> Result<Endian>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let b = reader.read_u8().await?;

        match b {
            0 => Ok(Endian::Big),
            1 => Ok(Endian::Little),
            n => Err(Error::BadResponse(format!(
                "response header corrupted, unrecognized endian flag {}",
                n
            ))),
        }
    }

    async fn parse_header<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let _ = reader.read_u64().await?;
        let msg_id = reader.read_u64().await?;

        let mut buf = Vec::new();
        let n = reader.read_until(b'\0', &mut buf).await?;

        if n == 0 {
            return Err(Error::UnexpectedEof);
        }

        self.topic = Some(String::from_utf8(buf)?);
        self.msg_id = msg_id;

        Ok(())
    }

    async fn parse_header_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let _ = reader.read_u64_le().await?;
        let msg_id = reader.read_u64_le().await?;

        let mut buf = Vec::new();
        let n = reader.read_until(b'\0', &mut buf).await?;

        if n == 0 {
            return Err(Error::UnexpectedEof);
        }

        self.topic = Some(String::from_utf8(buf)?);
        self.msg_id = msg_id;

        Ok(())
    }

    fn dispatch_data(&mut self, c: ConstantImpl) -> Result<()> {
        match c {
            ConstantImpl::Table(ref t) => {
                if !t.is_empty() {
                    Err(Error::InvalidData {
                        expect: "schema table with 0 rows".into(),
                        actual: format!("table with {} rows", c.len()),
                    })
                } else {
                    Ok(())
                }
            }

            ConstantImpl::Vector(v) => match v {
                VectorImpl::Any(v) => {
                    if v.is_empty() {
                        return Err(Error::BadResponse("empty stream data".into()));
                    }

                    let len = v.first().unwrap().get().len();
                    if v.iter().any(|v| v.get().len() != len) {
                        return Err(Error::BadResponse("mismatched stream data len".into()));
                    }

                    let messages = self.pack_messages(v)?;
                    for msg in messages {
                        let _ = self.sender.send(msg);
                    }

                    Ok(())
                }
                _ => Err(Error::InvalidData {
                    expect: "Any vector".into(),
                    actual: format!("{} vector", v.data_type()),
                }),
            },

            _ => Err(Error::BadResponse(
                "invalid format of stream message".into(),
            )),
        }
    }
}
