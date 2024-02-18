mod header;
use crate::types::ConstantImpl;
use crate::Deserialize;

use self::header::ResponseHeader;
use std::io::{Error, ErrorKind, Result};
use tokio::io::AsyncBufReadExt;

#[derive(Default, Debug)]
struct ExecuteResult {
    res: String,
}

#[derive(Default, Debug)]
pub(crate) struct Response {
    pub(crate) header: ResponseHeader,
    res: ExecuteResult,
    pub(crate) data: Vec<ConstantImpl>,
}

impl Deserialize for Response {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        self.header.deserialize(reader).await?;

        println!("header: {:?}", self.header);

        self.res.deserialize(reader).await?;

        println!("response string: {:?}", self.res);

        for _ in 0..self.header.counts {
            let mut c = ConstantImpl::default();
            c.deserialize(reader).await?;
            self.data.push(c)
        }

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        self.header.deserialize_le(reader).await?;

        self.res.deserialize_le(reader).await?;

        for _ in 0..self.header.counts {
            let mut c = ConstantImpl::default();
            c.deserialize_le(reader).await?;
            self.data.push(c)
        }

        Ok(())
    }
}

impl Deserialize for ExecuteResult {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut buf = String::new();
        if reader.read_line(&mut buf).await? == 0 {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        if !buf.ends_with('\n') {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "expect new line at the end.",
            ));
        }
        buf.pop();

        if buf != "OK" {
            return Err(Error::new(ErrorKind::ConnectionRefused, buf));
        }

        self.res = buf;

        Ok(())
    }
}
