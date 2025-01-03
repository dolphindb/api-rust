mod header;
use crate::types::ConstantImpl;
use crate::{
    error::{Error, Result},
    Deserialize,
};

use self::header::ResponseHeader;
use tokio::io::AsyncBufReadExt;

#[derive(Default, Debug)]
struct ExecuteResult {
    res: String,
}

#[derive(Default, Debug)]
pub(crate) struct Response {
    pub(crate) header: ResponseHeader,
    res: ExecuteResult,
    pub(crate) data: Option<ConstantImpl>,
}

impl Deserialize for Response {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        self.header.deserialize(reader).await?;

        self.res.deserialize(reader).await?;

        match self.header.counts {
            0 => self.data = None,
            1 => {
                let mut c = ConstantImpl::default();
                c.deserialize(reader).await?;
                self.data = Some(c);
            }
            _ => return Err(Error::BadResponse("unexpected object numbers".to_string())),
        }

        Ok(())
    }

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        self.header.deserialize_le(reader).await?;

        self.res.deserialize_le(reader).await?;

        match self.header.counts {
            0 => self.data = None,
            1 => {
                let mut c = ConstantImpl::default();
                c.deserialize_le(reader).await?;
                self.data = Some(c);
            }
            _ => return Err(Error::BadResponse("unexpected object numbers".to_string())),
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
            return Err(Error::UnexpectedEof);
        }

        if !buf.ends_with('\n') {
            return Err(Error::UnexpectedEof);
        }
        buf.pop();

        if buf != "OK" {
            return Err(Error::BadResponse(format!("server response: {}", buf)));
        }

        self.res = buf;

        Ok(())
    }
}
