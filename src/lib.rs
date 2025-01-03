use bytes::BufMut;
use tokio::io::AsyncBufReadExt;

pub mod client;
pub mod error;
mod request;
mod response;
pub mod types;

use error::Result;

pub use request::BehaviorOptions;

pub(crate) trait Serialize {
    /// serialize data to buffer, may return length that should be written into commandLength field
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut;

    /// similar to `serialize()`, but in little endian.
    fn serialize_le<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: BufMut,
    {
        self.serialize(buffer)
    }
}

pub(crate) trait Deserialize {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin;

    async fn deserialize_le<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: AsyncBufReadExt + Unpin,
    {
        self.deserialize(reader).await
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub(crate) enum Endian {
    #[default]
    Little,
    Big,
}

impl Serialize for Endian {
    fn serialize<B>(&self, buffer: &mut B) -> Result<usize>
    where
        B: bytes::BufMut,
    {
        match self {
            Endian::Little => buffer.put_u8(b'1'),
            Endian::Big => buffer.put_u8(b'0'),
        };

        Ok(0)
    }
}
