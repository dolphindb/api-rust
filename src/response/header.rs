use crate::{
    error::{Error, Result},
    Endian,
};

use super::Deserialize;

#[derive(Default, Debug)]
pub(crate) struct ResponseHeader {
    pub(crate) session_id: Vec<u8>, // unspecified length in API document
    pub(crate) counts: usize,
    pub(crate) endian: Endian,
}

impl Deserialize for ResponseHeader {
    async fn deserialize<R>(&mut self, reader: &mut R) -> Result<()>
    where
        R: tokio::io::AsyncBufReadExt + Unpin,
    {
        let mut buf = String::new();

        if reader.read_line(&mut buf).await? == 0 {
            return Err(Error::UnexpectedEof);
        }

        if !buf.ends_with('\n') {
            return Err(Error::UnexpectedEof);
        }
        buf.pop();

        let mut parts = buf.split(' ').collect::<Vec<_>>();

        if parts.len() != 3 {
            return Err(Error::BadResponse(format!(
                "response header corrupted, expect 3 parts, but {} parts received",
                parts.len()
            )));
        }
        parts.reverse();

        self.session_id = parts.pop().unwrap().as_bytes().to_vec();

        self.counts = parts
            .pop()
            .unwrap()
            .parse::<i32>()
            .map_err(|e| Error::InvalidNumeric(e.to_string()))? as usize;

        self.endian = match *parts.last().unwrap() {
            "0" => Endian::Big,
            "1" => Endian::Little,
            e => {
                return Err(Error::BadResponse(format!(
                    "response header corrupted, unrecognized endian flag {}",
                    e
                )));
            }
        };

        Ok(())
    }
}
