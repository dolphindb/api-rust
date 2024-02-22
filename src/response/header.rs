use crate::Endian;

use super::Deserialize;
use std::io::{Error, ErrorKind, Result};

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
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        while buf == "MSG\n" {
            let mut msg = Vec::new();
            if reader.read_until(0, &mut msg).await? == 0 {
                return Err(Error::from(ErrorKind::UnexpectedEof));
            }
            msg.pop();
            match String::from_utf8(msg) {
                Ok(str) => {
                    println!("{str}");
                }
                Err(err) => return Err(Error::new(ErrorKind::InvalidData, err.to_string())),
            }

            buf = String::new();
            if reader.read_line(&mut buf).await? == 0 {
                return Err(Error::from(ErrorKind::UnexpectedEof));
            }
        }

        if !buf.ends_with('\n') {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "expect new line at the end.",
            ));
        }
        buf.pop();
        let mut parts = buf.split(|data| data == ' ').collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "response header corrupted.",
            ));
        }
        parts.reverse();

        self.session_id = parts.pop().unwrap().as_bytes().to_vec();
        self.counts = parts
            .pop()
            .unwrap()
            .parse::<i32>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
            as usize;
        self.endian = match *parts.last().unwrap() {
            "0" => Endian::Big,
            "1" => Endian::Little,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "response header corrupted.",
                ))
            }
        };

        Ok(())
    }
}
