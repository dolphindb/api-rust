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
        async fn read_parsed_line<R>(reader: &mut R) -> Result<Vec<String>>
        where
            R: tokio::io::AsyncBufRead + Unpin,
        {
            use tokio::io::AsyncBufReadExt;

            let mut buf = String::new();

            if reader.read_line(&mut buf).await? == 0 {
                return Err(Error::UnexpectedEof);
            }

            if !buf.ends_with('\n') {
                return Err(Error::UnexpectedEof);
            }
            buf.pop();

            let parts = buf
                .split(' ')
                .map(|s| s.to_string())
                .collect::<Vec<String>>();

            Ok(parts)
        }

        let mut parts = read_parsed_line(reader).await?;

        while parts.len() == 1 && parts[0] == "MSG" {
            let mut message_buf: Vec<u8> = Vec::new();
            reader.read_until(0, &mut message_buf).await?;
            if message_buf.last() == Some(&0) {
                message_buf.pop();
            }
            let s = String::from_utf8(message_buf).map_err(|e| Error::InvalidUtf8Encoding(e))?;
            println!("{}", s);

            parts = read_parsed_line(reader).await?;
        }

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

        self.endian = match parts.last().unwrap().as_str() {
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
