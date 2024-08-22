use std::{
    convert::Infallible,
    io::{Error, ErrorKind, Read, Write},
};

use pathkvs_core::error::{ProtocolError, TransactionConflict};

use crate::{
    message,
    utils::{ReadEx, WriteEx},
};

pub enum Message {
    Len { key: Vec<u8> },
    Get { key: Vec<u8> },
    Set { key_len: u32, key_value: Vec<u8> },
    StartTransaction,
    Commit,
    Rollback,
}

pub trait Server {
    fn len(&mut self, key: &[u8]) -> Result<u32, Error>;
    fn read(&mut self, key: &[u8], write: impl FnOnce(&[u8])) -> Result<(), Error>;
    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn start_transaction(&mut self) -> Result<(), Error>;
    fn commit(&mut self) -> Result<Result<(), TransactionConflict>, Error>;
    fn rollback(&mut self) -> Result<(), Error>;

    fn max_len(&self) -> u32 {
        u32::MAX
    }
}

pub fn serve<T>(stream: &mut T, server: &mut impl Server) -> Result<(), Error>
where
    T: Read + Write,
{
    match serve_indefinite(stream, server) {
        Ok(infallible) => match infallible {},
        Err(error) if error.kind() == ErrorKind::ConnectionReset => Ok(()),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(()),
        Err(error) => Err(error),
    }
}
fn serve_indefinite<T>(stream: &mut T, server: &mut impl Server) -> Result<Infallible, Error>
where
    T: Read + Write,
{
    loop {
        let mut recv_command = [0];
        stream.read_exact(&mut recv_command)?;
        match recv_command[0] {
            message::LEN => {
                let max_len = server.max_len();
                let key = stream.read_vec_lengthed(max_len)?;
                let len = server.len(&key)?;
                stream.write_u8(message::LEN)?;
                stream.write_u32(len)?;
            }
            message::GET => {
                let max_len = server.max_len();
                let key = stream.read_vec_lengthed(max_len)?;
                let client_max_len = stream.read_u32()?;
                let mut write_result = None;
                server.read(&key, |bytes| {
                    let result = if bytes.len() <= client_max_len as usize {
                        stream
                            .write_u8(message::GET)
                            .and_then(|()| stream.write_vec_lengthed(bytes))
                    } else {
                        stream.write_u8(message::LIMIT_EXCEEDED)
                    };
                    write_result = Some(result);
                })?;
                match write_result {
                    Some(result) => result?,
                    None => {
                        stream.write_u8(message::GET)?;
                        stream.write_u32(0)?;
                    }
                }
            }
            message::SET => {
                let max_len = server.max_len();
                let key = stream.read_vec_lengthed(max_len)?;
                let value = stream.read_vec_lengthed(max_len)?;
                server.write(&key, &value)?;
                stream.write_u8(message::SET)?;
            }
            message::START_TRANSACTION => {
                server.start_transaction()?;
                stream.write_u8(message::START_TRANSACTION)?;
            }
            message::COMMIT => match server.commit()? {
                Ok(()) => {
                    stream.write_u8(message::COMMIT)?;
                }
                Err(TransactionConflict) => {
                    stream.write_u8(message::CONFLICT)?;
                }
            },
            message::ROLLBACK => {
                server.rollback()?;
                stream.write_u8(message::ROLLBACK)?;
            }
            byte => {
                dbg!(byte);
                return Err(ProtocolError.into());
            }
        }
    }
}
