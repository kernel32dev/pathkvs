use std::{
    convert::Infallible,
    io::{Error, ErrorKind, Read, Write},
};

use pathkvs_core::error::{ProtocolError, TransactionConflict};

use crate::{
    message,
    utils::{ReadEx, WriteEx},
};

pub trait Server {
    fn len(&mut self, key: &[u8]) -> Result<u32, Error>;
    fn read(&mut self, key: &[u8], write: impl FnOnce(&[u8])) -> Result<(), Error>;
    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn start_transaction(&mut self) -> Result<(), Error>;
    fn commit(&mut self) -> Result<Result<(), TransactionConflict>, Error>;
    fn rollback(&mut self) -> Result<(), Error>;
    fn count(&mut self, start: &[u8], end: &[u8]) -> Result<u32, Error>;
    fn list(&mut self, start: &[u8], end: &[u8], write: impl FnOnce(&[&[u8]]))
        -> Result<(), Error>;
    fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        write: impl FnOnce(&[(&[u8], &[u8])]),
    ) -> Result<(), Error>;

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
            message::READ => {
                let max_len = server.max_len();
                let key = stream.read_vec_lengthed(max_len)?;
                let client_max_len = stream.read_u32()?;
                let mut result = None;
                server.read(&key, |bytes| {
                    result = Some((|| {
                        if bytes.len() <= client_max_len as usize {
                            stream.write_u8(message::READ)?;
                            stream.write_vec_lengthed(bytes)?;
                        } else {
                            stream.write_u8(message::LIMIT_EXCEEDED)?;
                        }
                        Ok::<_, Error>(())
                    })());
                })?;
                match result {
                    Some(result) => result?,
                    None => {
                        stream.write_u8(message::READ)?;
                        stream.write_u32(0)?;
                    }
                }
            }
            message::WRITE => {
                let max_len = server.max_len();
                let key = stream.read_vec_lengthed(max_len)?;
                let value = stream.read_vec_lengthed(max_len)?;
                server.write(&key, &value)?;
                stream.write_u8(message::WRITE)?;
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
            message::COUNT => {
                let max_len = server.max_len();
                let start = stream.read_vec_lengthed(max_len)?;
                let end = stream.read_vec_lengthed(max_len)?;
                let count = server.count(&start, &end)?;
                stream.write_u8(message::COUNT)?;
                stream.write_u32(count)?;
            }
            message::LIST => {
                let max_len = server.max_len();
                let start = stream.read_vec_lengthed(max_len)?;
                let end = stream.read_vec_lengthed(max_len)?;
                let client_max_len = stream.read_u32()?;
                let mut result = None;
                server.list(&start, &end, |list| {
                    result = Some((|| {
                        let total = list.iter().map(|x| x.len()).fold(Some(0usize), |acc, x| {
                            acc.and_then(|acc| acc.checked_add(x))
                        });
                        if total.is_some_and(|x| x < client_max_len as usize) {
                            stream.write_u8(message::LIST)?;
                            stream.write_u32(list.len() as u32)?;
                            for i in list {
                                stream.write_vec_lengthed(i)?;
                            }
                        } else {
                            stream.write_u8(message::LIMIT_EXCEEDED)?;
                        }
                        Ok::<_, Error>(())
                    })());
                })?;
                match result {
                    Some(result) => result?,
                    None => {
                        stream.write_u8(message::LIST)?;
                        stream.write_u32(0)?;
                    }
                }
            }
            message::SCAN => {
                let max_len = server.max_len();
                let start = stream.read_vec_lengthed(max_len)?;
                let end = stream.read_vec_lengthed(max_len)?;
                let client_max_len = stream.read_u32()?;
                let mut result = None;
                server.scan(&start, &end, |scan| {
                    result = Some((|| {
                        let total = scan
                            .iter()
                            .flat_map(|(k, v)| [k.len(), v.len()])
                            .fold(Some(0usize), |acc, x| {
                                acc.and_then(|acc| acc.checked_add(x))
                            });
                        if total.is_some_and(|x| x < client_max_len as usize) {
                            stream.write_u8(message::SCAN)?;
                            stream.write_u32(scan.len() as u32)?;
                            for (k, v) in scan {
                                stream.write_vec_lengthed(k)?;
                                stream.write_vec_lengthed(v)?;
                            }
                        } else {
                            stream.write_u8(message::LIMIT_EXCEEDED)?;
                        }
                        Ok::<_, Error>(())
                    })());
                })?;
                match result {
                    Some(result) => result?,
                    None => {
                        stream.write_u8(message::SCAN)?;
                        stream.write_u32(0)?;
                    }
                }
            }
            byte => {
                dbg!(byte);
                return Err(ProtocolError.into());
            }
        }
    }
}
