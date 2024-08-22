use pathkvs_core::error::ProtocolError;
use std::io::{Error, Read, Write};

pub trait ReadEx: Read {
    fn read_u8(&mut self) -> Result<u8, Error> {
        let mut buf = [0];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }
    fn read_u32(&mut self) -> Result<u32, Error> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
    fn read_vec(&mut self, len: usize) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        buf.reserve_exact(len);
        unsafe { buf.set_len(len) };
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
    fn read_vec_lengthed(&mut self, max_len: u32) -> Result<Vec<u8>, Error> {
        let len = self.read_u32()?;
        if len > max_len {
            return Err(ProtocolError.into());
        }
        self.read_vec(len as usize)
    }
}
impl<T: Read + ?Sized> ReadEx for T {}

pub trait WriteEx: Write {
    fn write_u8(&mut self, value: u8) -> Result<(), Error> {
        self.write_all(std::slice::from_ref(&value))
    }
    fn write_u32(&mut self, value: u32) -> Result<(), Error> {
        self.write_all(&u32::to_le_bytes(value))
    }
    fn write_vec_lengthed(&mut self, bytes: &[u8]) -> Result<(), Error> {
        assert!(bytes.len() <= u32::MAX as usize);
        let len = bytes.len() as u32;
        self.write_u32(len)?;
        self.write_all(bytes)
    }
}
impl<T: Write + ?Sized> WriteEx for T {}
