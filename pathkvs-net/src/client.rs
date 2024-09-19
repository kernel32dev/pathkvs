use std::io::{Error, ErrorKind, Read, Write};

use pathkvs_core::error::{LimitExceeded, ProtocolError, TransactionError};

use crate::{
    message,
    utils::{ReadEx, WriteEx},
};

pub struct Connection<T> {
    conn: T,
}

impl<T> Connection<T>
where
    T: Read + Write,
{
    pub fn new(inner: T) -> Self {
        Self { conn: inner }
    }
    pub fn get_inner(&mut self) -> &mut T {
        &mut self.conn
    }
    pub fn len(&mut self, key: impl AsRef<[u8]>) -> Result<u32, Error> {
        let key = key.as_ref();
        if key.is_empty() {
            return Ok(0);
        }
        assert!(key.len() <= u32::MAX as usize);
        self.conn.write_u8(message::LEN)?;
        self.conn.write_u32(key.len() as u32)?;
        self.conn.write_all(key)?;
        self.conn.flush()?;
        if self.conn.read_u8()? != message::LEN {
            return Err(ProtocolError.into());
        }
        self.conn.read_u32()
    }
    pub fn read(&mut self, key: impl AsRef<[u8]>) -> Result<Vec<u8>, Error> {
        let key = key.as_ref();
        self.read_limited(key, u32::MAX)
    }
    pub fn read_limited(&mut self, key: impl AsRef<[u8]>, max_len: u32) -> Result<Vec<u8>, Error> {
        let key = key.as_ref();
        match self.read_limited_opt(key, max_len).transpose() {
            Some(result) => result,
            None => Err(LimitExceeded.into()),
        }
    }
    pub fn read_limited_opt(
        &mut self,
        key: impl AsRef<[u8]>,
        max_len: u32,
    ) -> Result<Option<Vec<u8>>, Error> {
        let key = key.as_ref();
        if key.is_empty() {
            return Ok(Some(Vec::new()));
        }
        assert!(key.len() <= u32::MAX as usize);
        self.conn.write_u8(message::READ)?;
        self.conn.write_u32(key.len() as u32)?;
        self.conn.write_all(key)?;
        self.conn.write_u32(max_len)?;
        self.conn.flush()?;
        match self.conn.read_u8()? {
            message::READ => {
                let recv_len = self.conn.read_u32()?;
                if recv_len <= max_len {
                    self.conn.read_vec(recv_len as usize).map(Some)
                } else {
                    Err(ProtocolError.into())
                }
            }
            message::LIMIT_EXCEEDED => Ok(None),
            _ => Err(ProtocolError.into()),
        }
    }
    pub fn write(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), Error> {
        let key = key.as_ref();
        if key.is_empty() {
            return Ok(());
        }
        let value = value.as_ref();
        assert!(key.len() <= u32::MAX as usize);
        assert!(value.len() <= u32::MAX as usize);
        self.conn.write_u8(message::WRITE)?;
        self.conn.write_u32(key.len() as u32)?;
        self.conn.write_all(key)?;
        self.conn.write_u32(value.len() as u32)?;
        self.conn.write_all(value)?;
        self.conn.flush()?;
        if self.conn.read_u8()? != message::WRITE {
            return Err(ProtocolError.into());
        }
        Ok(())
    }
    pub fn start_transaction(&mut self) -> Result<(), Error> {
        self.conn.write_u8(message::START_TRANSACTION)?;
        self.conn.flush()?;
        if self.conn.read_u8()? != message::START_TRANSACTION {
            return Err(ProtocolError.into());
        }
        Ok(())
    }
    pub fn commit(&mut self) -> Result<(), TransactionError> {
        self.conn.write_u8(message::COMMIT)?;
        self.conn.flush()?;
        match self.conn.read_u8()? {
            message::COMMIT => Ok(()),
            message::CONFLICT => Err(TransactionError::Conflict),
            _ => Err(TransactionError::Io(ProtocolError.into())),
        }
    }
    pub fn rollback(&mut self) -> Result<(), Error> {
        self.conn.write_u8(message::ROLLBACK)?;
        self.conn.flush()?;
        if self.conn.read_u8()? != message::ROLLBACK {
            return Err(ProtocolError.into());
        }
        Ok(())
    }
    pub fn count(&mut self, start: impl AsRef<[u8]>, end: impl AsRef<[u8]>) -> Result<u32, Error> {
        let start = start.as_ref();
        let end = end.as_ref();
        assert!(start
            .len()
            .checked_add(end.len())
            .is_some_and(|x| x <= u32::MAX as usize));
        self.conn.write_u8(message::COUNT)?;
        self.conn.write_u32(start.len() as u32)?;
        self.conn.write_all(start)?;
        self.conn.write_u32(end.len() as u32)?;
        self.conn.write_all(end)?;
        self.conn.flush()?;
        if self.conn.read_u8()? != message::COUNT {
            return Err(ProtocolError.into());
        }
        self.conn.read_u32()
    }
    pub fn list(
        &mut self,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let start = start.as_ref();
        let end = end.as_ref();
        self.list_limited(start, end, u32::MAX)
    }
    pub fn list_limited(
        &mut self,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
        max_len: u32,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let start = start.as_ref();
        let end = end.as_ref();
        match self.list_limited_opt(start, end, max_len).transpose() {
            Some(result) => result,
            None => Err(LimitExceeded.into()),
        }
    }
    pub fn list_limited_opt(
        &mut self,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
        max_len: u32,
    ) -> Result<Option<Vec<Vec<u8>>>, Error> {
        let start = start.as_ref();
        let end = end.as_ref();
        assert!(start
            .len()
            .checked_add(end.len())
            .is_some_and(|x| x <= u32::MAX as usize));
        self.conn.write_u8(message::LIST)?;
        self.conn.write_u32(start.len() as u32)?;
        self.conn.write_all(start)?;
        self.conn.write_u32(end.len() as u32)?;
        self.conn.write_all(end)?;
        self.conn.write_u32(max_len)?;
        self.conn.flush()?;
        match self.conn.read_u8()? {
            message::LIST => {
                let mut total = Some(0u32);
                let mut rows = Vec::new();
                let rowc = self.conn.read_u32()?;
                rows.reserve_exact(rowc as usize);
                for _ in 0..rowc {
                    let recv_len = self.conn.read_u32()?;
                    total = total.and_then(|x| x.checked_add(recv_len));
                    if total.is_some_and(|total| total <= max_len) {
                        rows.push(self.conn.read_vec(recv_len as usize)?);
                    } else {
                        return Err(ProtocolError.into());
                    }
                }
                Ok(Some(rows))
            }
            message::LIMIT_EXCEEDED => Ok(None),
            _ => Err(ProtocolError.into()),
        }
    }
    pub fn scan(
        &mut self,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let start = start.as_ref();
        let end = end.as_ref();
        self.scan_limited(start, end, u32::MAX)
    }
    pub fn scan_limited(
        &mut self,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
        max_len: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let start = start.as_ref();
        let end = end.as_ref();
        match self.scan_limited_opt(start, end, max_len).transpose() {
            Some(result) => result,
            None => Err(LimitExceeded.into()),
        }
    }
    pub fn scan_limited_opt(
        &mut self,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
        max_len: u32,
    ) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>, Error> {
        let start = start.as_ref();
        let end = end.as_ref();
        assert!(start
            .len()
            .checked_add(end.len())
            .is_some_and(|x| x <= u32::MAX as usize));
        self.conn.write_u8(message::SCAN)?;
        self.conn.write_u32(start.len() as u32)?;
        self.conn.write_all(start)?;
        self.conn.write_u32(end.len() as u32)?;
        self.conn.write_all(end)?;
        self.conn.write_u32(max_len)?;
        self.conn.flush()?;
        match self.conn.read_u8()? {
            message::SCAN => {
                let mut total = Some(0u32);
                let mut rows = Vec::new();
                let rowc = self.conn.read_u32()?;
                rows.reserve_exact(rowc as usize);
                for _ in 0..rowc {
                    let recv_len = self.conn.read_u32()?;
                    total = total.and_then(|x| x.checked_add(recv_len));
                    if !total.is_some_and(|total| total <= max_len) {
                        return Err(ProtocolError.into());
                    }
                    let key = self.conn.read_vec(recv_len as usize)?;

                    let recv_len = self.conn.read_u32()?;
                    total = total.and_then(|x| x.checked_add(recv_len));
                    if !total.is_some_and(|total| total <= max_len) {
                        return Err(ProtocolError.into());
                    }
                    let value = self.conn.read_vec(recv_len as usize)?;

                    rows.push((key, value));
                }
                Ok(Some(rows))
            }
            message::LIMIT_EXCEEDED => Ok(None),
            _ => Err(ProtocolError.into()),
        }
    }

    pub fn read_str(&mut self, key: impl AsRef<[u8]>) -> Result<String, Error> {
        let key = key.as_ref();
        self.read_str_limited(key, u32::MAX)
    }
    pub fn read_str_limited(
        &mut self,
        key: impl AsRef<[u8]>,
        max_len: u32,
    ) -> Result<String, Error> {
        let key = key.as_ref();
        match String::from_utf8(self.read_limited(key, max_len)?) {
            Ok(text) => Ok(text),
            Err(error) => Err(Error::other(error)),
        }
    }

    pub fn read_u8_bin(&mut self, key: impl AsRef<[u8]>) -> Result<u8, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 1)?.try_into() {
            Ok(array) => Ok(u8::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u8")),
        }
    }
    pub fn read_u16_bin(&mut self, key: impl AsRef<[u8]>) -> Result<u16, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 2)?.try_into() {
            Ok(array) => Ok(u16::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u16")),
        }
    }
    pub fn read_u32_bin(&mut self, key: impl AsRef<[u8]>) -> Result<u32, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 4)?.try_into() {
            Ok(array) => Ok(u32::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u32")),
        }
    }
    pub fn read_u64_bin(&mut self, key: impl AsRef<[u8]>) -> Result<u64, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 8)?.try_into() {
            Ok(array) => Ok(u64::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u64")),
        }
    }
    pub fn read_u128_bin(&mut self, key: impl AsRef<[u8]>) -> Result<u128, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 16)?.try_into() {
            Ok(array) => Ok(u128::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u128")),
        }
    }
    pub fn read_i8_bin(&mut self, key: impl AsRef<[u8]>) -> Result<i8, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 1)?.try_into() {
            Ok(array) => Ok(i8::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u8")),
        }
    }
    pub fn read_i16_bin(&mut self, key: impl AsRef<[u8]>) -> Result<i16, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 2)?.try_into() {
            Ok(array) => Ok(i16::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u16")),
        }
    }
    pub fn read_i32_bin(&mut self, key: impl AsRef<[u8]>) -> Result<i32, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 4)?.try_into() {
            Ok(array) => Ok(i32::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u32")),
        }
    }
    pub fn read_i64_bin(&mut self, key: impl AsRef<[u8]>) -> Result<i64, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 8)?.try_into() {
            Ok(array) => Ok(i64::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u64")),
        }
    }
    pub fn read_i128_bin(&mut self, key: impl AsRef<[u8]>) -> Result<i128, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 16)?.try_into() {
            Ok(array) => Ok(i128::from_le_bytes(array)),
            Err(_) => Err(invalid("expected binary u128")),
        }
    }

    pub fn read_u8(&mut self, key: impl AsRef<[u8]>) -> Result<u8, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 3)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u8"))
    }
    pub fn read_u16(&mut self, key: impl AsRef<[u8]>) -> Result<u16, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 5)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u16"))
    }
    pub fn read_u32(&mut self, key: impl AsRef<[u8]>) -> Result<u32, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 10)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u32"))
    }
    pub fn read_u64(&mut self, key: impl AsRef<[u8]>) -> Result<u64, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 20)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u64"))
    }
    pub fn read_u128(&mut self, key: impl AsRef<[u8]>) -> Result<u128, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 39)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u128"))
    }
    pub fn read_i8(&mut self, key: impl AsRef<[u8]>) -> Result<i8, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 4)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i8"))
    }
    pub fn read_i16(&mut self, key: impl AsRef<[u8]>) -> Result<i16, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 6)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i16"))
    }
    pub fn read_i32(&mut self, key: impl AsRef<[u8]>) -> Result<i32, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 11)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i32"))
    }
    pub fn read_i64(&mut self, key: impl AsRef<[u8]>) -> Result<i64, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 20)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i64"))
    }
    pub fn read_i128(&mut self, key: impl AsRef<[u8]>) -> Result<i128, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 40)?;
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i128"))
    }

    pub fn read_u8_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u8>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 1)?.try_into() {
            Ok(array) => Ok(Some(u8::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u8")),
        }
    }
    pub fn read_u16_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u16>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 2)?.try_into() {
            Ok(array) => Ok(Some(u16::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u16")),
        }
    }
    pub fn read_u32_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u32>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 4)?.try_into() {
            Ok(array) => Ok(Some(u32::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u32")),
        }
    }
    pub fn read_u64_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u64>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 8)?.try_into() {
            Ok(array) => Ok(Some(u64::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u64")),
        }
    }
    pub fn read_u128_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u128>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 16)?.try_into() {
            Ok(array) => Ok(Some(u128::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u128")),
        }
    }
    pub fn read_i8_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i8>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 1)?.try_into() {
            Ok(array) => Ok(Some(i8::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u8")),
        }
    }
    pub fn read_i16_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i16>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 2)?.try_into() {
            Ok(array) => Ok(Some(i16::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u16")),
        }
    }
    pub fn read_i32_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i32>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 4)?.try_into() {
            Ok(array) => Ok(Some(i32::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u32")),
        }
    }
    pub fn read_i64_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i64>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 8)?.try_into() {
            Ok(array) => Ok(Some(i64::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u64")),
        }
    }
    pub fn read_i128_bin_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i128>, Error> {
        let key = key.as_ref();
        match self.read_limited(key, 16)?.try_into() {
            Ok(array) => Ok(Some(i128::from_le_bytes(array))),
            Err(error) if error.is_empty() => Ok(None),
            Err(_) => Err(invalid("expected binary u128")),
        }
    }

    pub fn read_u8_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u8>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 3)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u8"))
            .map(Some)
    }
    pub fn read_u16_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u16>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 5)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u16"))
            .map(Some)
    }
    pub fn read_u32_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u32>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 10)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u32"))
            .map(Some)
    }
    pub fn read_u64_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u64>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 20)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u64"))
            .map(Some)
    }
    pub fn read_u128_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<u128>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 39)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected u128"))
            .map(Some)
    }
    pub fn read_i8_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i8>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 4)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i8"))
            .map(Some)
    }
    pub fn read_i16_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i16>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 6)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i16"))
            .map(Some)
    }
    pub fn read_i32_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i32>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 11)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i32"))
            .map(Some)
    }
    pub fn read_i64_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i64>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 20)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i64"))
            .map(Some)
    }
    pub fn read_i128_opt(&mut self, key: impl AsRef<[u8]>) -> Result<Option<i128>, Error> {
        let key = key.as_ref();
        let bytes = self.read_limited_opt(key, 40)?;
        if bytes.as_ref().is_some_and(|x| x.is_empty()) {
            return Ok(None);
        }
        bytes
            .as_deref()
            .and_then(|x| std::str::from_utf8(x).ok())
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| invalid("expected i128"))
            .map(Some)
    }

    pub fn clear(&mut self, key: impl AsRef<[u8]>) -> Result<(), Error> {
        self.write(key, &[])
    }
    pub fn write_u8_bin(&mut self, key: impl AsRef<[u8]>, value: u8) -> Result<(), Error> {
        self.write(key, u8::to_le_bytes(value))
    }
    pub fn write_u16_bin(&mut self, key: impl AsRef<[u8]>, value: u16) -> Result<(), Error> {
        self.write(key, u16::to_le_bytes(value))
    }
    pub fn write_u32_bin(&mut self, key: impl AsRef<[u8]>, value: u32) -> Result<(), Error> {
        self.write(key, u32::to_le_bytes(value))
    }
    pub fn write_u64_bin(&mut self, key: impl AsRef<[u8]>, value: u64) -> Result<(), Error> {
        self.write(key, u64::to_le_bytes(value))
    }
    pub fn write_u128_bin(&mut self, key: impl AsRef<[u8]>, value: u128) -> Result<(), Error> {
        self.write(key, u128::to_le_bytes(value))
    }
    pub fn write_i8_bin(&mut self, key: impl AsRef<[u8]>, value: i8) -> Result<(), Error> {
        self.write(key, i8::to_le_bytes(value))
    }
    pub fn write_i16_bin(&mut self, key: impl AsRef<[u8]>, value: i16) -> Result<(), Error> {
        self.write(key, i16::to_le_bytes(value))
    }
    pub fn write_i32_bin(&mut self, key: impl AsRef<[u8]>, value: i32) -> Result<(), Error> {
        self.write(key, i32::to_le_bytes(value))
    }
    pub fn write_i64_bin(&mut self, key: impl AsRef<[u8]>, value: i64) -> Result<(), Error> {
        self.write(key, i64::to_le_bytes(value))
    }
    pub fn write_i128_bin(&mut self, key: impl AsRef<[u8]>, value: i128) -> Result<(), Error> {
        self.write(key, i128::to_le_bytes(value))
    }
    pub fn write_fmt(
        &mut self,
        key: impl AsRef<[u8]>,
        value: std::fmt::Arguments,
    ) -> Result<(), Error> {
        use std::fmt::Write;
        let mut buf = buf::<55>();
        buf.write_fmt(value).unwrap();
        self.write(key, &buf)
    }
    pub fn write_u8(&mut self, key: impl AsRef<[u8]>, value: u8) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_u16(&mut self, key: impl AsRef<[u8]>, value: u16) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_u32(&mut self, key: impl AsRef<[u8]>, value: u32) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_u64(&mut self, key: impl AsRef<[u8]>, value: u64) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_u128(&mut self, key: impl AsRef<[u8]>, value: u128) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_i8(&mut self, key: impl AsRef<[u8]>, value: i8) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_i16(&mut self, key: impl AsRef<[u8]>, value: i16) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_i32(&mut self, key: impl AsRef<[u8]>, value: i32) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_i64(&mut self, key: impl AsRef<[u8]>, value: i64) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
    pub fn write_i128(&mut self, key: impl AsRef<[u8]>, value: i128) -> Result<(), Error> {
        self.write_fmt(key, format_args!("{value}"))
    }
}

fn invalid(description: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, description)
}

const fn buf<const N: usize>() -> BufWriter<N> {
    BufWriter::Stack {
        cursor: 0,
        bytes: [0; N],
    }
}
enum BufWriter<const N: usize> {
    Stack { cursor: usize, bytes: [u8; N] },
    Heap { cursor: usize, bytes: Vec<u8> },
}
impl<const N: usize> AsRef<[u8]> for BufWriter<N> {
    fn as_ref(&self) -> &[u8] {
        match self {
            BufWriter::Stack { cursor, bytes } => &bytes[..*cursor],
            BufWriter::Heap { cursor, bytes } => &bytes[..*cursor],
        }
    }
}
impl<const N: usize> std::fmt::Write for BufWriter<N> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        match self {
            BufWriter::Stack { cursor, bytes } => {
                let mut vector = Vec::with_capacity(N.next_power_of_two());
                vector.extend_from_slice(bytes);
                *self = BufWriter::Heap {
                    cursor: *cursor,
                    bytes: vector,
                };
            }
            BufWriter::Heap { .. } => {}
        }
        match self {
            BufWriter::Stack { cursor, bytes } => {
                bytes[*cursor..*cursor + s.len()].copy_from_slice(s.as_bytes());
                *cursor += s.len();
            }
            BufWriter::Heap { cursor, bytes } => {
                bytes[*cursor..*cursor + s.len()].copy_from_slice(s.as_bytes());
                *cursor += s.len();
            }
        }
        Ok(())
    }
}
