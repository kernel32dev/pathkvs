use std::io::Error;

#[derive(Clone, Copy)]
pub struct ProtocolError;
impl std::fmt::Debug for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pahtkvs protocol error")
    }
}
impl std::error::Error for ProtocolError {}
impl From<ProtocolError> for Error {
    fn from(value: ProtocolError) -> Self {
        Self::other(value)
    }
}

#[derive(Clone, Copy)]
pub struct TransactionConflict;
impl std::fmt::Debug for TransactionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for TransactionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pahtkvs transaction conflict")
    }
}
impl std::error::Error for TransactionConflict {}
impl From<TransactionConflict> for Error {
    fn from(value: TransactionConflict) -> Self {
        Self::other(value)
    }
}

#[derive(Clone, Copy)]
pub struct LimitExceeded;
impl std::fmt::Debug for LimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pahtkvs limit exceeded")
    }
}
impl std::error::Error for LimitExceeded {}
impl From<LimitExceeded> for Error {
    fn from(value: LimitExceeded) -> Self {
        Self::other(value)
    }
}

#[derive(Debug)]
pub enum TransactionError {
    Conflict,
    Io(Error),
}

impl From<Error> for TransactionError {
    fn from(value: Error) -> Self {
        Self::Io(value)
    }
}

impl From<TransactionError> for Error {
    fn from(value: TransactionError) -> Self {
        match value {
            TransactionError::Conflict => Error::other(TransactionConflict),
            TransactionError::Io(error) => error,
        }
    }
}

pub trait TransposeConflict {
    type Output;
    fn transpose_conflict(self) -> Result<Result<Self::Output, TransactionConflict>, Error>;
}
impl<T> TransposeConflict for Result<T, TransactionError> {
    type Output = T;
    fn transpose_conflict(self) -> Result<Result<Self::Output, TransactionConflict>, Error> {
        match self {
            Ok(x) => Ok(Ok(x)),
            Err(TransactionError::Conflict) => Ok(Err(TransactionConflict)),
            Err(TransactionError::Io(error)) => Err(error),
        }
    }
}
