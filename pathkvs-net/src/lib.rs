pub mod client;
pub mod server;
mod utils;

mod message {
    pub const LEN: u8 = 1;
    pub const READ: u8 = 2;
    pub const WRITE: u8 = 3;
    pub const START_TRANSACTION: u8 = 4;
    pub const COMMIT: u8 = 5;
    pub const ROLLBACK: u8 = 6;
    pub const COUNT: u8 = 7;
    pub const LIST: u8 = 8;
    pub const SCAN: u8 = 9;
    pub const LIMIT_EXCEEDED: u8 = 254;
    pub const CONFLICT: u8 = 255;
}
