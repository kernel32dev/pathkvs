use std::{io::Error, time::Duration};

use pathkvs_core::error::{ProtocolError, TransactionConflict, TransposeConflict};

pub fn serve() -> Result<std::convert::Infallible, Error> {
    let listener = std::net::TcpListener::bind("127.0.0.1:6314")?;
    let database = pathkvs_core::Database::open("data.pathkvs")?;
    let database = &*Box::leak(Box::new(database));
    loop {
        let (mut stream, _) = listener.accept()?;
        std::thread::spawn(move || {
            let mut server = Server::new(database);
            let result = pathkvs_net::server::serve(&mut stream, &mut server);
            match result {
                Ok(()) => {}
                Err(error) => {
                    println!("{error:#?}")
                }
            }
        });
    }
}

#[derive(Default)]
enum ServerMode {
    #[default]
    Normal,
    Transaction(pathkvs_core::Transaction<'static>),
    Snapshot(pathkvs_core::Snapshot<'static>),
}

struct Server {
    db: &'static pathkvs_core::Database,
    mode: ServerMode,
}

impl Server {
    const fn new(db: &'static pathkvs_core::Database) -> Self {
        Self {
            db,
            mode: ServerMode::Normal,
        }
    }
}

impl pathkvs_net::server::Server for Server {
    fn len(&mut self, key: &[u8]) -> Result<u32, Error> {
        match &mut self.mode {
            ServerMode::Normal => Ok(self.db.len(key)),
            ServerMode::Transaction(tr) => Ok(tr.len(key)),
            ServerMode::Snapshot(sn) => Ok(sn.len(key)),
        }
    }

    fn read(&mut self, key: &[u8], write: impl FnOnce(&[u8])) -> Result<(), Error> {
        match &mut self.mode {
            ServerMode::Normal => write(self.db.read(key)),
            ServerMode::Transaction(tr) => write(tr.read(key)),
            ServerMode::Snapshot(sn) => write(sn.read(key)),
        }
        Ok(())
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match &mut self.mode {
            ServerMode::Normal => {
                self.db.write(key, value)?;
            }
            ServerMode::Transaction(tr) => {
                tr.write(key, value);
            }
            ServerMode::Snapshot(_) => return Err(ProtocolError.into()),
        }
        Ok(())
    }

    fn start_transaction(&mut self) -> Result<(), Error> {
        self.rollback()?;
        self.mode = ServerMode::Transaction(self.db.start_writes());
        Ok(())
    }

    fn commit(&mut self) -> Result<Result<Option<Duration>, TransactionConflict>, Error> {
        match std::mem::take(&mut self.mode) {
            ServerMode::Normal => Ok(Ok(None)),
            ServerMode::Transaction(tr) => tr.commit().transpose_conflict().map(|x| x.map(Some)),
            ServerMode::Snapshot(_) => Ok(Ok(None)),
        }
    }

    fn rollback(&mut self) -> Result<(), Error> {
        match std::mem::take(&mut self.mode) {
            ServerMode::Normal => {},
            ServerMode::Transaction(tr) => {tr.rollback();},
            ServerMode::Snapshot(_) => {},
        }
        Ok(())
    }

    fn count(&mut self, start: &[u8], end: &[u8]) -> Result<u32, Error> {
        match &mut self.mode {
            ServerMode::Normal => {
                Ok(self.db.count(start, end))
            }
            ServerMode::Transaction(tr) => {
                Ok(tr.count(start, end))
            }
            ServerMode::Snapshot(sn) => {
                Ok(sn.count(start, end))
            },
        }
    }
    fn list(
        &mut self,
        start: &[u8],
        end: &[u8],
        write: impl FnOnce(&[&[u8]]),
    ) -> Result<(), Error> {
        match &mut self.mode {
            ServerMode::Normal => {
                write(&self.db.list(start, end));
            }
            ServerMode::Transaction(tr) => {
                write(&tr.list(start, end));
            }
            ServerMode::Snapshot(sn) => {
                write(&sn.list(start, end));
            },
        }
        Ok(())
    }
    fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        write: impl FnOnce(&[(&[u8], &[u8])]),
    ) -> Result<(), Error> {
        match &mut self.mode {
            ServerMode::Normal => {
                write(&self.db.scan(start, end));
            }
            ServerMode::Transaction(tr) => {
                write(&tr.scan(start, end));
            }
            ServerMode::Snapshot(sn) => {
                write(&sn.scan(start, end));
            },
        }
        Ok(())
    }

    fn start_snapshot(&mut self, past_unix_time: Option<std::time::Duration>) -> Result<(), Error> {
        self.rollback()?;
        let sn = match past_unix_time {
            Some(past_unix_time) => self.db.past_unix_time_snapshot_with(past_unix_time),
            None => self.db.snapshot(),
        };
        self.mode = ServerMode::Snapshot(sn);
        Ok(())
    }
}
