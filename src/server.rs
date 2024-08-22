use std::io::Error;

use pathkvs_core::error::{TransactionConflict, TransposeConflict};

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

struct Server {
    db: &'static pathkvs_core::Database,
    tr: Option<pathkvs_core::Transaction<'static>>,
}

impl Server {
    const fn new(db: &'static pathkvs_core::Database) -> Self {
        Self { db, tr: None }
    }
}

impl pathkvs_net::server::Server for Server {
    fn len(&mut self, key: &[u8]) -> Result<u32, Error> {
        match &mut self.tr {
            Some(tr) => Ok(tr.len(key)),
            None => Ok(self.db.len(key)),
        }
    }

    fn read(&mut self, key: &[u8], write: impl FnOnce(&[u8])) -> Result<(), Error> {
        match &mut self.tr {
            Some(tr) => {
                write(tr.read(key));
            }
            None => {
                write(self.db.read(key));
            }
        }
        Ok(())
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match &mut self.tr {
            Some(tr) => {
                tr.write(key, value);
            }
            None => {
                self.db.write(key, value)?;
            }
        }
        Ok(())
    }

    fn start_transaction(&mut self) -> Result<(), Error> {
        self.rollback()?;
        self.tr = Some(self.db.start_writes());
        Ok(())
    }

    fn commit(&mut self) -> Result<Result<(), TransactionConflict>, Error> {
        match self.tr.take() {
            Some(tr) => tr.commit().transpose_conflict(),
            None => Ok(Ok(())),
        }
    }

    fn rollback(&mut self) -> Result<(), Error> {
        if let Some(tr) = self.tr.take() {
            tr.rollback();
        }
        Ok(())
    }
}
