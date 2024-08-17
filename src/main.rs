mod fmt;

use std::io::BufRead;

use fmt::DisplayBytesEx;
use pathkvs_core::TransactionError;

fn main() -> Result<(), std::io::Error> {
    let db = pathkvs_core::Database::open("data.pathkvs")?;

    let stdin = std::io::stdin();
    let handle = stdin.lock();
    let mut lines = handle.lines();
    loop {
        let mut tr = db.start_writes();
        while let Some(line) = lines.next() {
            let line = line?;
            match line.split_once('=') {
                Some((key, value)) => {
                    tr.write(key.as_bytes(), value.as_bytes());
                    continue;
                }
                None => {}
            }
            match line.as_str() {
                "commit" => {
                    print!("commit: ");
                    match tr.commit() {
                        Ok(()) => {
                            println!("successful");
                        }
                        Err(TransactionError::Conflict) => {
                            println!("conflict");
                        }
                        Err(TransactionError::Io(error)) => {
                            println!("{error:#?}");
                        }
                    }
                    break;
                }
                "rollback" => {
                    break;
                }
                key => {
                    println!("{}", tr.read(key.as_bytes()).display());
                }
            }
        }
    }
}

/*
fn main() -> Result<(), std::io::Error> {
    let db = patkvs_core::Database::open("data.patkvs")?;
    let id = gen_id(&db)?;
    dbg!(id);
    Ok(())
}

fn gen_id(db: &patkvs_core::Database) -> Result<u32, std::io::Error> {
    let mut tr = db.start_writes();
    let accumulator = tr.read(b"accumulator");
    let value = if accumulator.is_empty() {
        0
    } else {
        u32::from_le_bytes(accumulator.try_into().expect("accumulator must have 4 bytes"))
    };
    let accumulator = (value + 1).to_le_bytes();
    tr.write(b"accumulator", &accumulator);
    tr.commit()?;
    Ok(value + 1)
}
*/

/*
fn main() -> Result<(), std::io::Error> {
    let db = patkvs_core::Database::create("data.patkvs")?;

    let mut tr1 = db.start_writes();
    tr1.write(b"key", b"value");

    let mut tr2 = db.start_writes();
    let mut value = tr2.read(b"key").to_vec();
    value.extend_from_slice(b"!!!");
    tr2.write(b"key", &value);

    tr1.commit()?;
    tr2.commit()?;

    println!("value: {:?}", db.read(b"key"));
    Ok(())
}
*/
