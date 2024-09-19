use pathkvs_core::error::{TransactionConflict, TransactionError, TransposeConflict};
use std::{io::BufRead, time::Duration};

const RETURN: &str = "\x1B[1A\x1B[2K\x1B[G";

use crate::fmt::DisplayBytesEx;

pub fn client() -> Result<(), std::io::Error> {
    let conn = std::net::TcpStream::connect("127.0.0.1:6314")?;
    conn.set_read_timeout(Some(Duration::from_secs(1)))?;
    conn.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut conn = pathkvs_net::client::Connection::new(conn);
    let stdin = std::io::stdin();
    let handle = stdin.lock();
    let mut lines = handle.lines();
    let mut read_count = 0;
    let mut write_count = 0;
    conn.start_transaction()?;
    while let Some(line) = lines.next() {
        let line = line?;
        match line.split_once('=') {
            Some((key, value)) => {
                match key.split_once('*') {
                    Some((start, end)) if value.is_empty() => {
                        let scan = conn.scan(start.as_bytes(), end.as_bytes())?;
                        read_count += scan.len();
                        match scan.as_slice() {
                            [] => {
                                println!("{RETURN}scan: no matches for \"{}\"", key);
                            }
                            [(k, v)] => {
                                println!("{RETURN}scan: 1 match for \"{}\"", key);
                                println!("{}={}", k.display(), v.display());
                            }
                            scan => {
                                println!("{RETURN}scan: {} matches for \"{}\"", scan.len(), key);
                                for (k, v) in scan {
                                    println!("{}={}", k.display(), v.display());
                                }
                            }
                        }
                    }
                    Some(_) => {
                        println!("{RETURN}error: can't assign to scan");
                    }
                    None => {
                        write_count += 1;
                        conn.write(key.as_bytes(), value.as_bytes())?;
                    }
                }
                continue;
            }
            None => {}
        }
        match line.as_str() {
            "commit" => {
                match conn.commit() {
                    Ok(()) => {
                        println!(
                            "{RETURN}commited {read_count} read(s) and {write_count} write(s)"
                        );
                        read_count = 0;
                        write_count = 0;
                    }
                    Err(TransactionError::Conflict) => {
                        println!("{RETURN}commit failed, {read_count} read(s) and {write_count} write(s)");
                        read_count = 0;
                        write_count = 0;
                    }
                    Err(TransactionError::Io(error)) => {
                        return Err(error);
                    }
                }
                conn.start_transaction()?;
            }
            "rollback" => {
                println!("{RETURN}rolled back {read_count} read(s) and {write_count} write(s)");
                read_count = 0;
                write_count = 0;
                conn.rollback()?;
                conn.start_transaction()?;
            }
            pattern => {
                read_count += 1;
                match pattern.split_once('*') {
                    Some((start, end)) => {
                        let list = conn.list(start.as_bytes(), end.as_bytes())?;
                        read_count += list.len();
                        match list.as_slice() {
                            [] => {
                                println!("{RETURN}list: no matches for \"{}\"", pattern);
                            }
                            [key] => {
                                println!("{RETURN}list: 1 match for \"{}\"", pattern);
                                println!("{}", key.display());
                            }
                            list => {
                                println!("{RETURN}list: {} matches for \"{}\"", list.len(), pattern);
                                for key in list {
                                    println!("{}", key.display());
                                }
                            }
                        }
                    }
                    None => {
                        println!("{RETURN}{}={}", pattern, conn.read(pattern.as_bytes())?.display());
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn stress() -> Result<(), std::io::Error> {
    let conn = std::net::TcpStream::connect("127.0.0.1:6314")?;
    conn.set_read_timeout(Some(Duration::from_secs(1)))?;
    conn.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut conn = pathkvs_net::client::Connection::new(conn);
    let mut remaining = 500;
    while remaining > 0 {
        conn.start_transaction()?;
        let inc = conn.read_u64_opt("INC")?.unwrap_or(0);
        conn.write_u64("INC", inc + 1)?;
        match conn.commit().transpose_conflict()? {
            Ok(()) => {
                remaining -= 1;
            }
            Err(TransactionConflict) => {
                println!("conflito ao escrever {inc}");
            }
        }
    }
    Ok(())
}
