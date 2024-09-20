use chrono::{DateTime, Local};
use pathkvs_core::error::{TransactionConflict, TransactionError, TransposeConflict};
use pathkvs_net::client::ConnectionMode;
use std::{
    io::BufRead,
    time::{Duration, SystemTime},
};

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
                                println!("{RETURN}{}: no matches", key);
                            }
                            [(k, v)] => {
                                println!("{RETURN}{}: 1 match", key);
                                println!("{}={}", k.display(), v.display());
                            }
                            scan => {
                                println!("{RETURN}{}: {} matches", key, scan.len());
                                for (k, v) in scan {
                                    println!("{}={}", k.display(), v.display());
                                }
                            }
                        }
                    }
                    Some(_) => {
                        println!("{RETURN}error: can't assign to scan");
                    }
                    None if conn.mode().is_snapshot() => {
                        println!("{RETURN}error: can't write to snapshot");
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
            "start" => {
                conn.start_transaction()?;
                match conn.mode() {
                    ConnectionMode::Normal => println!("{RETURN}started transaction"),
                    ConnectionMode::Transaction => println!("{RETURN}started transaction, rolled back previous transaction"),
                    ConnectionMode::Snapshot => println!("{RETURN}started transaction, finished previous transaction"),
                }
            }
            line if line.starts_with("snap") => {
                let timestamp = line[4..].trim();
                if timestamp.is_empty() {
                    match conn.mode() {
                        ConnectionMode::Normal => println!("{RETURN}snapshoted database"),
                        ConnectionMode::Transaction => println!("{RETURN}snapshoted database, rolled back previous transaction"),
                        ConnectionMode::Snapshot => println!("{RETURN}snapshoted database, finished previous transaction"),
                    }
                    println!("{RETURN}snapshoted database");
                    conn.start_snapshot(None)?;
                } else if let Some(time) = parse_general_timestamp(timestamp) {
                    let display = DateTime::<Local>::from(time).format("%Y-%m-%d %H:%M:%S");
                    match conn.mode() {
                        ConnectionMode::Normal => println!("{RETURN}snapshoted past database at {display}"),
                        ConnectionMode::Transaction => println!("{RETURN}snapshoted past database at {display}, rolled back previous transaction"),
                        ConnectionMode::Snapshot => println!("{RETURN}snapshoted past database at {display}, finished previous transaction"),
                    }
                    conn.start_snapshot(Some(time))?;
                } else {
                    println!("invalid timestamp, valid formats:");
                    println!("YYYY-MM-DD HH:MM:SS.mmm");
                    println!("YYYY-MM-DD HH:MM:SS");
                    println!("YYYY-MM-DD");
                    println!("HH:MM:SS");
                    println!("HH:MM");
                    println!("-Xms");
                    println!("-Xs");
                    println!("-Xm");
                    println!("-Xh");
                    println!("-Xd");
                    println!("-Xw");
                }
            }
            "commit" => match conn.mode() {
                ConnectionMode::Normal => {
                    println!("{RETURN}commited nothing, not in a transaction");
                }
                ConnectionMode::Transaction => match conn.commit() {
                    Ok(Some(commit_time)) => {
                        let commit_time =
                            DateTime::<Local>::from(commit_time).format("%Y-%m-%d %H:%M:%S");
                        println!(
                            "{RETURN}commited {read_count} read(s) and {write_count} write(s) at {commit_time}"
                        );
                        read_count = 0;
                        write_count = 0;
                    }
                    Ok(None) => {
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
                },
                ConnectionMode::Snapshot => {
                    println!("{RETURN}commited nothing, finished snapshot");
                }
            },
            "rollback" => match conn.mode() {
                ConnectionMode::Normal => {
                    println!("{RETURN}rolled back nothing, not in a transaction");
                }
                ConnectionMode::Transaction => {
                    println!("{RETURN}rolled back {read_count} read(s) and {write_count} write(s)");
                    read_count = 0;
                    write_count = 0;
                    conn.rollback()?;
                }
                ConnectionMode::Snapshot => {
                    println!("{RETURN}rolled back nothing, finished snapshot");
                }
            },
            "quit" | "exit" | "bye" => {
                break;
            }
            pattern => {
                read_count += 1;
                match pattern.split_once('*') {
                    Some((start, end)) => {
                        let list = conn.list(start.as_bytes(), end.as_bytes())?;
                        read_count += list.len();
                        match list.as_slice() {
                            [] => {
                                println!("{RETURN}{}: no matches", pattern);
                            }
                            [key] => {
                                println!("{RETURN}{}: 1 match", pattern);
                                println!("{}", key.display());
                            }
                            list => {
                                println!("{RETURN}{}: {} matches", pattern, list.len());
                                for key in list {
                                    println!("{}", key.display());
                                }
                            }
                        }
                    }
                    None => {
                        println!(
                            "{RETURN}{}={}",
                            pattern,
                            conn.read(pattern.as_bytes())?.display()
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn stress(count: u64) -> Result<(), std::io::Error> {
    let conn = std::net::TcpStream::connect("127.0.0.1:6314")?;
    conn.set_read_timeout(Some(Duration::from_secs(1)))?;
    conn.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut conn = pathkvs_net::client::Connection::new(conn);
    let mut remaining = count;
    let start = std::time::Instant::now();
    while remaining > 0 {
        conn.start_transaction()?;
        let inc = conn.read_u64_opt("INC")?.unwrap_or(0);
        conn.write_u64("INC", inc + 1)?;
        match conn.commit().transpose_conflict()? {
            Ok(_) => {
                remaining -= 1;
            }
            Err(TransactionConflict) => {
                println!("conflito ao escrever {inc}");
            }
        }
    }
    println!("incrementado INC {count} vezes em {:#?}", start.elapsed());
    Ok(())
}

fn parse_general_timestamp(input: &str) -> Option<SystemTime> {
    let input = input.trim();
    if input.starts_with('-') {
        return parse_duration(&input[1..]).and_then(|x| SystemTime::now().checked_sub(x));
    }
    let patterns = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%H:%M:%S",
        "%H:%M",
    ];

    for pattern in patterns {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(input, pattern) {
            return naive.and_local_timezone(Local).earliest().map(|x| x.into());
        }
    }
    None
}
fn parse_duration(input: &str) -> Option<Duration> {
    let input = input.trim().to_lowercase();

    let parse_value_and_unit = {
        let mut chars = input.chars();
        let mut value = String::new();

        while let Some(c) = chars.next() {
            if c.is_digit(10) || c == '.' {
                value.push(c);
            } else {
                break;
            }
        }

        let unit = chars.as_str().trim();
        value.parse::<u64>().ok().map(|val| (val, unit))
    };

    match parse_value_and_unit {
        Some((value, unit)) => match unit {
            "ms" | "millisecond" | "milliseconds" | "millis" | "milissegundo" | "milissegundos" => {
                Some(Duration::from_millis(value))
            }
            "s" | "second" | "seconds" | "segundo" | "segundos" => Some(Duration::from_secs(value)),
            "m" | "minute" | "minutes" | "minuto" | "minutos" => {
                Some(Duration::from_secs(value * 60))
            }
            "h" | "hour" | "hours" | "hora" | "horas" => Some(Duration::from_secs(value * 60 * 60)),
            "d" | "day" | "days" | "dia" | "dias" => {
                Some(Duration::from_secs(value * 60 * 60 * 24))
            }
            "w" | "week" | "weeks" | "semana" | "semanas" => {
                Some(Duration::from_secs(value * 60 * 60 * 24 * 7))
            }
            _ => None,
        },
        None => None,
    }
}
