use chrono::{DateTime, Local};
use pathkvs_core::error::{TransactionConflict, TransactionError, TransposeConflict};
use pathkvs_net::client::ConnectionMode;
use std::{io::BufRead, time::Duration};

const CLEAR: &str = "\x1B[H\x1B[2J\x1B[3J";
const RETURN: &str = "\x1B[1A\x1B[2K\x1B[G";

use crate::utils::{parse_general_timestamp, DisplayBytesEx};

pub fn client() -> Result<(), std::io::Error> {
    let addr = "127.0.0.1:6314";
    let conn = std::net::TcpStream::connect(addr)?;
    conn.set_read_timeout(Some(Duration::from_secs(1)))?;
    conn.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut conn = pathkvs_net::client::Connection::new(conn);
    let stdin = std::io::stdin();
    let handle = stdin.lock();
    let mut lines = handle.lines();
    let mut read_count = 0;
    let mut write_count = 0;
    println!("{CLEAR}PATHKVS: cliente interativo, conectado a {addr}");
    println!("use o comando \"=h\" para ver a ajuda");
    println!("aperte Ctrl+C para sair");
    println!();
    while let Some(line) = lines.next() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        match line.split_once('=') {
            Some(("", value)) => match value {
                "s" | "start" => {
                    let mode = conn.mode();
                    conn.start_transaction()?;
                    match mode {
                        ConnectionMode::Normal => println!("{RETURN}começado a transação"),
                        ConnectionMode::Transaction => {
                            println!(
                                "{RETURN}começado a transação, descartado a transação anterior"
                            )
                        }
                        ConnectionMode::Snapshot => {
                            println!("{RETURN}começado a transação, finalizado a snapshot anterior")
                        }
                    }
                }
                line if line.starts_with("snap") => {
                    let timestamp = line[4..].trim();
                    if timestamp.is_empty() {
                        let mode = conn.mode();
                        conn.start_snapshot(None)?;
                        match mode {
                            ConnectionMode::Normal => println!("{RETURN}obtido o snapshot atual"),
                            ConnectionMode::Transaction => println!(
                                "{RETURN}obtido o snapshot atual, descartado a transação anterior"
                            ),
                            ConnectionMode::Snapshot => {
                                println!(
                                    "{RETURN}obtido o snapshot atual, finalizado a snapshot anterior"
                                )
                            }
                        }
                    } else if let Some(time) = parse_general_timestamp(timestamp) {
                        let display = DateTime::<Local>::from(time).format("%Y-%m-%d %H:%M:%S");
                        let mode = conn.mode();
                        conn.start_snapshot(Some(time))?;
                        match mode {
                            ConnectionMode::Normal => println!("{RETURN}obtido o snapshot de {display}"),
                            ConnectionMode::Transaction => println!("{RETURN}obtido o snapshot de {display}, descartado a transação anterior"),
                            ConnectionMode::Snapshot => println!("{RETURN}obtido o snapshot de {display}, finalizado a snapshot anterior"),
                        }
                    } else {
                        println!("tempo inválido, formatos suportados:");
                        println!("YYYY-MM-DD HH:MM:SS.mmm");
                        println!("YYYY-MM-DD HH:MM:SS");
                        println!("YYYY-MM-DD");
                        println!("-Xms");
                        println!("-Xs");
                        println!("-Xm");
                        println!("-Xh");
                        println!("-Xd");
                        println!("-Xw");
                        println!();
                    }
                }
                "c" | "commit" => match conn.mode() {
                    ConnectionMode::Normal => {
                        println!("{RETURN}commit: não estamos em uma transação");
                    }
                    ConnectionMode::Transaction => match conn.commit() {
                        Ok(Some(commit_time)) => {
                            let commit_time =
                                DateTime::<Local>::from(commit_time).format("%Y-%m-%d %H:%M:%S");
                            println!(
                            "{RETURN}commit: salvo {read_count} leitura(s) e {write_count} escritas(s) em {commit_time}"
                        );
                            read_count = 0;
                            write_count = 0;
                        }
                        Ok(None) => {
                            println!(
                                "{RETURN}commit: salvo {read_count} leitura(s) e {write_count} escritas(s)"
                            );
                            read_count = 0;
                            write_count = 0;
                        }
                        Err(TransactionError::Conflict) => {
                            println!("{RETURN}commit: houve um conflito, nada foi salvo");
                            read_count = 0;
                            write_count = 0;
                        }
                        Err(TransactionError::Io(error)) => {
                            return Err(error);
                        }
                    },
                    ConnectionMode::Snapshot => {
                        println!("{RETURN}commit: a snapshot foi finalizada, nada foi salvo");
                    }
                },
                "r" | "rollback" => {
                    match conn.mode() {
                        ConnectionMode::Normal => {
                            println!("{RETURN}rollback: nada foi descartado, não estamos em uma transação");
                        }
                        ConnectionMode::Transaction => {
                            conn.rollback()?;
                            println!("{RETURN}rollback: descartado {read_count} leitura(s) and {write_count} escrita(s)");
                            read_count = 0;
                            write_count = 0;
                        }
                        ConnectionMode::Snapshot => {
                            conn.rollback()?;
                            println!(
                                "{RETURN}rollback: a snapshot foi finalizada, nada foi descartado"
                            );
                        }
                    }
                }
                line if line.starts_with("stress") => {
                    let count = line[6..].trim();
                    let count = count.parse().unwrap_or(500);
                    let mut remaining = count;
                    let start = std::time::Instant::now();
                    let mut last_inc = None;
                    let result = (|| {
                        while remaining > 0 {
                            conn.start_transaction()?;
                            let inc = conn.read_u64_opt("INC")?.unwrap_or(0);
                            last_inc = Some(inc);
                            conn.write_u64("INC", inc + 1)?;
                            match conn.commit().transpose_conflict()? {
                                Ok(_) => {
                                    last_inc = Some(inc + 1);
                                    remaining -= 1;
                                }
                                Err(TransactionConflict) => {
                                    println!("conflito ao escrever {inc}");
                                }
                            }
                        }
                        Ok::<(), std::io::Error>(())
                    })();
                    match result {
                        Ok(()) => {
                            println!("incrementado INC {count} vezes em {:#?}", start.elapsed());
                        }
                        Err(error) => {
                            println!("ocorreu um erro ao incrementar INC");
                            if let Some(last_inc) = last_inc {
                                println!("o último valor conhecido do INC foi {last_inc}");
                            } else {
                                println!("o erro ocorreu antes da primeira leitura do INC");
                            }
                            return Err(error);
                        }
                    }
                }
                "q" | "quit" | "e" | "exit" | "bye" => {
                    break;
                }
                "h" | "help" => {
                    println!("Comandos: (começam com =)");
                    println!("  =h =help     - mostrar essa ajuda");
                    println!("  =s =start    - começar uma transação");
                    println!("  =snap        - tira um foto para leitura");
                    println!("  =snap YYYY-MM-DD HH:MM:DD - obter uma foto do passado");
                    println!("  =c =commit   - salvar a transação ou finalizar a snapshot");
                    println!("  =r =rollback - descartar a transação ou finalizar a snapshot");
                    println!("  =stress N    - incrementar INC N vezes");
                    println!("  =q =e =quit =exit =bye - sair do programa");
                    println!("Comando de escrita:");
                    println!("  mudar o valor da variável INC: \"INC=0\"");
                    println!("Comandos de leitura:");
                    println!("  ver o valor da variável INC: \"INC\"");
                    println!("  mostrar todas as chaves do banco: \"*\"");
                    println!("  mostrar todas as chaves e valores do banco: \"*=\"");
                    println!("  mostrar todas as chaves que começam com A: \"A*\"");
                    println!();
                }
                command => {
                    println!(
                        "{RETURN}={}: não é um comando, digite \"=h\" para ver a ajuda",
                        command
                    );
                }
            },
            Some((key, value)) => match key.split_once('*') {
                Some((start, end)) if value.is_empty() => {
                    let scan = conn.scan(start.as_bytes(), end.as_bytes())?;
                    read_count += scan.len();
                    match scan.as_slice() {
                        [] => {
                            println!("{RETURN}{}: nada foi encontrado", key);
                        }
                        [(k, v)] => {
                            println!("{RETURN}{}: um foi encontrado", key);
                            println!("{}={}", k.display(), v.display());
                        }
                        scan => {
                            println!("{RETURN}{}: {} itens encontrados", key, scan.len());
                            for (k, v) in scan {
                                println!("{}={}", k.display(), v.display());
                            }
                        }
                    }
                }
                Some(_) => {
                    println!("{RETURN}erro: não é possível mudar vários valores de uma vez");
                }
                None if conn.mode().is_snapshot() => {
                    println!("{RETURN}erro: não é possivel escrever em uma snapshot");
                }
                None => {
                    write_count += 1;
                    conn.write(key.as_bytes(), value.as_bytes())?;
                }
            },
            None => match line.split_once('*') {
                Some((start, end)) => {
                    let list = conn.list(start.as_bytes(), end.as_bytes())?;
                    read_count += list.len();
                    match list.as_slice() {
                        [] => {
                            println!("{RETURN}{}: nada foi encontrado", line);
                        }
                        [key] => {
                            println!("{RETURN}{}: um foi encontrado", line);
                            println!("{}", key.display());
                        }
                        list => {
                            println!("{RETURN}{}: {} itens encontrados", line, list.len());
                            for key in list {
                                println!("{}", key.display());
                            }
                        }
                    }
                }
                None => {
                    read_count += 1;
                    println!("{RETURN}{}={}", line, conn.read(line.as_bytes())?.display());
                }
            },
        }
    }
    Ok(())
}
