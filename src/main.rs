mod client;
mod fmt;
mod server;

fn main() -> Result<(), std::io::Error> {
    let arg = std::env::args().nth(1).unwrap_or_default();
    if arg == "stress" {
        let count = std::env::args().nth(2).and_then(|x| x.parse().ok()).unwrap_or(500);
        client::stress(count)?;
    } else if arg == "serve" {
        server::serve()?;
    } else if arg == "local" {
        local()?;
    } else {
        client::client()?;
    }
    Ok(())
}

fn local() -> Result<(), std::io::Error> {
    let db = pathkvs_core::Database::open("data.pathkvs")?;
    for _ in 0..500 {
        let mut tr = db.start_writes();
        let inc = tr.read(b"INC");
        let inc = if inc.is_empty() {
            0
        } else {
            std::str::from_utf8(inc).unwrap().parse().unwrap()
        };
        let inc = inc + 1;
        tr.write(b"INC", format!("{inc}").as_bytes());
        tr.commit()?;
    }
    Ok(())
}
