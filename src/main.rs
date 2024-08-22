mod client;
mod fmt;
mod server;

fn main() -> Result<(), std::io::Error> {
    let arg = std::env::args().nth(1).unwrap_or_default();
    if arg == "stress" {
        client::stress()?;
    } else if arg == "serve" {
        server::serve()?;
    } else {
        client::client()?;
    }
    Ok(())
}

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