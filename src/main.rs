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
