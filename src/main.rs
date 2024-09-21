mod client;
mod server;
mod utils;

use clap::{Parser, Subcommand};
use pathkvs_core::DatabaseWriteSyncMode;

#[derive(Parser)]
#[command(name = "pathkvs", about = "Um banco chave valor")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Serve o banco
    Serve {
        /// Caminho do banco de dados (opcional)
        path: Option<String>,
        /// Commits retornam quando os dados estiverem no disco
        #[arg(short, long)]
        sync: bool,
        /// Commits retornam quando os sistema operacional obter a escrita
        #[arg(short, long)]
        flush: bool,
        /// Commits retornam quando os conflitos forem resolvido
        #[arg(short, long)]
        cache: bool,
    },
}

fn main() -> std::io::Result<()> {
    let _ = ctrlc::set_handler(|| std::process::exit(0));
    match Cli::parse().command {
        Some(Commands::Serve {
            path,
            sync,
            flush,
            cache: cached,
        }) => {
            let mode = if sync {
                DatabaseWriteSyncMode::Sync
            } else if flush {
                DatabaseWriteSyncMode::Flush
            } else if cached {
                DatabaseWriteSyncMode::Cached
            } else {
                DatabaseWriteSyncMode::Sync
            };
            server::serve(path, mode)?;
        }
        None => {
            client::client()?;
        }
    }
    Ok(())
}
