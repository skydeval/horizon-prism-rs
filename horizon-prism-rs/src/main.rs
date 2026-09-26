//! The `horizon-prism-rs` binary (design-v2 §3.3).

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "horizon-prism-rs", version, about = "ATProto AppView with kryphocron private-tier reads")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// HTTP, metrics and every background worker (default).
    Serve,
    /// `GET http://127.0.0.1:3030/xrpc/_health`; exit 0 on 200 with `"ok":true`.
    Healthcheck,
    /// Mutating admin commands, executed by `serve` (§3.3).
    Admin(horizon_prism_rs::cli::dispatch::AdminArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => match runtime.block_on(horizon_prism_rs::serve::run()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{e}");
                tracing::error!(event = "serve_failed", error = %e, exit_code = e.exit_code, "serve exited");
                ExitCode::from(e.exit_code)
            }
        },
        Command::Admin(args) => ExitCode::from(runtime.block_on(horizon_prism_rs::cli::dispatch::run(&args))),
        Command::Healthcheck => {
            if runtime.block_on(horizon_prism_rs::serve::healthcheck()) {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
    }
}
