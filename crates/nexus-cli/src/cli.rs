//! CLI argument definitions for Nexus management commands.

use clap::{Parser, Subcommand};

/// Top-level CLI definition.
#[derive(Parser, Debug)]
#[command(name = "nexus-cli", about = "Nexus management CLI")]
pub struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Supported management commands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Import/update mailing list metadata from public-inbox manifest.
    ImportMailingLists,
    /// Drop all data and re-run migrations (requires confirmation).
    ResetDb,
}
