use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio::{net::TcpListener, signal, time};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "kasdexd", about = "Kasdex local indexer and dashboard daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "127.0.0.1:18180")]
        listen: SocketAddr,
        #[arg(long, default_value = ".kasdex/index")]
        data_dir: PathBuf,
    },
    Openapi {
        #[arg(long)]
        output: PathBuf,
    },
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    Index {
        #[arg(long, default_value = "http://127.0.0.1:16110")]
        rpc_url: String,
        #[arg(long, default_value = ".kasdex/index")]
        data_dir: PathBuf,
        #[arg(long, default_value_t = 100)]
        limit_blocks: usize,
        #[arg(long)]
        start_hash: Option<String>,
        #[arg(long)]
        follow: bool,
        #[arg(long, default_value_t = 5)]
        interval_secs: u64,
    },
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    Probe {
        #[arg(long, default_value = "http://127.0.0.1:16110")]
        rpc_url: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match Cli::parse().command {
        Command::Serve { listen, data_dir } => serve(listen, data_dir).await,
        Command::Openapi { output } => write_openapi(output),
        Command::Node { command } => match command {
            NodeCommand::Probe { rpc_url } => probe_node(rpc_url).await,
        },
        Command::Index {
            rpc_url,
            data_dir,
            limit_blocks,
            start_hash,
            follow,
            interval_secs,
        } => {
            index(
                rpc_url,
                data_dir,
                limit_blocks,
                start_hash,
                follow,
                interval_secs,
            )
            .await
        }
    }
}

async fn serve(listen: SocketAddr, data_dir: PathBuf) -> anyhow::Result<()> {
    let store = kasdex_store_rocks::RocksStore::open(&data_dir)
        .with_context(|| format!("failed to open index store at {}", data_dir.display()))?;
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind {listen}"))?;
    info!(%listen, data_dir = %data_dir.display(), "serving kasdex api");

    axum::serve(listener, kasdex_api::router_with_store(store))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("api server failed")
}

fn write_openapi(output: PathBuf) -> anyhow::Result<()> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let json = kasdex_api::openapi_json_pretty().context("failed to serialize OpenAPI spec")?;
    std::fs::write(&output, json).with_context(|| format!("failed to write {}", output.display()))
}

async fn probe_node(rpc_url: String) -> anyhow::Result<()> {
    let status = kasdex_node::GrpcKaspaNode::connect(rpc_url)
        .await?
        .probe()
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "endpoint": status.endpoint,
            "network": status.network,
            "server_version": status.server_version,
            "is_synced": status.is_synced,
            "is_archival": status.is_archival,
            "has_utxo_index": status.has_utxo_index,
            "virtual_daa_score": status.virtual_daa_score,
            "pruning_point_hash": status.pruning_point_hash,
            "sink": status.sink,
            "virtual_chain_sample_start": status.virtual_chain_sample_start,
            "virtual_chain_added": status.virtual_chain_added,
            "accepted_transaction_batches": status.accepted_transaction_batches,
            "sink_block_transaction_count": status.sink_block_transaction_count,
        }))?
    );
    Ok(())
}

async fn index(
    rpc_url: String,
    data_dir: PathBuf,
    limit_blocks: usize,
    start_hash: Option<String>,
    follow: bool,
    interval_secs: u64,
) -> anyhow::Result<()> {
    let store = kasdex_store_rocks::RocksStore::open(&data_dir)?;
    let mut configured_start_hash = start_hash;

    loop {
        let report = kasdex_indexer::run_bounded_backfill(
            &store,
            kasdex_indexer::BackfillConfig {
                rpc_url: rpc_url.clone(),
                limit_blocks,
                start_hash: configured_start_hash.take(),
            },
        )
        .await?;

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "data_dir": data_dir,
                "network": report.network,
                "start_hash": report.start_hash,
                "fetched_chain_blocks": report.fetched_chain_blocks,
                "indexed_blocks": report.indexed_blocks,
                "indexed_transactions": report.indexed_transactions,
                "checkpoint_daa_score": report.checkpoint_daa_score,
                "checkpoint_hash": report.checkpoint_hash,
            }))?
        );

        if !follow {
            break;
        }

        time::sleep(time::Duration::from_secs(interval_secs)).await;
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
