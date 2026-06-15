use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context;
use axum::Router;
use clap::{Parser, Subcommand};
use kasdex_indexer::{IndexerRuntimeConfig, IndexerStatusHandle};
use tokio::{net::TcpListener, signal, time};
use tower_http::services::{ServeDir, ServeFile};
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
        #[arg(long, default_value = "apps/web/dist")]
        web_dir: PathBuf,
        #[arg(long)]
        index_follow: bool,
        #[arg(long, default_value = "http://127.0.0.1:16110")]
        index_rpc_url: String,
        #[arg(long, default_value_t = 100)]
        index_limit_blocks: usize,
        #[arg(long, default_value_t = 5)]
        index_interval_secs: u64,
        #[arg(long, default_value_t = 1_000)]
        index_tail_lag_threshold: u64,
        #[arg(long, default_value_t = 120)]
        index_stalled_after_secs: u64,
        #[arg(long, default_value_t = 120)]
        index_stale_after_secs: u64,
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
        Command::Serve {
            listen,
            data_dir,
            web_dir,
            index_follow,
            index_rpc_url,
            index_limit_blocks,
            index_interval_secs,
            index_tail_lag_threshold,
            index_stalled_after_secs,
            index_stale_after_secs,
        } => {
            serve(ServeConfig {
                listen,
                data_dir,
                web_dir,
                index_follow,
                index_rpc_url,
                index_limit_blocks,
                index_interval_secs,
                index_tail_lag_threshold,
                index_stalled_after_secs,
                index_stale_after_secs,
            })
            .await
        }
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

#[derive(Debug)]
struct ServeConfig {
    listen: SocketAddr,
    data_dir: PathBuf,
    web_dir: PathBuf,
    index_follow: bool,
    index_rpc_url: String,
    index_limit_blocks: usize,
    index_interval_secs: u64,
    index_tail_lag_threshold: u64,
    index_stalled_after_secs: u64,
    index_stale_after_secs: u64,
}

async fn serve(config: ServeConfig) -> anyhow::Result<()> {
    let store = Arc::new(
        kasdex_store_rocks::RocksStore::open(&config.data_dir).with_context(|| {
            format!(
                "failed to open index store at {}",
                config.data_dir.display()
            )
        })?,
    );
    let indexer_status = IndexerStatusHandle::new(IndexerRuntimeConfig {
        tail_lag_threshold: config.index_tail_lag_threshold,
        stalled_after: time::Duration::from_secs(config.index_stalled_after_secs),
        stale_after: time::Duration::from_secs(config.index_stale_after_secs),
    });
    if config.index_follow {
        spawn_indexer(
            Arc::clone(&store),
            indexer_status.clone(),
            config.index_rpc_url,
            config.index_limit_blocks,
            config.index_interval_secs,
        );
    }

    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind {}", config.listen))?;
    info!(
        listen = %config.listen,
        data_dir = %config.data_dir.display(),
        index_follow = config.index_follow,
        "serving kasdex api"
    );

    let app = attach_web(
        kasdex_api::router_with_store_and_indexer_status(store, indexer_status),
        &config.web_dir,
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("api server failed")
}

fn attach_web(app: Router, web_dir: &PathBuf) -> Router {
    let index = web_dir.join("index.html");
    if !index.is_file() {
        tracing::warn!(
            web_dir = %web_dir.display(),
            "web build directory missing; serving API only"
        );
        return app;
    }

    info!(web_dir = %web_dir.display(), "serving dashboard web app");
    app.fallback_service(ServeDir::new(web_dir).not_found_service(ServeFile::new(index)))
}

fn spawn_indexer(
    store: Arc<kasdex_store_rocks::RocksStore>,
    status: IndexerStatusHandle,
    rpc_url: String,
    limit_blocks: usize,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        loop {
            let poll_store = Arc::clone(&store);
            let poll_status = status.clone();
            let poll_rpc_url = rpc_url.clone();
            let started_at = std::time::SystemTime::now();
            poll_status.mark_poll_started(started_at);

            let poll = tokio::spawn(async move {
                kasdex_indexer::run_bounded_backfill(
                    &poll_store,
                    kasdex_indexer::BackfillConfig {
                        rpc_url: poll_rpc_url,
                        limit_blocks,
                        start_hash: None,
                    },
                )
                .await
            });

            match poll.await {
                Ok(Ok(report)) => {
                    let finished_at = std::time::SystemTime::now();
                    status.mark_poll_success(&report, started_at, finished_at);
                    info!(
                        network = %report.network,
                        start_hash = %report.start_hash,
                        indexed_blocks = report.indexed_blocks,
                        indexed_transactions = report.indexed_transactions,
                        virtual_daa_score = report.virtual_daa_score,
                        checkpoint_daa_score = ?report.checkpoint_daa_score,
                        "indexer poll completed"
                    );
                }
                Ok(Err(err)) => {
                    status.mark_poll_error(&err, Some(started_at), std::time::SystemTime::now());
                    tracing::warn!(error = %err, "indexer poll failed");
                }
                Err(err) => {
                    status.mark_poll_error(
                        format!("indexer task failed: {err}"),
                        Some(started_at),
                        std::time::SystemTime::now(),
                    );
                    tracing::warn!(error = %err, "indexer task failed");
                }
            }

            time::sleep(time::Duration::from_secs(interval_secs)).await;
        }
    });
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
                "virtual_daa_score": report.virtual_daa_score,
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
