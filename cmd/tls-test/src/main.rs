#![feature(const_option_ext)]

use std::{sync::Arc, time::Duration};

use backup_stream::{
    errors::{Error, Result},
    metadata::{
        keys::MetaKey,
        store::{Keys, MetaStore, Snapshot},
        ConnectionConfig, LazyEtcdClient,
    },
};
use clap::Parser;
use grpcio::ChannelBuilder;
use security::{SecurityConfig, SecurityManager};
use server::setup::initial_logger;
use slog_global::info;
use tikv::config::TikvConfig;
use tokio::join;

#[derive(Clone, Debug, PartialEq, Default, Parser)]
struct Config {
    #[arg(
        short,
        long,
        default_value = "https://127.0.0.1:2379",
        help = "the PD addresses, this option will override the config from TiKV config."
    )]
    endpoints: Vec<String>,
    #[arg(
        long,
        default_value = "",
        help = "when set, would read the TLS suite (cannot be overriden then) and endpoints config (can be overriden by set `--endpoints` or `-e`) from the TiKV config."
    )]
    tikv_config_path: String,
    #[arg(long, default_value = "", help = "the path to client trusted CA cert.")]
    ca_path: String,
    #[arg(long, default_value = "", help = "the client cert path.")]
    cert_path: String,
    #[arg(long, default_value = "", help = "the client key path.")]
    key_path: String,
    #[arg(
        short,
        long,
        default_value_t = false,
        help = "Do nothing and show the build info."
    )]
    version: bool,
}

#[derive(Clone)]
struct Run {
    security_manager: Arc<SecurityManager>,
    endpoints: Vec<String>,
}

type MayFail<T> = std::result::Result<T, Box<dyn std::error::Error>>;

impl Config {
    fn convert_direct(self) -> MayFail<Run> {
        let c = self;
        let sec = SecurityConfig {
            ca_path: c.ca_path,
            cert_path: c.cert_path,
            key_path: c.key_path,
            ..SecurityConfig::default()
        };
        let endpoints = c.endpoints;
        let security_manager = Arc::new(SecurityManager::new(&sec)?);
        Ok(Run {
            endpoints,
            security_manager,
        })
    }
}

fn init_log() {
    initial_logger(&Default::default());
}

async fn execute_test<M: MetaStore>(cli: &M) -> Result<()> {
    let snap = cli.snapshot().await?;
    let query = Keys::Prefix(MetaKey(b"/tidb/br-stream".to_vec()));
    let keys = snap.get(query).await?;
    info!("Query successed!"; "pitr-key-len" => %keys.len());
    Ok(())
}

async fn test_tikv_connection(run: &Run) -> Result<()> {
    let env = grpcio::EnvBuilder::new().cq_count(1).build();
    let conn = run
        .security_manager
        .connect(ChannelBuilder::new(Arc::new(env)), &run.endpoints[0]);
    if !conn.wait_for_connected(Duration::from_secs(10)).await {
        return Err(Error::Other("TiKV connection timeout".into()));
    }
    Ok(())
}

async fn test_pitr_connection(run: &Run) -> Result<()> {
    let ccfg = ConnectionConfig {
        keep_alive_interval: Duration::from_secs(3),
        keep_alive_timeout: Duration::from_secs(10),
        tls: Arc::clone(&run.security_manager),
    };
    let etcd_cli = LazyEtcdClient::new(run.endpoints.as_slice(), ccfg);

    execute_test(&etcd_cli).await?;
    Ok(())
}

fn init_run(cfg: &Config) -> MayFail<Run> {
    if [&cfg.ca_path, &cfg.cert_path, &cfg.key_path]
        .iter()
        .all(|x| !x.is_empty())
    {
        return Ok(cfg.clone().convert_direct()?);
    }
    if !cfg.tikv_config_path.is_empty() {
        let content = std::fs::read(&cfg.tikv_config_path)?;
        let kvcfg: TikvConfig = toml::from_str(&String::from_utf8(content)?)?;
        info!("Loaded config from TiKV config."; "cfg" => ?kvcfg.security, "pds" => ?kvcfg.pd.endpoints);
        let security_manager = Arc::new(SecurityManager::new(&kvcfg.security)?);
        // If manually configuried the endpoints, don't override it by the TiKV config.
        let endpoints = if cfg.endpoints != ["https://127.0.0.1:2379"] {
            cfg.endpoints.clone()
        } else {
            kvcfg.pd.endpoints
        };
        return Ok(Run {
            endpoints,
            security_manager,
        });
    }
    Err("must provide [--ca-path, --cert-path, --key-path] or --tikv-config-path".into())
}

const BUILD_HASH: &'static str = option_env!("TIKV_BUILD_GIT_HASH").unwrap_or("<UNKNOWN>");
const BUILD_BRANCH: &'static str = option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or("<UNKNOWN>");

fn run() -> Result<()> {
    let cfg = Config::parse();
    if cfg.version {
        println!("BUILD_HASH: {BUILD_HASH}");
        println!("BUILD_BRANCH: {BUILD_BRANCH}");
        return Ok(());
    }

    info!("Welcome to TLS compatibility test utility!";
        "build_hash" => %BUILD_HASH,
        "build_branch" => %BUILD_BRANCH
    );
    info!("Using config."; "cfg" => ?cfg);
    let run = init_run(&cfg).map_err(|err| Error::Other(format!("{}", err).into()))?;
    let run2 = run.clone();
    let show = |r: Result<()>, name: &str| {
        match &r {
            Ok(_) => info!("success on {}", name),
            Err(err) => info!("failed on {}: err = {}", name, err),
        }
        r
    };
    let tikv = async move { show(test_tikv_connection(&run).await, "grpc") };
    let pitr = async move { show(test_pitr_connection(&run2).await, "tonic") };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let s1 = tokio::spawn(pitr);
            let s2 = tokio::spawn(tikv);
            let r1 = s1.await.unwrap();
            let r2 = s2.await.unwrap();
            r1.and(r2)
        })?;
    Ok(())
}

fn main() {
    init_log();
    let result = run();
    slog_global::clear_global();
    // For waiting logs to be fully print...
    // We cannot be waken up when logger dropped...
    std::thread::sleep(Duration::from_secs(1));
    match result {
        Ok(_) => println!("SUCCESS"),
        Err(err) => println!("FAILURE: {err}"),
    }
}
