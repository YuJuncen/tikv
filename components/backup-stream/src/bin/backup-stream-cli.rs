use backup_stream::{self};
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "Backup Stream CLI", 
           version = env!("CARGO_PKG_VERSION"), 
           author = "TiKV Contributors", 
           about = "Command line tool for TiKV Backup Stream service")]
struct Cli {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
enum Command {
    /// Test writing to temp files and uploading to external storage
    #[structopt(name = "io-test")]
    IoTest {
        /// The bucket name for external storage
        #[structopt(long)]
        bucket: String,

        /// The prefix for external storage
        #[structopt(long, default_value = "")]
        prefix: String,

        /// Size of each write operation in bytes
        #[structopt(long, default_value = "1048576")]
        write_size: usize,

        /// Total data size limit in bytes
        #[structopt(long, default_value = "104857600")]
        limit_size: usize,

        /// Cache quota for the temp file pool in bytes
        #[structopt(long, default_value = "10485760")]
        cache_quota: usize,

        /// Type of external storage to use
        #[structopt(long, default_value = "local", possible_values = &["local", "azure"])]
        storage_type: String,

        /// Number of times to repeat the test for collecting statistics
        #[structopt(long, default_value = "1")]
        repeat: usize,

        /// The STS Token used to authroziation.
        #[structopt(long)]
        sts_token: Option<String>,

        /// Total number of files to create during the test
        #[structopt(long, default_value = "1")]
        file_count: usize,

        /// Enable round-robin writing to the specified number of files
        #[structopt(long)]
        round_robin: bool,
    },
}

fn main() {
    test_util::init_log_for_test();

    let args = Cli::from_args();

    match args.command {
        Command::IoTest {
            bucket,
            prefix,
            write_size,
            limit_size,
            cache_quota,
            storage_type,
            repeat,
            sts_token,
            file_count,
            round_robin,
        } => {
            // Create tokio runtime for async operations
            let rt = tokio::runtime::Runtime::new().unwrap();
            if let Err(e) = rt.block_on(backup_stream::bin::io_testing::upload_files_to_storage(
                &bucket,
                &prefix,
                write_size,
                limit_size,
                cache_quota,
                &storage_type,
                repeat,
                sts_token.as_deref(),
                file_count,
                round_robin,
            )) {
                eprintln!("Error during IO test: {}", e);
                std::process::exit(1);
            }
        }
    }
}
