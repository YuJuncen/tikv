use std::error::Error;

use backup_stream::bin::io_testing;
use clap::ArgMatches;

pub fn run(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let bucket = matches.value_of("bucket").unwrap();
    let prefix = matches.value_of("prefix").unwrap_or("");
    let write_size = matches
        .value_of("write-size")
        .unwrap_or("1048576")
        .parse::<usize>()?;
    let limit_size = matches
        .value_of("limit-size")
        .unwrap_or("104857600")
        .parse::<usize>()?;
    let cache_quota = matches
        .value_of("cache-quota")
        .unwrap_or("10485760")
        .parse::<usize>()?;
    let storage_type = matches.value_of("storage-type").unwrap_or("local");

    // Create tokio runtime for async operations
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(io_testing::upload_files_to_storage(
        bucket,
        prefix,
        write_size,
        limit_size,
        cache_quota,
        storage_type,
    ))?;

    Ok(())
}
