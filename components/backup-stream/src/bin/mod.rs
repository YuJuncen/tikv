pub mod io_testing {
    use std::{
        path::Path,
        sync::{atomic::AtomicUsize, Arc},
        time::Instant,
    };

    use cloud::blob::{BucketConf, StringNonEmpty};
    use external_storage::{ExternalStorage, UnpinReader};
    use kvproto::brpb::CompressionType;
    use rand::{distributions::Alphanumeric, Rng};
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use crate::{
        tempfiles::{Config as TempFileConfig, TempFilePool},
        utils::{CompressionWriter, FilesReader},
    };

    pub async fn upload_files_to_storage(
        bucket: &str,
        prefix: &str,
        write_size: usize,
        limit_size: usize,
        cache_quota: usize,
        storage_type: &str,
        repeat: usize,
        sas_token: Option<&str>,
        file_count: usize,
        round_robin: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting upload test with parameters:");
        println!("  Bucket: {}", bucket);
        println!("  Prefix: {}", prefix);
        println!("  Write size: {} bytes", write_size);
        println!("  Size limit: {} bytes", limit_size);
        println!("  Cache quota: {} bytes", cache_quota);
        println!("  Storage type: {}", storage_type);
        println!("  Repeat count: {}", repeat);
        println!("  File count: {}", file_count);
        println!("  Round-robin mode: {}", round_robin);

        // Create a temporary directory for our tempfile pool
        let temp_dir = TempDir::new()?;
        println!(
            "Created temporary directory at: {}",
            temp_dir.path().display()
        );

        // Initialize the TempFilePool
        let temp_file_config = TempFileConfig {
            cache_size: AtomicUsize::new(cache_quota),
            swap_files: temp_dir.path().to_owned(),
            content_compression: CompressionType::Unknown,
            minimal_swap_out_file_size: 4096,
            write_buffer_size: 2048,
        };

        let pool = Arc::new(TempFilePool::new(temp_file_config)?);
        println!(
            "Initialized TempFilePool with cache quota of {} bytes",
            cache_quota
        );

        // Create storage client based on type
        let storage = match storage_type.to_lowercase().as_str() {
            "local" => {
                let local_dir = temp_dir.path().join("external_storage");
                std::fs::create_dir_all(&local_dir)?;

                let strg = external_storage::local::LocalStorage::new(&local_dir)?;

                println!("Creating local storage at {}", local_dir.display());
                Box::new(strg) as Box<dyn ExternalStorage>
            }
            "azure" => {
                // Create bucket config
                let bucket_name = StringNonEmpty::required(bucket.to_string())?;
                let mut bucket_conf = BucketConf::default(bucket_name);
                if !prefix.is_empty() {
                    bucket_conf.prefix = StringNonEmpty::opt(prefix.to_string());
                }

                // Create input config
                let mut input_config = kvproto::brpb::AzureBlobStorage::default();
                input_config.set_bucket(bucket.to_string());
                input_config.set_prefix(prefix.to_string());
                if let Some(name) = sas_token {
                    input_config.set_access_sig(name.to_string());
                }
                // We're assuming the Azure credentials are in environment variables

                println!(
                    "Creating Azure blob storage with bucket {} and prefix {}",
                    bucket, prefix
                );
                let mut bkend = kvproto::brpb::StorageBackend::new();
                bkend.set_azure_blob_storage(input_config);
                let strg = external_storage_export::create_storage(&bkend, Default::default())?;
                strg
            }
            _ => return Err(format!("Unsupported storage type: {}", storage_type).into()),
        };

        println!("Connected to external storage");

        // Variables to track stats across repeated runs
        let mut total_duration = std::time::Duration::new(0, 0);
        let mut upload_speeds = Vec::with_capacity(repeat);

        // Repeat the test the specified number of times
        for run in 1..=repeat {
            println!("\n--- Starting run {}/{} ---", run, repeat);

            // Generate random data and write to files
            let mut rng = rand::thread_rng();
            let mut total_bytes_written = 0;
            let mut files = Vec::new();

            // Pre-create all files in round-robin mode
            for i in 0..file_count {
                let file_path = format!("file_{}.dat", i);
                // Just open and close to create the files
                let file = pool.open_for_write(Path::new(&file_path))?;
                files.push(file);
            }
            println!("Created {} files for round-robin writing", file_count);

            println!("Starting to write files...");

            let mut current_file_idx = 0;
            while total_bytes_written < limit_size {
                current_file_idx = if round_robin {
                    (current_file_idx + 1) % file_count
                } else {
                    rand::random::<usize>() % file_count
                };
                let file = &mut files[current_file_idx];

                // Write random data of specified size
                let data: String = (&mut rng)
                    .sample_iter(&Alphanumeric)
                    .take(write_size)
                    .map(char::from)
                    .collect();

                file.write_all(data.as_bytes()).await?;

                total_bytes_written += write_size;

                println!(
                    "Written to file {} ({} bytes), total: {} bytes",
                    file.path().display(),
                    write_size,
                    total_bytes_written
                );

                // If we've reached the limit, break
                if total_bytes_written >= limit_size {
                    break;
                }
            }

            println!(
                "Finished writing to {} files, total {} bytes",
                files.len(),
                total_bytes_written
            );

            let mut file_names = vec![];
            for mut file in files {
                file.flush().await?;
                file.done().await?;
                file_names.push(file.path().to_owned());
            }

            // Now concatenate and upload
            println!("Starting upload to external storage...");
            let start_time = Instant::now();

            let mut readers = Vec::new();
            for file_name in &file_names {
                let reader = pool.open_raw_for_read(file_name)?;
                readers.push(reader);
            }

            let files_reader = FilesReader::new(readers);
            let upload_path = format!(
                "backup_test_upload_{}_{:02}",
                chrono::Utc::now().format("%Y%m%d%H%M%S"),
                run
            );

            println!("Uploading concatenated files to {}", upload_path);

            let content_length = total_bytes_written as u64;
            storage
                .write(
                    &upload_path,
                    UnpinReader(Box::new(files_reader.compat())),
                    content_length,
                )
                .await?;

            let duration = start_time.elapsed();
            let upload_speed =
                (total_bytes_written as f64 / 1024.0 / 1024.0) / duration.as_secs_f64();
            upload_speeds.push(upload_speed);
            total_duration += duration;

            println!("Upload completed successfully in {:.2?}", duration);
            println!("Upload speed: {:.2} MB/s", upload_speed);
        }

        // Calculate and display statistics across runs
        if repeat > 1 {
            println!("\n--- Test Summary ---");
            println!("Total runs: {}", repeat);
            println!(
                "Average upload time: {:.2?}",
                total_duration / repeat as u32
            );

            let avg_speed: f64 = upload_speeds.iter().sum::<f64>() / repeat as f64;
            println!("Average upload speed: {:.2} MB/s", avg_speed);

            if repeat > 2 {
                upload_speeds.sort_by(|a, b| a.partial_cmp(b).unwrap());
                println!("Min upload speed: {:.2} MB/s", upload_speeds[0]);
                println!("Max upload speed: {:.2} MB/s", upload_speeds[repeat - 1]);

                // Calculate median
                let median = if repeat % 2 == 0 {
                    (upload_speeds[repeat / 2 - 1] + upload_speeds[repeat / 2]) / 2.0
                } else {
                    upload_speeds[repeat / 2]
                };
                println!("Median upload speed: {:.2} MB/s", median);
            }
        }

        println!("Test completed successfully!");
        Ok(())
    }
}
