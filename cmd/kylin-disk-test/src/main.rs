use std::path::Path;

fn put_random_file(path: &Path, size: usize) -> std::io::Result<()> {
    let mut rng = rand::thread_rng();
    let mut file = std::fs::File::create(path)?;
    const CHUNK_SIZE: usize = 4096;
    let mut rem = size;
    let mut buf = vec![0u8; CHUNK_SIZE];
    while rem > 0 {
        let buf = &mut buf[..CHUNK_SIZE.min(rem)];
        rng.fill_bytes();
        file.write_all(&buf)?;
        rem -= buf.len();
    }
    file.sync_all()?;
    Ok(())
}

fn read_random(path: &Path) {

}

fn main() {
    let (_, read) = backup_stream::utils::with_record_read_throughput(f)
}
