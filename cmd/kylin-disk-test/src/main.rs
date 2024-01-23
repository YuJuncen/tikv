use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

const CHUNK_SIZE: usize = 4096;

fn put_random_file(path: &Path, size: usize) -> std::io::Result<()> {
    let mut rng = rand::thread_rng();
    let mut file = std::fs::File::create(path)?;
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

#[repr(C, align(4096))]
#[alignment(CHUNK_SIZE)]
struct DirectBuffer([u8; CHUNK_SIZE * 16]);

fn read_chunk(fd: &mut File, offset: usize, length: usize) -> std::io::Result<()> {
    let mut buf = DirectBuffer([0u8; CHUNK_SIZE * 16]);
    fd.seek(SeekFrom::Start(offset))?;
    let mut rem = length;
    while rem > 0 {
        let buf = &mut buf[..(rem.min(buf.0.len()))];
        let n = fd.read(buf)?;
        rem -= n;
    }
    Ok(())
}

fn align_to_4096(num: usize) -> usize {
    n - (n % 4096)
}

fn main() -> std::io::Result<()> {
    let tmp = tempfile::tempdir()?;
    let file_path = tmp.path().join("random.bin");
    put_random_file(&file_path, 1 * 1024 * 1024 * 1024)?;
    // Open the file with direct I/O.
    let mut fd = OpenOptions::new()
        .custom_flags(0x4000 /* O_DIRECT */ | libc::O_RDONLY)
        .open(&file_path)?;
    for _ in 0..100 {
        let offset = align_to_4096(rng.gen_range(0..(1 * 1024 * 1024 * 1024)));
        let length = align_to_4096(rng.gen_range(0..(CHUNK_SIZE * 32)));
        let (res, size) = backup_stream::utils::with_record_read_throughput(|| {
            read_chunk(&mut fd, offset, length)
        });
        res?;
        if length != size {
            println!("Diff {} vs {}", length, size);
        } else {
            print!(".");
        }
    }
    Ok(())
}
