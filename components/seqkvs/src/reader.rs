use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::{
    io::{Read, Result},
    ops::Bound,
};

use crate::MAGIC;

pub struct PlainFileReader<R> {
    inner: R,
    common_prefix_length: usize,
    key_buf: BytesMut,
    value_buf: BytesMut,
}

impl<R: Read> PlainFileReader<R> {
    /// Create a new plain file reader over a reader.
    /// The compression might must be handled at outside.
    /// (Maybe we need add the compress type )
    pub fn new(r: R) -> Result<Self> {
        let mut reader = Self {
            inner: r,
            common_prefix_length: 0,
            key_buf: BytesMut::with_capacity(1024 * 8),
            value_buf: BytesMut::with_capacity(1024 * 8),
        };
        reader.read_header()?;
        Ok(reader)
    }

    fn read_header(&mut self) -> Result<()> {
        let mut magic = [0u8; 4];
        self.inner.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad magic number detached",
            ));
        }
        let len = self.inner.read_u32::<BigEndian>()? as usize;
        self.common_prefix_length = len;
        let mut common_prefix = vec![0u8; len];
        self.inner.read_exact(&mut common_prefix)?;
        self.key_buf.clear();
        self.key_buf.put(&common_prefix[..]);
        self.value_buf.clear();
        self.value_buf.put(&common_prefix[..]);
        Ok(())
    }

    fn next_kv_length(&mut self) -> Result<(usize, usize)> {
        let k_size = self.inner.read_u32::<BigEndian>()?;
        let v_size = self.inner.read_u32::<BigEndian>()?;

        Ok((k_size as _, v_size as _))
    }

    fn prepare_buffer(&mut self, key: usize, value: usize) {
        self.key_buf.resize(key + self.common_prefix_length, 0);
        self.value_buf.resize(value, 0);
    }

    /// Read the next KV pair via the managed internal buffer.
    /// The buffer should be managed for better common prefix prepending.
    pub fn next_kv_slice(&mut self) -> Result<(&[u8], &[u8])> {
        use std::io::{Error, ErrorKind};
        let (k, v) = self.next_kv_length()?;
        self.prepare_buffer(k, v);
        // TODO handle error when the file was broken.
        self.inner
            .read_exact(&mut self.key_buf[self.common_prefix_length..])
            .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
        self.inner
            .read_exact(&mut self.value_buf[..])
            .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
        Ok((&self.key_buf[..], &self.value_buf[..]))
    }

    /// Use the callback to iterate all the remain keys.
    pub fn for_remain_keys(&mut self, mut f: impl FnMut(&[u8], &[u8])) -> Result<()> {
        use std::io::ErrorKind;
        loop {
            match self.next_kv_slice() {
                Ok((k, v)) => {
                    f(k, v);
                }
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(()),
                Err(err) => return Err(err),
            }
        }
    }

    pub fn for_kv_in_bound(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        mut f: impl FnMut(&[u8], &[u8]),
    ) -> Result<()> {
        use std::io::ErrorKind;
        use std::ops::RangeBounds;
        loop {
            match self.next_kv_slice() {
                Ok((k, _)) if !RangeBounds::<&[u8]>::contains(&(start, end), &k) => continue,
                Ok((k, v)) => f(k, v),
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(()),
                Err(err) => return Err(err),
            }
        }
    }

    pub fn for_kv_in_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        mut f: impl FnMut(&[u8], &[u8]),
    ) -> Result<()> {
        use std::io::ErrorKind;
        loop {
            match self.next_kv_slice() {
                Ok((k, _)) if k < start => continue,
                Ok((k, _)) if k >= end => return Ok(()),
                Ok((k, v)) => f(k, v),
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(()),
                Err(err) => return Err(err),
            }
        }
    }
}

#[cfg(test)]
mod reader_test {

    use crate::{reader::PlainFileReader, writer::InMemoryPlainFileWriter};

    fn run_test_over(
        mut w: InMemoryPlainFileWriter,
        kvs: &[(&[u8], &[u8])],
        prefix: &[u8],
    ) -> std::io::Result<()> {
        let mut file = vec![];
        for (k, v) in kvs {
            w.add(k, v)?;
        }
        let mut w = w.into_reader();
        std::io::copy(&mut w, &mut file)?;
        let mut c = std::io::Cursor::new(&mut file);
        let mut r = PlainFileReader::new(&mut c)?;
        let mut idx = 0;
        r.for_remain_keys(|k, v| {
            let (key, value) = kvs[idx];
            idx += 1;
            assert_eq!(&k[..prefix.len()], prefix);
            assert_eq!(&k[prefix.len()..], key);
            assert_eq!(v, value);
        })?;
        Ok(())
    }

    #[test]
    fn test_basic() -> std::io::Result<()> {
        let w = InMemoryPlainFileWriter::new()?;
        let cases = [
            (&b"city"[..], &b"coffee"[..]),
            (&b"micrale"[..], &b"here"[..]),
            (
                b"large key!",
                b"larrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrge value!",
            ),
        ];

        run_test_over(w, &cases, b"")
    }

    #[test]
    fn test_common_prefix() -> std::io::Result<()> {
        let w = InMemoryPlainFileWriter::with_capacity_and_common_prefix(1024, b"t1_r")?;
        let kvs = [
            (&b"city"[..], &b"coffee"[..]),
            (&b"micrale"[..], &b"here"[..]),
            (b"large key!", &[0u8; 1024 * 9]),
        ];

        run_test_over(w, &kvs, b"t1_r")
    }

    #[test]
    fn test_scan_ranged() -> std::io::Result<()> {
        use std::ops::Bound;
        let mut w = InMemoryPlainFileWriter::new()?;
        let mut file = vec![];
        w.add(b"1", b"one")?;
        w.add(b"2", b"two")?;
        w.add(b"4", b"four")?;
        w.add(b"5", b"five")?;
        let mut w = w.into_reader();

        std::io::copy(&mut w, &mut file)?;
        let mut c = std::io::Cursor::new(&mut file);
        let mut r = PlainFileReader::new(&mut c)?;
        let mut collected = Vec::with_capacity(2);
        r.for_kv_in_range(b"2", b"5", |k, v| {
            collected.push((k.to_owned(), v.to_owned()));
        })?;
        // TODO finish the test.
        println!("{:?}", collected);
        let mut c = std::io::Cursor::new(&mut file);
        let mut r = PlainFileReader::new(&mut c)?;
        r.for_kv_in_bound(Bound::Included(b"2"), Bound::Included(b"4"), |k, v| {
            println!("{:?} => {:?}", k, v);
        })?;
        Ok(())
    }
}
