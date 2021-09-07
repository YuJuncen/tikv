use std::io::{Cursor, Read, Result, Write};

use byteorder::{BigEndian, WriteBytesExt};

use crate::MAGIC;

pub type InMemoryPlainFileWriter = PlainFileWriter<Vec<u8>>;
pub type CompressedPlainFileWriter = PlainFileWriter<lz4::Encoder<Vec<u8>>>;

impl InMemoryPlainFileWriter {
    pub fn new() -> Result<Self> {
        Self::with_capacity_and_common_prefix(8 * 1024, b"")
    }

    pub fn with_capacity_and_common_prefix(cap: usize, common_prefix: &[u8]) -> Result<Self> {
        let mut writer = Self {
            buf: Vec::with_capacity(cap),
            common_prefix: common_prefix.to_owned(),
        };
        writer.write_header(common_prefix)?;
        Ok(writer)
    }

    pub fn compressed(self) -> Result<CompressedPlainFileWriter> {
        Ok(CompressedPlainFileWriter {
            buf: lz4::EncoderBuilder::new().build(self.buf)?,
            common_prefix: self.common_prefix,
        })
    }
}

impl CompressedPlainFileWriter {
    pub fn with_capacity_and_common_prefix(
        cap: usize,
        common_prefix: &[u8],
        compress_level: u32,
    ) -> Result<Self> {
        let mut writer = Self {
            buf: lz4::EncoderBuilder::new()
                .level(compress_level)
                .build(Vec::with_capacity(cap))?,
            common_prefix: common_prefix.to_owned(),
        };
        writer.write_header(common_prefix)?;
        Ok(writer)
    }
}

/// Buffer is a writeable in memory buffer.
pub trait Buffer: Write {
    type Reader: Read;
    fn len(&self) -> usize;
    fn finish(self) -> Self::Reader;
}

impl Buffer for Vec<u8> {
    type Reader = Cursor<Self>;

    fn len(&self) -> usize {
        self.len()
    }

    fn finish(self) -> Self::Reader {
        Cursor::new(self)
    }
}

impl Buffer for lz4::Encoder<Vec<u8>> {
    type Reader = Cursor<Vec<u8>>;

    fn len(&self) -> usize {
        self.writer().len()
    }

    fn finish(self) -> Self::Reader {
        let (w, r) = lz4::Encoder::finish(self);
        // todo: handle error with a always-error io reader.
        r.unwrap();
        Cursor::new(w)
    }
}

pub struct PlainFileWriter<W> {
    buf: W,
    common_prefix: Vec<u8>,
}

impl<W: Buffer> PlainFileWriter<W> {
    pub fn add(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.buf.write_u32::<BigEndian>(k.len() as _)?;
        self.buf.write_u32::<BigEndian>(v.len() as _)?;
        self.buf.write_all(k)?;
        self.buf.write_all(v)?;
        Ok(())
    }

    pub fn into_reader(self) -> impl std::io::Read {
        Buffer::finish(self.buf)
    }

    pub fn size(&self) -> usize {
        self.buf.len()
    }

    pub fn prefix(&self) -> &[u8] {
        &self.common_prefix
    }

    /// write the common header of the kv seq file.
    fn write_header(&mut self, common_prefix: &[u8]) -> Result<()> {
        // magic
        self.buf.write_all(MAGIC)?;
        // length
        self.buf.write_u32::<BigEndian>(common_prefix.len() as _)?;
        // actual key
        self.buf.write_all(common_prefix)
    }
}

#[cfg(test)]
mod writer_test {
    use super::{CompressedPlainFileWriter, InMemoryPlainFileWriter, MAGIC};
    use byteorder::{BigEndian, ReadBytesExt};

    fn assert_header<R: ReadBytesExt>(mut b: R, common_prefix: &[u8]) -> std::io::Result<()> {
        let mut magic = [0u8; 4];
        b.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "kvsequence: bad magic number detached",
            ));
        }
        let len = b.read_u32::<BigEndian>()?;
        assert_eq!(len, common_prefix.len() as u32);
        let mut items = Vec::with_capacity(common_prefix.len());
        b.read_exact(&mut items)?;
        assert_eq!(items.as_slice(), common_prefix);
        Ok(())
    }

    fn assert_next_key<R: ReadBytesExt>(mut b: R, k: &[u8], v: &[u8]) -> std::io::Result<()> {
        let (key_size, value_size) = (
            b.read_u32::<BigEndian>().unwrap_or(0),
            b.read_u32::<BigEndian>().unwrap_or(0),
        );
        assert_eq!(key_size as usize, k.len());
        assert_eq!(value_size as usize, v.len());
        let mut key = vec![0u8; key_size as _];
        b.read_exact(key.as_mut_slice())?;
        assert_eq!(key, k, "key mismatch {:?} vs {:?}", key, k);
        let mut value = vec![0u8; value_size as _];
        b.read_exact(value.as_mut_slice())?;
        assert_eq!(value, v);
        Ok(())
    }

    #[test]
    fn test_basic() -> std::io::Result<()> {
        let mut buf = Vec::<u8>::new();
        let mut writer = InMemoryPlainFileWriter::new()?;
        writer.add(b"key", b"hello,world!")?;
        writer.add(b"key2", b"bye,world!")?;
        let mut r = writer.into_reader();
        std::io::copy(&mut r, &mut buf)?;
        let mut b = buf.as_slice();
        assert_header(&mut b, b"")?;
        assert_next_key(&mut b, b"key", b"hello,world!")?;
        assert_next_key(&mut b, b"key2", b"bye,world!")?;
        Ok(())
    }

    #[test]
    fn test_compressed() -> std::io::Result<()> {
        let mut buf = Vec::<u8>::new();
        let mut writer =
            CompressedPlainFileWriter::with_capacity_and_common_prefix(1024 * 8, b"", 4)?;
        writer.add(b"key", b"hello,world!")?;
        writer.add(b"key2", b"bye,world!")?;
        let mut r = writer.into_reader();
        std::io::copy(&mut r, &mut buf)?;
        let mut b = lz4::Decoder::new(buf.as_slice())?;
        assert_header(&mut b, b"")?;
        assert_next_key(&mut b, b"key", b"hello,world!")?;
        assert_next_key(&mut b, b"key2", b"bye,world!")?;
        Ok(())
    }
}
