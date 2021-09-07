// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use external_storage_export::ExternalStorage;
use file_system::Sha256Reader;
use futures_util::io::AllowStdIo;
use kvproto::brpb::File;
use seqkvs::writer::{CompressedPlainFileWriter, InMemoryPlainFileWriter};
use tikv::coprocessor::checksum_crc64_xor;
use tikv::storage::txn::TxnEntry;
use tikv_util::{
    self, box_err, error,
    time::{Instant, Limiter},
};

use crate::metrics::*;
use crate::{backup_file_name, Error, Result};

pub struct PlainWriter {
    writer: CompressedPlainFileWriter,
    pub total_kvs: u64,
    pub total_bytes: u64,
    pub checksum: u64,
    digest: crc64fast::Digest,
}

impl PlainWriter {
    pub fn with_common_prefix(prefix: &[u8], compress_level: u32) -> Self {
        Self {
            // default 96M
            writer: CompressedPlainFileWriter::with_capacity_and_common_prefix(
                96 * 1024 * 1024,
                prefix,
                compress_level,
            )
            // todo handle the exception
            .unwrap(),
            total_kvs: 0,
            total_bytes: 0,
            checksum: 0,
            digest: crc64fast::Digest::new(),
        }
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // HACK: The actual key stored in TiKV is called
        // data_key and always prefix a `z`. But iterator strips
        // it, we need to add the prefix manually.
        let data_key_write = keys::data_key(key);
        // strip the common prefix.
        self.writer
            .add(&data_key_write[self.writer.prefix().len()..], value)?;
        Ok(())
    }

    pub fn update_with(&mut self, entry: TxnEntry, need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        if need_checksum {
            entry.with_kvpair(|k, v| {
                self.total_bytes += (k.len() + v.len()) as u64;
                self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), &k, &v);
            })?;
        }
        Ok(())
    }

    pub fn update_raw_with(&mut self, key: &[u8], value: &[u8], need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        self.total_bytes += (key.len() + value.len()) as u64;
        if need_checksum {
            self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), key, value);
        }
        Ok(())
    }

    pub fn save_and_build_file(
        mut self,
        name: &str,
        cf: &'static str,
        limiter: Limiter,
        storage: &dyn ExternalStorage,
    ) -> Result<File> {
        let size = self.writer.size() as u64;
        let sst_reader = self.writer.into_reader();
        BACKUP_RANGE_SIZE_HISTOGRAM_VEC
            .with_label_values(&[cf])
            .observe(size as f64);
        let file_name = format!("{}_{}.sst", name, cf);

        let (reader, hasher) = Sha256Reader::new(sst_reader)
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;
        storage.write(
            &file_name,
            Box::new(limiter.limit(AllowStdIo::new(reader))),
            size,
        )?;
        let sha256 = hasher
            .lock()
            .unwrap()
            .finish()
            .map(|digest| digest.to_vec())
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;

        let mut file = File::default();
        file.set_name(file_name);
        file.set_sha256(sha256);
        file.set_crc64xor(self.checksum);
        file.set_total_kvs(self.total_kvs);
        file.set_total_bytes(self.total_bytes);
        file.set_cf(cf.to_owned());
        file.set_size(size);
        Ok(file)
    }

    pub fn is_empty(&self) -> bool {
        self.total_kvs == 0
    }
}
