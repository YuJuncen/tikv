// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{codec::Result, Either};
use bytes::{Buf, Bytes};
use std::io::prelude::*;
use std::io::Cursor;

pub trait Iterator {
    fn next(&mut self) -> Result<()>;

    fn valid(&self) -> bool;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];
}

pub struct EventIterator {
    buf: Cursor<Vec<u8>>,
    key_pos: usize,
    value_pos: usize,
    key_len: usize,
    value_len: usize,
}

impl EventIterator {
    pub fn new(buf: Vec<u8>) -> EventIterator {
        EventIterator {
            buf: Cursor::new(buf),
            key_pos: 0,
            key_len: 0,
            value_pos: 0,
            value_len: 0,
        }
    }
}

impl Iterator for EventIterator {
    fn next(&mut self) -> Result<()> {
        if self.valid() {
            self.key_len = self.buf.get_u32_le() as usize;
            self.key_pos = self.buf.position() as _;
            self.buf.set_position((self.key_pos + self.key_len) as u64);
            self.value_len = self.buf.get_u32_le() as usize;
            self.value_pos = self.buf.position() as _;
            self.buf
                .set_position((self.value_pos + self.value_len) as u64);
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        (self.buf.position() as usize) < self.buf.get_ref().len()
    }

    fn key(&self) -> &[u8] {
        &self.buf.get_ref()[self.key_pos..self.key_pos + self.key_len]
    }

    fn value(&self) -> &[u8] {
        &self.buf.get_ref()[self.value_pos..self.value_pos + self.value_len]
    }
}

#[derive(Clone)]
pub struct EventEncoder;

impl EventEncoder {
    pub fn encode_event<'e>(key: &'e [u8], value: &'e [u8]) -> [impl AsRef<[u8]> + 'e; 4] {
        let key_len = (key.len() as u32).to_le_bytes();
        let val_len = (value.len() as u32).to_le_bytes();
        [
            Either::Left(key_len),
            Either::Right(key),
            Either::Left(val_len),
            Either::Right(value),
        ]
    }

    #[allow(dead_code)]
    fn decode_event(e: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut buf = Cursor::new(Bytes::from(e.to_vec()));
        let len = buf.get_u32_le() as usize;
        let mut key = vec![0; len];
        buf.read_exact(key.as_mut_slice()).unwrap();
        let len = buf.get_u32_le() as usize;
        let mut val = vec![0; len];
        buf.read_exact(val.as_mut_slice()).unwrap();
        (key, val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_encode_decode() {
        let mut rng = rand::thread_rng();
        for _i in 0..10 {
            let key: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let val: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let e = EventEncoder::encode_event(&key, &val);
            let mut event = vec![];
            for s in e {
                event.extend_from_slice(s.as_ref());
            }
            let (decoded_key, decoded_val) = EventEncoder::decode_event(&event);
            assert_eq!(key, decoded_key);
            assert_eq!(val, decoded_val);
        }
    }

    #[test]
    fn test_decode_events() {
        let mut rng = rand::thread_rng();
        let mut event = vec![];
        let mut keys = vec![];
        let mut vals = vec![];
        let count = 20;

        for _i in 0..count {
            let key: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let val: Vec<u8> = (0..100).map(|_| rng.gen_range(0..255)).collect();
            let e = EventEncoder::encode_event(&key, &val);
            for s in e {
                event.extend_from_slice(s.as_ref());
            }
            keys.push(key);
            vals.push(val);
        }

        let mut iter = EventIterator::new(event);

        let mut index = 0_usize;
        loop {
            if !iter.valid() {
                break;
            }
            iter.next().unwrap();
            assert_eq!(iter.key(), keys[index]);
            assert_eq!(iter.value(), vals[index]);
            index += 1;
        }
        assert_eq!(count, index);
    }
}
