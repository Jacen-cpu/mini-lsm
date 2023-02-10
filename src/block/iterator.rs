#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{sync::Arc};

use super::{Block, SIZEOF_U16};
use bytes::{BufMut, Buf};

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }
    
    /// help method to seek to
    fn seek_to(&mut self, idx: usize) {
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    } 
    
    /// help method to decord the kv
    fn seek_to_offset(&mut self, offset: usize) {
        // get the kv position
        let key_len = (&self.block.data[offset..]).get_u16();
        let key_begin = offset + SIZEOF_U16;
        let key_end = key_begin + key_len as usize;
        let value_len = (&self.block.data[key_end..]).get_u16();
        let value_begin = key_end + SIZEOF_U16; 
        let value_end = value_begin + value_len as usize;
        // clear the dirty data
        self.key.clear();
        self.value.clear();
        // fill the vec
        self.key.put(&self.block.data[key_begin..key_end]);
        self.value.put(&self.block.data[value_begin..value_end]);
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        return !self.key.is_empty();
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }
}
