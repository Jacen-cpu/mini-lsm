use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    idx: usize,
    is_valid: bool,
}

impl SsTableIterator {
    pub fn new(table: Arc<SsTable>, block_iter: BlockIterator) -> Self {
        Self {
            table,
            block_iter,
            idx: 0,
            is_valid: true,
        }
    }
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_iter = SsTableIterator::seek_block_iter(0, &table)?;
        let mut iter = SsTableIterator::new(table, block_iter);
        iter.seek_to_first()?;
        Ok(iter)
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to_block(0)?;
        self.block_iter.seek_to_first();
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let block_iter = SsTableIterator::seek_block_iter(0, &table)?;
        let mut iter = SsTableIterator::new(table, block_iter);
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let metas = &self.table.block_metas;
        let mut low = 0;
        let mut high = metas.len();
        while low < high - 1 {
            let mid = low + (high - low) / 2;
            let first_key = &metas[mid].first_key[..];
            match first_key.cmp(key) {
                std::cmp::Ordering::Less => low = mid,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    self.seek_to_block(mid)?;
                    self.block_iter.seek_to_key(key);
                    return Ok(());
                }
            }
        }
        self.seek_to_block(low)?;
        if self.is_valid {
            self.block_iter.seek_to_key(key);
            if !self.block_iter.is_valid() {
                self.seek_to_block(high)?;
                self.block_iter.seek_to_key(key);
            }
            // assert!(self.block_iter.is_valid());
        }
        Ok(())
    }

    /// Seek to the target block
    pub fn seek_to_block(&mut self, idx: usize) -> Result<()> {
        self.is_valid = true;
        if idx == self.idx {
            return Ok(());
        }
        if idx >= self.table.block_metas.len() {
            self.is_valid = false;
            return Ok(());
        }
        self.block_iter = Self::seek_block_iter(idx, &self.table)?;
        self.idx = idx;
        Ok(())
    }

    /// Seek the target block iter and return it
    pub fn seek_block_iter(idx: usize, table: &Arc<SsTable>) -> Result<BlockIterator> {
        let metas = &table.block_metas;
        let block_end = metas[idx].offset as u64;
        let block_begin = if idx > 0 {
            metas[idx - 1].offset as u64
        } else {
            0 as u64
        };
        let block_len = block_end - block_begin;
        let block = table.file.read(block_begin, block_len)?;
        Ok(BlockIterator::create_and_seek_to_first(Arc::new(
            Block::decode(&block),
        )))
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn key(&self) -> &[u8] {
        assert!(self.block_iter.is_valid());
        self.block_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            self.seek_to_block(self.idx + 1)?;
        }
        Ok(())
    }
}
