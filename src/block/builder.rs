#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    current_size : usize,
    target_size : usize,
    block : Box<Option<Block>>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        return BlockBuilder { 
            current_size: 0, 
            target_size: block_size, 
            block: Box::new(None) 
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        if self.current_size + key.len() + value.len() + 4 > self.target_size {
            return false;
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.current_size == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        unimplemented!()
    }
}
