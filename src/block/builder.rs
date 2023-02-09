#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    current_size: usize,
    target_size: usize,
    last_offset: u16,
    num_of_elements: u16,
    block: Box<Block>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        return BlockBuilder { 
            current_size: 0, 
            target_size: block_size, 
            last_offset: 0,
            num_of_elements: 0,
            block: Box::new(Block {data: Vec::new(), offsets: Vec::new()}) 
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        // check size
        if self.current_size + key.len() + value.len() + 7 > self.target_size { // 4 + 3
            return false;
        }
        // append new entry in data vec
        let key_len_b= (key.len() as u16).to_be_bytes();
        let value_len_b = (value.len() as u16).to_be_bytes();
        self.block.data.append(&mut key_len_b.to_vec());
        self.block.data.append(&mut key.to_vec());
        self.block.data.append(&mut value_len_b.to_vec());
        self.block.data.append(&mut value.to_vec());
        // append offset in offset vec
        self.block.offsets.push(self.last_offset);
        // update builder
        self.current_size += key.len() + value.len() + 6; // 4 + 2
        self.last_offset = self.block.data.len() as u16; 
        self.num_of_elements += 1;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.current_size == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        *self.block
    }
}
