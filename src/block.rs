mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let offsets_end = data.len() - SIZEOF_U16;
        let num_entry = (&data[data.len() - SIZEOF_U16..]).get_u16();
        let offsets_begin = data.len() - ((num_entry + 1) as usize) * SIZEOF_U16;
        // decode the data and offsets
        let decode_data = data[..offsets_begin].to_vec();
        let decode_offsets = data[offsets_begin..offsets_end]
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        Self {
            data: decode_data,
            offsets: decode_offsets,
        }
    }
}

#[cfg(test)]
mod tests;
