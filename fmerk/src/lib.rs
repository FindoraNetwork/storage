#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub use rocksdb;

/// Error and Result types.
mod error;
/// The top-level store API.
mod merk;
/// Provides a container type that allows temporarily taking ownership of a value.
// TODO: move this into its own crate
pub mod owner;
/// Algorithms for generating and verifying Merkle proofs.
mod proofs;
/// Various helpers useful for tests or benchmarks.
pub mod test_utils;
/// The core tree data structure.
pub mod tree;

pub use self::merk::Merk;
pub use error::{Error, Result};
pub use proofs::verify as verify_proof;
pub use tree::{Batch, BatchEntry, Hash, Op, PanicSource, HASH_LENGTH};

/// Converts KVEntry to BatchEntry
pub fn to_batch<I: IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>>(items: I) -> Vec<BatchEntry> {
    let mut batch = Vec::new();
    for (key, val) in items {
        match val {
            Some(val) => batch.push((key, Op::Put(val))),
            None => batch.push((key, Op::Delete)),
        }
    }
    batch
}
