mod merk_db;
mod rocks_db;
mod temp_db;
mod temp_db_rocks;

use merk::{rocksdb, BatchEntry, Op};
pub use merk_db::{FinDB, MerkleDB};
pub use rocks_db::RocksDB;
pub use temp_db::TempFinDB;
pub use temp_db_rocks::TempRocksDB;

/// types
pub type StoreKey = Vec<u8>;
pub type KValue = (StoreKey, Vec<u8>);
pub type KVEntry = (StoreKey, Option<Vec<u8>>);
pub type KVBatch = Vec<KVEntry>;

/// iterator
pub type DBIter<'a> = rocksdb::DBIterator<'a>;
pub enum IterOrder {
    Asc,
    Desc,
}

/// Converts KVEntry to BatchEntry
pub fn to_batch<I: IntoIterator<Item = KVEntry>>(items: I) -> Vec<BatchEntry> {
    let mut batch = Vec::new();
    for (key, val) in items {
        match val {
            Some(val) => batch.push((key, Op::Put(val))),
            None => batch.push((key, Op::Delete)),
        }
    }
    batch
}
