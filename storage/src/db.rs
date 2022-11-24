use ruc::Result;
use std::iter::Iterator;
use std::path::Path;

/// types
pub type StoreKey = Vec<u8>;
pub type KValue = (StoreKey, Vec<u8>);
pub type KVEntry = (StoreKey, Option<Vec<u8>>);
pub type KVBatch = Vec<KVEntry>;
pub type DbIter<'a> = Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>;

#[derive(Debug)]
pub enum IterOrder {
    Asc,
    Desc,
}

/// Merkleized KV store interface
pub trait MerkleDB {
    fn root_hash(&self) -> Vec<u8>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn put_batch(&mut self, kvs: KVBatch) -> Result<()>;

    fn iter(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DbIter<'_>;

    fn iter_aux(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DbIter<'_>;

    fn db_all_iterator(&self, order: IterOrder) -> DbIter<'_>;

    fn commit(&mut self, kvs: KVBatch, flush: bool) -> Result<()>;

    fn snapshot<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    fn decode_kv(&self, kv_pair: (Box<[u8]>, Box<[u8]>)) -> KValue;

    #[inline]
    fn as_mut(&mut self) -> &mut Self {
        self
    }

    fn clean_aux(&mut self) -> Result<()>;
}
