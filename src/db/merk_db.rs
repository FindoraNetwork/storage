use crate::db::{to_batch, DBIter, IterOrder, KVBatch, KValue};
use merk::{rocksdb, tree::Tree, Merk};
use ruc::*;
use std::path::Path;

/// Merkleized KV store interface
pub trait MerkleDB {
    fn root_hash(&self) -> Vec<u8>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn put_batch(&mut self, kvs: KVBatch) -> Result<()>;

    fn iter(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DBIter;

    fn iter_aux(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DBIter;

    fn commit(&mut self, kvs: KVBatch, flush: bool) -> Result<()>;

    fn snapshot<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    fn decode_kv(&self, kv_pair: (Box<[u8]>, Box<[u8]>)) -> KValue;

    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

/// Findora db

pub struct FinDB {
    db: Merk,
}

impl FinDB {
    /// Opens a db with the specified file path. If no db exists at that
    ///
    /// path, one will be created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<FinDB> {
        let db = Merk::open(path).map_err(|_| eg!("Failed to open db"))?;
        Ok(Self { db })
    }

    /// Closes db and deletes all data from disk.
    pub fn destroy(self) -> Result<()> {
        self.db.destroy().map_err(|_| eg!("Failed to destory db"))
    }
}

impl MerkleDB for FinDB {
    /// Returns the root hash of the tree (a digest for the entire db which
    ///
    /// proofs can be checked against). If the tree is empty, returns the null
    ///
    /// hash (zero-filled).
    fn root_hash(&self) -> Vec<u8> {
        self.db.root_hash().to_vec()
    }

    /// Gets a value for the given key. If the key is not found, `None` is returned.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .get(key)
            .map_err(|_| eg!("Failed to get data from db"))
    }

    /// Gets an auxiliary value.
    fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .get_aux(key)
            .map_err(|_| eg!("Failed to get aux from db"))
    }

    /// Puts a batch of KVs
    fn put_batch(&mut self, kvs: KVBatch) -> Result<()> {
        let batch = to_batch(kvs);
        self.db
            .apply(batch.as_ref())
            .map_err(|_| eg!("Failed to put batch data to db"))
    }

    /// Gets range iterator
    fn iter(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DBIter {
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(lower.to_vec());
        readopts.set_iterate_upper_bound(upper.to_vec());
        match order {
            IterOrder::Asc => self.db.iter_opt(rocksdb::IteratorMode::Start, readopts),
            IterOrder::Desc => self.db.iter_opt(rocksdb::IteratorMode::End, readopts),
        }
    }

    /// Gets range iterator for aux
    fn iter_aux(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DBIter {
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(lower.to_vec());
        readopts.set_iterate_upper_bound(upper.to_vec());
        match order {
            IterOrder::Asc => self.db.iter_opt_aux(rocksdb::IteratorMode::Start, readopts),
            IterOrder::Desc => self.db.iter_opt_aux(rocksdb::IteratorMode::End, readopts),
        }
    }

    /// Commits changes.
    fn commit(&mut self, aux: KVBatch, flush: bool) -> Result<()> {
        let batch_aux = to_batch(aux);
        self.db
            .commit(batch_aux.as_ref())
            .map_err(|_| eg!("Failed to commit to db"))?;
        if flush {
            self.db
                .flush()
                .map_err(|_| eg!("Failed to flush memtables"))?;
        }
        Ok(())
    }

    /// Takes a snapshot using checkpoint
    fn snapshot<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.db
            .snapshot(path)
            .map_err(|_| eg!("Failed to take snapshot"))?;
        Ok(())
    }

    /// Decode key value pair
    fn decode_kv(&self, kv_pair: (Box<[u8]>, Box<[u8]>)) -> KValue {
        let kv = Tree::decode(kv_pair.0.to_vec(), &kv_pair.1);
        (kv.key().to_vec(), kv.value().to_vec())
    }
}
