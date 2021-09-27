use fmerk::Op;
use ruc::*;
use std::path::{Path, PathBuf};

use crate::db::merk_db::{to_batch, DBIter};
use crate::db::{IterOrder, KVBatch};

const CF_STATE: &str = "state";

/// RocksDB KV store interface
pub trait IRocksDB {
    /// Gets a value for the given key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Gets range iterator
    fn iter(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DBIter;

    /// Commits changes.
    fn commit(&mut self, kvs: KVBatch) -> Result<()>;
}

/// Rocks db
pub struct RocksDB {
    db: rocksdb::DB,
    path: PathBuf,
}

impl RocksDB {
    /// Opens a store with the specified file path. If no store exists at that
    /// path, one will be created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_opts = Self::default_db_opts();
        Self::open_opt(path, db_opts)
    }

    /// Closes the store and deletes all data from disk.
    pub fn destroy(self) -> Result<()> {
        let opts = Self::default_db_opts();
        let path = self.path.clone();
        drop(self);
        rocksdb::DB::destroy(&opts, path).c(d!())?;
        Ok(())
    }

    /// Opens a store with the specified file path and the given options. If no
    /// store exists at that path, one will be created.
    fn open_opt<P>(path: P, db_opts: rocksdb::Options) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let cfs = vec![rocksdb::ColumnFamilyDescriptor::new(
            CF_STATE,
            Self::default_db_opts(),
        )];
        let db = rocksdb::DB::open_cf_descriptors(&db_opts, &path_buf, cfs).c(d!())?;

        Ok(Self { db, path: path_buf })
    }

    fn default_db_opts() -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_allow_mmap_writes(true);
        opts.set_allow_mmap_reads(true);
        opts.create_missing_column_families(true);
        opts.set_atomic_flush(true);
        opts
    }

    fn iter_opt(
        &self,
        mode: rocksdb::IteratorMode,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBIterator {
        let aux_cf = self.db.cf_handle(CF_STATE).unwrap();
        self.db.iterator_cf_opt(aux_cf, readopts, mode)
    }
}

impl IRocksDB for RocksDB {
    /// Gets a value for the given key. If the key is not found, `None` is returned.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(cf) = self.db.cf_handle(CF_STATE) {
            Ok(self.db.get_cf(cf, key).c(d!("get data failed"))?)
        } else {
            Ok(None)
        }
    }

    /// Gets range iterator
    fn iter(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DBIter {
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(lower.to_vec());
        readopts.set_iterate_upper_bound(upper.to_vec());
        match order {
            IterOrder::Asc => self.iter_opt(rocksdb::IteratorMode::Start, readopts),
            IterOrder::Desc => self.iter_opt(rocksdb::IteratorMode::End, readopts),
        }
    }

    /// Commits changes.
    fn commit(&mut self, kvs: KVBatch) -> Result<()> {
        // update cf in batch
        let batch_kvs = to_batch(kvs);
        let state_cf = self.db.cf_handle(CF_STATE).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in batch_kvs {
            match value {
                Op::Put(value) => batch.put_cf(state_cf, key, value),
                Op::Delete => batch.delete_cf(state_cf, key),
            };
        }

        // write to db
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(false);
        self.db.write_opt(batch, &opts).c(d!())?;

        // flush
        self.db
            .flush()
            .map_err(|_| eg!("Failed to flush memtables"))?;

        Ok(())
    }
}
