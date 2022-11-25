use fin_db::RocksDB;
use ruc::*;
use std::env::temp_dir;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::time::SystemTime;
use storage::db::{DbIter, IterOrder, KVBatch, KValue, MerkleDB};

/// Wraps a RocksDB instance and deletes it from disk it once it goes out of scope.
pub struct TempRocksDB {
    inner: Option<RocksDB>,
}

impl TempRocksDB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<TempRocksDB> {
        let inner = Some(RocksDB::open(path)?);
        Ok(TempRocksDB { inner })
    }

    /// Opens a `TempRocksDB` at an autogenerated, temporary file path.
    pub fn new() -> Result<TempRocksDB> {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut path = temp_dir();
        path.push(format!("temp-rocksdb–{}", time));
        TempRocksDB::open(path)
    }

    /// Closes db and deletes all data from disk.
    fn destroy(&mut self) -> Result<()> {
        self.inner.take().unwrap().destroy()
    }
}

impl MerkleDB for TempRocksDB {
    fn root_hash(&self) -> Vec<u8> {
        self.deref().root_hash()
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.deref().get(key)
    }

    fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.deref().get(key)
    }

    fn put_batch(&mut self, kvs: KVBatch) -> Result<()> {
        self.deref_mut().put_batch(kvs)
    }

    fn iter(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DbIter<'_> {
        self.deref().iter(lower, upper, order)
    }

    fn iter_aux(&self, lower: &[u8], upper: &[u8], order: IterOrder) -> DbIter<'_> {
        self.deref().iter(lower, upper, order)
    }
    fn db_all_iterator(&self, order: IterOrder) -> DbIter<'_> {
        self.deref().db_all_iterator(order)
    }
    fn commit(&mut self, kvs: KVBatch, flush: bool) -> Result<()> {
        self.deref_mut().commit(kvs, flush)
    }

    fn snapshot<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.deref().snapshot(path)
    }

    fn decode_kv(&self, kv_pair: (Box<[u8]>, Box<[u8]>)) -> KValue {
        self.deref().decode_kv(kv_pair)
    }

    fn clean_aux(&mut self) -> Result<()> {
        self.deref_mut().clean_aux()
    }
    fn export_aux(&mut self, cs: &mut Self) -> Result<()> {
        self.deref_mut().export_aux(cs)
    }
}

impl Deref for TempRocksDB {
    type Target = RocksDB;
    fn deref(&self) -> &RocksDB {
        self.inner.as_ref().unwrap()
    }
}

impl DerefMut for TempRocksDB {
    fn deref_mut(&mut self) -> &mut RocksDB {
        self.inner.as_mut().unwrap()
    }
}

impl Drop for TempRocksDB {
    fn drop(&mut self) {
        self.destroy().expect("failed to delete db");
    }
}

#[cfg(test)]
mod tests {
    use super::TempRocksDB;
    use std::thread;
    use storage::db::{IterOrder, MerkleDB};

    /*
    #[test]
    fn db_export_aux() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempRocksDB::open(path).expect("failed to open db");

        fdb.commit(vec![(b"k10".to_vec(), Some(b"v10".to_vec()))], false)
            .unwrap();
        fdb.commit(vec![(b"height".to_vec(), Some(b"100".to_vec()))], false)
            .unwrap();

        // update data at height
        fdb.commit(
            vec![
                (b"k10".to_vec(), Some(b"v12".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
            ],
            false,
        )
        .unwrap();
        // commit data with aux
        fdb.commit(vec![(b"height".to_vec(), Some(b"101".to_vec()))], false)
            .unwrap();

        // get and compare
        assert_eq!(fdb.get_aux(b"k10").unwrap(), Some(b"v12".to_vec()));
        assert_eq!(fdb.get_aux(b"k20").unwrap(), Some(b"v20".to_vec()));
        assert_eq!(fdb.get_aux(b"height").unwrap().unwrap(), b"101".to_vec());

        let mut cs_fdb = TempRocksDB::new().unwrap();
        fdb.export_aux(&mut cs_fdb).unwrap();
        assert_eq!(cs_fdb.get_aux(b"k10").unwrap(), Some(b"v12".to_vec()));
        assert_eq!(cs_fdb.get_aux(b"k20").unwrap(), Some(b"v20".to_vec()));
        assert_eq!(cs_fdb.get_aux(b"height").unwrap().unwrap(), b"101".to_vec());

        cs_fdb
            .commit(vec![(b"k10".to_vec(), Some(b"v10".to_vec()))], false)
            .unwrap();
        cs_fdb
            .commit(vec![(b"height".to_vec(), Some(b"100".to_vec()))], false)
            .unwrap();

        assert_eq!(cs_fdb.get_aux(b"k10").unwrap(), Some(b"v10".to_vec()));
        assert_eq!(cs_fdb.get_aux(b"k20").unwrap(), Some(b"v20".to_vec()));
        assert_eq!(cs_fdb.get_aux(b"height").unwrap().unwrap(), b"100".to_vec());

        let mut new_cs_fdb = TempRocksDB::new().unwrap();
        cs_fdb.export_aux(&mut new_cs_fdb).unwrap();
        new_cs_fdb
            .commit(vec![(b"height".to_vec(), Some(b"101".to_vec()))], false)
            .unwrap();

        assert_eq!(new_cs_fdb.get_aux(b"k10").unwrap(), Some(b"v10".to_vec()));
        assert_eq!(new_cs_fdb.get_aux(b"k20").unwrap(), Some(b"v20".to_vec()));
        assert_eq!(
            new_cs_fdb.get_aux(b"height").unwrap().unwrap(),
            b"101".to_vec()
        );
    }*/

    #[test]
    fn db_del_n_get() {
        let path = thread::current().name().unwrap().to_owned();
        let mut db = TempRocksDB::open(path).expect("failed to open db");

        // commit data
        db.commit(
            vec![
                (b"k10".to_vec(), Some(b"v10".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
            ],
            true,
        )
        .unwrap();

        // del data at height 101
        db.commit(vec![(b"k10".to_vec(), None), (b"k20".to_vec(), None)], true)
            .unwrap();

        // get and compare
        assert_eq!(db.get(b"k10").unwrap(), None);
        assert_eq!(db.get(b"k20").unwrap(), None);
    }

    #[test]
    fn db_put_n_update() {
        let path = thread::current().name().unwrap().to_owned();
        let mut db = TempRocksDB::open(path).expect("failed to open db");

        // commit data
        db.commit(vec![(b"k10".to_vec(), Some(b"v10".to_vec()))], true)
            .unwrap();

        // update data at height
        db.commit(
            vec![
                (b"k10".to_vec(), Some(b"v12".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
            ],
            true,
        )
        .unwrap();

        // get and compare
        assert_eq!(db.get(b"k10").unwrap(), Some(b"v12".to_vec()));
        assert_eq!(db.get(b"k20").unwrap(), Some(b"v20".to_vec()));
    }

    #[test]
    fn del_n_iter_range() {
        let path = thread::current().name().unwrap().to_owned();
        let mut db = TempRocksDB::open(path).expect("failed to open db");

        // commit data
        db.commit(
            vec![
                (b"k10".to_vec(), Some(b"v10".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
                (b"k30".to_vec(), Some(b"v30".to_vec())),
                (b"k40".to_vec(), Some(b"v40".to_vec())),
                (b"k50".to_vec(), Some(b"v50".to_vec())),
            ],
            true,
        )
        .unwrap();

        // del data at height 101
        db.commit(vec![(b"k20".to_vec(), None), (b"k40".to_vec(), None)], true)
            .unwrap();

        // iterate data on range ["k10", "k50")
        let iter = db.iter(b"k10", b"k50", IterOrder::Asc);
        let expected = vec![
            (b"k10".to_vec(), b"v10".to_vec()),
            (b"k30".to_vec(), b"v30".to_vec()),
        ];
        let actual = iter
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(expected, actual);
    }

    #[test]
    fn iter_range_inc() {
        let path = thread::current().name().unwrap().to_owned();
        let mut db = TempRocksDB::open(path).expect("failed to open db");

        // commit data
        db.commit(
            vec![
                (b"k10".to_vec(), Some(b"v10".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
                (b"k30".to_vec(), Some(b"v30".to_vec())),
                (b"k40".to_vec(), Some(b"v40".to_vec())),
                (b"k50".to_vec(), Some(b"v50".to_vec())),
            ],
            true,
        )
        .unwrap();

        // iterate data on range ["k20", "k50")
        let iter = db.iter(b"k20", b"k50", IterOrder::Asc);
        let expected = vec![
            (b"k20".to_vec(), b"v20".to_vec()),
            (b"k30".to_vec(), b"v30".to_vec()),
            (b"k40".to_vec(), b"v40".to_vec()),
        ];
        let actual = iter
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(expected, actual);
    }

    #[test]
    fn iter_range_desc() {
        let path = thread::current().name().unwrap().to_owned();
        let mut db = TempRocksDB::open(path).expect("failed to open db");

        // commit data
        db.commit(
            vec![
                (b"k10".to_vec(), Some(b"v10".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
                (b"k30".to_vec(), Some(b"v30".to_vec())),
                (b"k40".to_vec(), Some(b"v40".to_vec())),
                (b"k50".to_vec(), Some(b"v50".to_vec())),
            ],
            true,
        )
        .unwrap();

        // iterate data on range ["k20", "k50")
        let iter = db.iter(b"k20", b"k50", IterOrder::Desc);
        let expected = vec![
            (b"k40".to_vec(), b"v40".to_vec()),
            (b"k30".to_vec(), b"v30".to_vec()),
            (b"k20".to_vec(), b"v20".to_vec()),
        ];
        let actual = iter
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(expected, actual);
    }
}
