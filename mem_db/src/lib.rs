use ruc::*;
use std::collections::BTreeMap;
use std::path::Path;
use storage::db::{IterOrder, KVBatch, KValue, MerkleDB};

/// Wraps a Findora db instance and deletes it from disk it once it goes out of scope.
pub struct MemoryDB {
    root: Vec<u8>,
    inner: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryDB {
    pub fn new() -> Result<MemoryDB> {
        Ok(MemoryDB {
            root: vec![],
            inner: BTreeMap::new(),
        })
    }

    /// Closes db and deletes all data from disk.
    fn destroy(&mut self) {
        self.root.clear();
        self.inner.clear();
    }
}

impl MerkleDB for MemoryDB {
    fn root_hash(&self) -> Vec<u8> {
        self.root.clone()
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.get(key).cloned())
    }

    fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.get(key).cloned())
    }

    fn put_batch(&mut self, kvs: KVBatch) -> Result<()> {
        todo!()
    }

    fn iter(
        &self,
        lower: &[u8],
        upper: &[u8],
        order: IterOrder,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_> {
        todo!()
    }

    fn iter_aux(
        &self,
        lower: &[u8],
        upper: &[u8],
        order: IterOrder,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_> {
        todo!()
    }

    fn commit(&mut self, aux: KVBatch, flush: bool) -> Result<()> {
        todo!()
    }

    fn snapshot<P: AsRef<Path>>(&self, _path: P) -> Result<()> {
        todo!()
    }

    fn decode_kv(&self, kv_pair: (Box<[u8]>, Box<[u8]>)) -> KValue {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryDB;
    use std::thread;
    use storage::db::{IterOrder, MerkleDB};

    #[test]
    fn db_put_n_get() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ])
        .unwrap();
        // commit data with aux
        fdb.commit(vec![(b"height".to_vec(), Some(b"100".to_vec()))], false)
            .unwrap();

        // get and compare
        assert_eq!(fdb.get(b"k10").unwrap().unwrap(), b"v10".to_vec());
        assert_eq!(fdb.get(b"k20").unwrap().unwrap(), b"v20".to_vec());
        assert_eq!(fdb.get_aux(b"height").unwrap().unwrap(), b"100".to_vec());
    }

    #[test]
    fn db_del_n_get() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ])
        .unwrap();
        // commit data with aux
        fdb.commit(vec![(b"height".to_vec(), Some(b"100".to_vec()))], false)
            .unwrap();

        // del data at height 101
        fdb.put_batch(vec![(b"k10".to_vec(), None), (b"k20".to_vec(), None)])
            .unwrap();
        // commit data with aux
        fdb.commit(vec![(b"height".to_vec(), Some(b"101".to_vec()))], false)
            .unwrap();

        // get and compare
        assert_eq!(fdb.get(b"k10").unwrap(), None);
        assert_eq!(fdb.get(b"k20").unwrap(), None);
        assert_eq!(fdb.get_aux(b"height").unwrap().unwrap(), b"101".to_vec());
    }

    #[test]
    fn db_put_n_update() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data
        fdb.put_batch(vec![(b"k10".to_vec(), Some(b"v10".to_vec()))])
            .unwrap();
        // commit data with aux
        fdb.commit(vec![(b"height".to_vec(), Some(b"100".to_vec()))], false)
            .unwrap();

        // update data at height
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v12".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ])
        .unwrap();
        // commit data with aux
        fdb.commit(vec![(b"height".to_vec(), Some(b"101".to_vec()))], false)
            .unwrap();

        // get and compare
        assert_eq!(fdb.get(b"k10").unwrap(), Some(b"v12".to_vec()));
        assert_eq!(fdb.get(b"k20").unwrap(), Some(b"v20".to_vec()));
        assert_eq!(fdb.get_aux(b"height").unwrap().unwrap(), b"101".to_vec());
    }

    #[test]
    fn del_n_iter_range() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data and commit
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
            (b"k40".to_vec(), Some(b"v40".to_vec())),
            (b"k50".to_vec(), Some(b"v50".to_vec())),
        ])
        .unwrap();
        fdb.commit(vec![(b"height".to_vec(), Some(b"100".to_vec()))], false)
            .unwrap();

        // del data at height 101
        fdb.put_batch(vec![(b"k20".to_vec(), None), (b"k40".to_vec(), None)])
            .unwrap();
        // commit data with aux
        fdb.commit(vec![(b"height".to_vec(), Some(b"101".to_vec()))], false)
            .unwrap();

        // iterate data on range ["k10", "k50")
        let iter = fdb.iter(b"k10", b"k50", IterOrder::Asc);
        let expected = vec![
            (b"k10".to_vec(), b"v10".to_vec()),
            (b"k30".to_vec(), b"v30".to_vec()),
        ];
        let actual = iter
            .map(|(k, v)| {
                let kv = Tree::decode(k.to_vec(), &v);
                (kv.key().to_vec(), kv.value().to_vec())
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, actual);
        assert_eq!(fdb.get_aux(b"height").unwrap().unwrap(), b"101".to_vec());
    }

    #[test]
    fn iter_range_inc() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
            (b"k40".to_vec(), Some(b"v40".to_vec())),
            (b"k50".to_vec(), Some(b"v50".to_vec())),
        ])
        .unwrap();
        // commit data with aux
        fdb.commit(
            vec![
                (b"k11".to_vec(), Some(b"v11".to_vec())),
                (b"k21".to_vec(), Some(b"v21".to_vec())),
                (b"k31".to_vec(), Some(b"v31".to_vec())),
                (b"k41".to_vec(), Some(b"v41".to_vec())),
                (b"k51".to_vec(), Some(b"v51".to_vec())),
            ],
            true,
        )
        .unwrap();

        // iterate data on range ["k20", "k50")
        let iter = fdb.iter(b"k20", b"k50", IterOrder::Asc);
        let expected = vec![
            (b"k20".to_vec(), b"v20".to_vec()),
            (b"k30".to_vec(), b"v30".to_vec()),
            (b"k40".to_vec(), b"v40".to_vec()),
        ];
        let actual = iter
            .map(|(k, v)| {
                let kv = Tree::decode(k.to_vec(), &v);
                (kv.key().to_vec(), kv.value().to_vec())
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, actual);

        // iterate aux on range ["k21", "k51")
        let iter_aux = fdb.iter_aux(b"k21", b"k51", IterOrder::Asc);
        let expected_aux = vec![
            (b"k21".to_vec(), b"v21".to_vec()),
            (b"k31".to_vec(), b"v31".to_vec()),
            (b"k41".to_vec(), b"v41".to_vec()),
        ];
        let actual_aux = iter_aux
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(expected_aux, actual_aux);
    }

    #[test]
    fn iter_range_desc() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data and commit
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
            (b"k40".to_vec(), Some(b"v40".to_vec())),
            (b"k50".to_vec(), Some(b"v50".to_vec())),
        ])
        .unwrap();
        fdb.commit(vec![], true).unwrap();

        // iterate data on range ["k20", "k50")
        let iter = fdb.iter(b"k20", b"k50", IterOrder::Desc);
        let expected = vec![
            (b"k40".to_vec(), b"v40".to_vec()),
            (b"k30".to_vec(), b"v30".to_vec()),
            (b"k20".to_vec(), b"v20".to_vec()),
        ];
        let actual = iter
            .map(|(k, v)| {
                let kv = Tree::decode(k.to_vec(), &v);
                (kv.key().to_vec(), kv.value().to_vec())
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, actual);

        // commit aux
        fdb.commit(
            vec![
                (b"k11".to_vec(), Some(b"v11".to_vec())),
                (b"k21".to_vec(), Some(b"v21".to_vec())),
                (b"k31".to_vec(), Some(b"v31".to_vec())),
                (b"k41".to_vec(), Some(b"v41".to_vec())),
                (b"k51".to_vec(), Some(b"v51".to_vec())),
            ],
            true,
        )
        .unwrap();

        // iterate aux on range ["k21", "k51")
        let iter_aux = fdb.iter_aux(b"k21", b"k51", IterOrder::Desc);
        let expected_aux = vec![
            (b"k41".to_vec(), b"v41".to_vec()),
            (b"k31".to_vec(), b"v31".to_vec()),
            (b"k21".to_vec(), b"v21".to_vec()),
        ];
        let actual_aux = iter_aux
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(expected_aux, actual_aux);
    }

    #[test]
    fn db_snapshot() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path.clone()).expect("failed to open db");

        // put data
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
            (b"k40".to_vec(), Some(b"v40".to_vec())),
        ])
        .unwrap();

        // commit with some aux
        fdb.commit(
            vec![
                (b"k11".to_vec(), Some(b"v11".to_vec())),
                (b"k21".to_vec(), Some(b"v21".to_vec())),
                (b"k31".to_vec(), Some(b"v31".to_vec())),
            ],
            true,
        )
        .unwrap();

        // take snapshot
        let path_cp = format!("{}_cp", path);
        fdb.snapshot(path_cp.clone()).unwrap();

        // verify data
        let fdb_cp = TempFinDB::open(path_cp).expect("failed to open snapshot");
        assert_eq!(fdb_cp.get(b"k10").unwrap().unwrap(), b"v10".to_vec());
        assert_eq!(fdb_cp.get(b"k20").unwrap().unwrap(), b"v20".to_vec());
        assert_eq!(fdb_cp.get(b"k30").unwrap().unwrap(), b"v30".to_vec());
        assert_eq!(fdb_cp.get(b"k40").unwrap().unwrap(), b"v40".to_vec());

        // verify aux
        assert_eq!(fdb_cp.get_aux(b"k11").unwrap().unwrap(), b"v11".to_vec());
        assert_eq!(fdb_cp.get_aux(b"k21").unwrap().unwrap(), b"v21".to_vec());
        assert_eq!(fdb_cp.get_aux(b"k31").unwrap().unwrap(), b"v31".to_vec());

        // iterate data on range ["k10", "k40")
        let iter = fdb_cp.iter(b"k10", b"k40", IterOrder::Desc);
        let expected = vec![
            (b"k30".to_vec(), b"v30".to_vec()),
            (b"k20".to_vec(), b"v20".to_vec()),
            (b"k10".to_vec(), b"v10".to_vec()),
        ];
        let actual = iter
            .map(|(k, v)| {
                let kv = Tree::decode(k.to_vec(), &v);
                (kv.key().to_vec(), kv.value().to_vec())
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, actual);

        // iterate aux on range ["k11", "k31")
        let iter_aux = fdb_cp.iter_aux(b"k11", b"k31", IterOrder::Desc);
        let expected_aux = vec![
            (b"k21".to_vec(), b"v21".to_vec()),
            (b"k11".to_vec(), b"v11".to_vec()),
        ];
        let actual_aux = iter_aux
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(expected_aux, actual_aux);
    }
}
