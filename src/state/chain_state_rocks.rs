use crate::db::{IRocksDB, IterOrder, KVBatch, KValue};
use ruc::*;

const HEIGHT_KEY: &[u8; 6] = b"Height";

/// RocksChainState is a wrapper of RocksDB
pub struct RocksChainState<D: IRocksDB> {
    name: String,
    db: D,
}

impl<D: IRocksDB> RocksChainState<D> {
    /// Creates a new instance of the RocksDB state.
    ///
    /// A default name is used if not provided and a reference to a struct implementing the
    /// IRocksDB trait is assigned.
    ///
    /// Returns the implicit struct
    pub fn new(db: D, name: String) -> Self {
        let mut db_name = "rocks-state";
        if !name.is_empty() {
            db_name = name.as_str();
        }

        Self {
            name: db_name.to_string(),
            db,
        }
    }

    /// Gets a value for the given key from the RocksDB state
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key)
    }

    /// Iterates RocksDB state for a given range of keys.
    ///
    /// Executes a closure passed as a parameter with the corresponding key value pairs.
    pub fn iterate(
        &self,
        lower: &[u8],
        upper: &[u8],
        order: IterOrder,
        func: &mut dyn FnMut(KValue) -> bool,
    ) -> bool {
        // Get DB iterator
        let mut db_iter = self.db.iter(lower, upper, order);
        let mut stop = false;

        // Loop through each entry in range
        while !stop {
            let kv_pair = match db_iter.next() {
                Some(result) => result,
                None => break,
            };
            let entry: KValue = (kv_pair.0.to_vec(), kv_pair.1.to_vec());
            stop = func(entry);
        }
        true
    }

    /// Queries the DB for existence of a key.
    ///
    /// Returns a bool wrapped in a result as the query involves DB access.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        match self.get(key).c(d!())? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// Commits a key value batch to the RocksDB.
    ///
    /// Returns the current height.
    pub fn commit(&mut self, mut batch: KVBatch, height: u64) -> Result<u64> {
        // append height value in batch
        let h_str = height.to_string();
        batch.push((HEIGHT_KEY.to_vec(), Some(h_str.as_bytes().to_vec())));

        // sort and commit
        batch.sort();
        self.db.commit(batch).c(d!())?;

        Ok(height)
    }

    /// Returns current height of the RocksChainState
    pub fn height(&self) -> Result<u64> {
        let height = self.db.get(HEIGHT_KEY).c(d!())?;
        if let Some(value) = height {
            let height_str = String::from_utf8(value).c(d!())?;
            let last_height = height_str.parse::<u64>().c(d!())?;

            return Ok(last_height);
        }
        Ok(0u64)
    }

    /// Returns the Name of the RocksChainState
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::RocksChainState;
    use crate::db::TempRocksDB;
    use crate::db::{IterOrder, KValue};
    use std::thread;

    #[test]
    fn test_open_rocks_state() {
        //Create new database
        let path = thread::current().name().unwrap().to_owned();
        let db = TempRocksDB::open(path).expect("failed to open db");

        //Create new RocksChainState with new database
        let _rs = RocksChainState::new(db, "test_db".to_string());
    }

    #[test]
    fn test_get() {
        // Create RocksDB state
        let path = thread::current().name().unwrap().to_owned();
        let db = TempRocksDB::open(path).expect("failed to open db");
        let mut cs = RocksChainState::new(db, "test_db".to_string());

        // commit data
        cs.commit(
            vec![
                (b"k10".to_vec(), Some(b"v10".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
            ],
            25,
        )
        .unwrap();

        // verify
        assert_eq!(cs.get(&b"k10".to_vec()).unwrap(), Some(b"v10".to_vec()));
        assert_eq!(cs.get(&b"k20".to_vec()).unwrap(), Some(b"v20".to_vec()));
        assert_eq!(cs.get(&b"kN/A".to_vec()).unwrap(), None);
        assert_eq!(cs.height().unwrap(), 25);
    }

    #[test]
    fn test_iterate() {
        let path = thread::current().name().unwrap().to_owned();
        let db = TempRocksDB::open(path).expect("failed to open db");
        let mut cs = RocksChainState::new(db, "test_db".to_string());

        let mut index = 0;
        let batch = vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
            (b"k40".to_vec(), Some(b"v40".to_vec())),
            (b"k50".to_vec(), Some(b"v50".to_vec())),
            (b"k60".to_vec(), Some(b"v60".to_vec())),
            (b"k70".to_vec(), Some(b"v70".to_vec())),
            (b"k80".to_vec(), Some(b"v80".to_vec())),
        ];

        let batch_clone = batch.clone();

        // commit data
        cs.commit(batch, 25).unwrap();

        // verify
        let mut func_iter = |entry: KValue| {
            println!("Key: {:?}, Value: {:?}", entry.0, entry.1);
            //Assert Keys are equal
            assert_eq!(entry.0, batch_clone[index].0);
            //Assert Values are equal
            assert_eq!(entry.1, batch_clone[index].1.clone().unwrap());

            index += 1;
            false
        };
        cs.iterate(
            &b"k10".to_vec(),
            &b"k81".to_vec(),
            IterOrder::Asc,
            &mut func_iter,
        );
        assert_eq!(cs.height().unwrap(), 25);
    }

    #[test]
    fn test_exists() {
        let path = thread::current().name().unwrap().to_owned();
        let db = TempRocksDB::open(path).expect("failed to open db");
        let mut cs = RocksChainState::new(db, "test_db".to_string());

        // commit data
        cs.commit(
            vec![
                (b"k10".to_vec(), Some(b"v10".to_vec())),
                (b"k20".to_vec(), Some(b"v20".to_vec())),
            ],
            26,
        )
        .unwrap();

        // verify
        assert_eq!(cs.exists(&b"k10".to_vec()).unwrap(), true);
        assert_eq!(cs.exists(&b"k20".to_vec()).unwrap(), true);
        assert_eq!(cs.exists(&b"kN/A".to_vec()).unwrap(), false);
        assert_eq!(cs.height().unwrap(), 26);
    }

    #[test]
    fn test_height() {
        let path = thread::current().name().unwrap().to_owned();
        let db = TempRocksDB::open(path).expect("failed to open db");
        let mut cs = RocksChainState::new(db, "test_db".to_string());

        let batch = vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k70".to_vec(), Some(b"v70".to_vec())),
            (b"k80".to_vec(), Some(b"v80".to_vec())),
        ];

        // Create new Chain State with new database
        assert_eq!(cs.height().unwrap(), 0u64);

        let h1 = cs.commit(batch, 32).unwrap();
        assert_eq!(h1, 32);
        assert_eq!(cs.height().unwrap(), 32);

        let batch = vec![(b"k10".to_vec(), Some(b"v60".to_vec()))];

        let h2 = cs.commit(batch, 33).unwrap();
        assert_eq!(h2, 33);
        assert_eq!(cs.height().unwrap(), 33);

        let batch = vec![(b"k10".to_vec(), Some(b"v100".to_vec()))];

        let h3 = cs.commit(batch, 34).unwrap();
        assert_eq!(h3, 34);
        assert_eq!(cs.height().unwrap(), 34);
    }
}
