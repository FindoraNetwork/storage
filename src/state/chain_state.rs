/// ChainState is a storage for latest blockchain state data
///
/// This Structure will be the main interface to the persistence layer provided by MerkleDB
/// and RocksDB backend.
///
use crate::db::{IterOrder, KVBatch, KValue, MerkleDB};
use crate::store::Prefix;
use merk::tree::{Tree, NULL_HASH};
use ruc::*;
use std::str;

const HEIGHT_KEY: &[u8; 6] = b"Height";
const SPLIT_BGN: &str = "_";

/// Concrete ChainState struct containing a reference to an instance of MerkleDB, a name and
/// current tree height.
pub struct ChainState<D>
where
    D: MerkleDB,
{
    name: String,
    ver_window: u64,
    db: D,
}

/// Implementation of of the concrete ChainState struct
impl<D> ChainState<D>
where
    D: MerkleDB,
{
    /// Creates a new instance of the ChainState.
    ///
    /// A default name is used if not provided and a reference to a struct implementing the
    /// MerkleDB trait is assigned.
    ///
    /// Returns the implicit struct
    pub fn new(db: D, name: String, ver_window: u64) -> Self {
        let mut db_name = "chain-state";
        if !name.is_empty() {
            db_name = name.as_str();
        }

        ChainState {
            name: db_name.to_string(),
            ver_window,
            db,
        }
    }

    /// Gets a value for the given key from the primary data section in RocksDB
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key)
    }

    /// Gets a value for the given key from the auxiliary data section in RocksDB.
    ///
    /// This section of data is not used for root hash calculations.
    pub fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_aux(key)
    }

    /// Iterates MerkleDB for a given range of keys.
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

            let kv = Tree::decode(kv_pair.0.to_vec(), &kv_pair.1);
            let key = kv.key();
            let value = kv.value();

            let entry: KValue = (key.to_vec(), value.to_vec());
            stop = func(entry);
        }
        true
    }

    /// Iterates MerkleDB allocated in auxiliary section for a given range of keys.
    ///
    /// Executes a closure passed as a parameter with the corresponding key value pairs.
    pub fn iterate_aux(
        &self,
        lower: &[u8],
        upper: &[u8],
        order: IterOrder,
        func: &mut dyn FnMut(KValue) -> bool,
    ) -> bool {
        // Get DB iterator
        let mut db_iter = self.db.iter_aux(lower, upper, order);
        let mut stop = false;

        // Loop through each entry in range
        while !stop {
            let kv_pair = match db_iter.next() {
                Some(result) => result,
                None => break,
            };

            //AUX data doesn't need to be decoded
            let key = kv_pair.0;
            let value = kv_pair.1;

            let entry: KValue = (key.to_vec(), value.to_vec());
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

    /// Queries the Aux DB for existence of a key.
    ///
    /// Returns a bool wrapped in a result as the query involves DB access.
    pub fn exists_aux(&self, key: &[u8]) -> Result<bool> {
        match self.get_aux(key).c(d!())? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// Deletes the auxiliary keys stored with a prefix < ( height - ver_window ),
    /// if the ver_window == 0 then the function returns without deleting any keys.
    ///
    /// The main purpose is to save memory on the disk
    fn prune_aux_batch(&self, height: u64, batch: &mut KVBatch) -> Result<()> {
        if self.ver_window == 0 || height < self.ver_window + 1 {
            return Ok(());
        }
        //Build range keys for window limits
        let window_start_height = (height - self.ver_window).to_string();
        let pruning_height = (height - self.ver_window - 1).to_string();

        let new_window_limit = Prefix::new(window_start_height.as_bytes());
        let old_window_limit = Prefix::new(pruning_height.as_bytes());

        //Range all auxiliary keys at pruning height
        self.iterate_aux(
            &old_window_limit.begin(),
            &old_window_limit.end(),
            IterOrder::Asc,
            &mut |(k, v)| -> bool {
                let key: Vec<_> = str::from_utf8(&k)
                    .unwrap_or("")
                    .split(SPLIT_BGN)
                    .collect();
                if key.len() < 2 {
                    return false;
                }
                println!("KEY: {:?}", key);
                //If the key doesn't already exist in the window start height, need to add it
                if !self
                    .exists_aux(new_window_limit.push(key[1].as_bytes()).as_ref())
                    .unwrap_or(false)
                {
                    // Add the key to new window limit height
                    batch.push((
                        new_window_limit
                            .clone()
                            .push(key[1].as_ref())
                            .as_ref()
                            .to_vec(),
                        Some(v),
                    ));
                }
                //Delete the key from the batch
                batch.push((
                    old_window_limit
                        .clone()
                        .push(key[1].as_ref())
                        .as_ref()
                        .to_vec(),
                    None,
                ));
                false
            },
        );
        Ok(())
    }

    /// Builds a new batch which is a copy of the original commit with the current height
    /// prefixed to each key.
    ///
    /// This is to keep a versioned history of KV pairs.
    fn build_aux_batch(&self, height: u64, batch: &mut KVBatch) -> Result<KVBatch> {
        let height_str = height.to_string();
        let prefix = Prefix::new(height_str.as_bytes());

        // Copy keys from batch to aux batch while prefixing them with the current height
        let mut aux_batch: KVBatch = batch
            .iter()
            .map(|(k, v)| {
                (
                    prefix.clone().push(k.clone().as_slice()).as_ref().to_vec(),
                    v.clone(),
                )
            })
            .collect();

        // Store the current height in auxiliary batch
        aux_batch.push((HEIGHT_KEY.to_vec(), Some(height_str.as_bytes().to_vec())));

        // Prune Aux data in the db
        self.prune_aux_batch(height, &mut aux_batch)?;

        Ok(aux_batch)
    }

    /// Commits a key value batch to the MerkleDB.
    ///
    /// The current height is updated in the ChainState as well as in the auxiliary data of the DB.
    /// An optional flag is also passed to indicate whether RocksDB should flush its mem table
    /// to disk.
    ///
    /// Due to the requirements of MerkleDB, the batch needs to be sorted prior to a commit.
    ///
    /// Returns the current height as well as the updated root hash of the Merkle Tree.
    pub fn commit(
        &mut self,
        mut batch: KVBatch,
        height: u64,
        flush: bool,
    ) -> Result<(Vec<u8>, u64)> {
        batch.sort();
        let aux = self.build_aux_batch(height, &mut batch).c(d!())?;

        self.db.put_batch(batch).c(d!())?;
        self.db.commit(aux, flush).c(d!())?;

        Ok((self.root_hash(), height))
    }

    /// Calculate and returns current root hash of the Merkle tree
    pub fn root_hash(&self) -> Vec<u8> {
        let hash = self.db.root_hash();
        if hash == NULL_HASH {
            return vec![];
        }
        hash
    }

    /// Returns current height of the ChainState
    pub fn height(&self) -> Result<u64> {
        let height = self.db.get_aux(HEIGHT_KEY).c(d!())?;
        if let Some(value) = height {
            let height_str = String::from_utf8(value).c(d!())?;
            let last_height = height_str.parse::<u64>().c(d!())?;

            return Ok(last_height);
        }
        Ok(0u64)
    }

    /// Returns the Name of the ChainState
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// This function will prune the tree of spent transaction outputs to reduce memory usage
    pub fn prune_tree() {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{IterOrder, KVBatch, KValue, MerkleDB, TempFinDB};
    use crate::state::chain_state;
    use std::thread;
    const VER_WINDOW: u64 = 100;

    #[test]
    fn test_new_chain_state() {
        //Create new database
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path).expect("failed to open db");

        //Create new Chain State with new database
        let _cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);
    }

    #[test]
    fn test_get() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ])
        .unwrap();

        // commit data without flushing to disk
        fdb.commit(vec![(b"height".to_vec(), Some(b"25".to_vec()))], false)
            .unwrap();

        //Create new Chain State with new database
        let cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);

        assert_eq!(cs.get(&b"k10".to_vec()).unwrap(), Some(b"v10".to_vec()));
        assert_eq!(cs.get(&b"k20".to_vec()).unwrap(), Some(b"v20".to_vec()));
        assert_eq!(cs.get(&b"kN/A".to_vec()).unwrap(), None);
        assert_eq!(
            cs.get_aux(&b"height".to_vec()).unwrap(),
            Some(b"25".to_vec())
        );
    }

    #[test]
    fn test_iterate() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

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

        // put data
        fdb.put_batch(batch).unwrap();

        // commit data without flushing to disk
        fdb.commit(vec![(b"height".to_vec(), Some(b"26".to_vec()))], false)
            .unwrap();

        //Create new Chain State with new database
        let cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);
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
            &b"k80".to_vec(),
            IterOrder::Asc,
            &mut func_iter,
        );
    }

    #[test]
    fn test_aux_iterator() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

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

        // put data
        fdb.put_batch(vec![]).unwrap();

        // commit data without flushing to disk, the batch is passed to the aux data here
        fdb.commit(batch, false).unwrap();

        //Create new Chain State with new database
        let cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);
        let mut func_iter = |entry: KValue| {
            println!("Key: {:?}, Value: {:?}", entry.0, entry.1);
            //Assert Keys are equal
            assert_eq!(entry.0, batch_clone[index].0);
            //Assert Values are equal
            assert_eq!(entry.1, batch_clone[index].1.clone().unwrap());

            index += 1;
            false
        };
        cs.iterate_aux(
            &b"k10".to_vec(),
            &b"k80".to_vec(),
            IterOrder::Asc,
            &mut func_iter,
        );
    }

    #[test]
    fn test_exists() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ])
        .unwrap();

        // commit data without flushing to disk
        fdb.commit(vec![(b"height".to_vec(), Some(b"26".to_vec()))], false)
            .unwrap();

        //Create new Chain State with new database
        let cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);

        assert_eq!(cs.exists(&b"k10".to_vec()).unwrap(), true);
        assert_eq!(cs.exists(&b"k20".to_vec()).unwrap(), true);
        assert_eq!(cs.exists(&b"kN/A".to_vec()).unwrap(), false);
    }

    #[test]
    fn test_commit() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path).expect("failed to open db");

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

        //Create new Chain State with new database
        let mut cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);

        // Commit batch to db, in production the flush would be true
        let result = cs.commit(batch, 55, false).unwrap();
        assert_eq!(result.1, 55);

        let mut func_iter = |entry: KValue| {
            //Assert Keys are equal
            assert_eq!(entry.0, batch_clone[index].0);
            //Assert Values are equal
            assert_eq!(entry.1, batch_clone[index].1.clone().unwrap());

            index += 1;
            false
        };
        cs.iterate(
            &b"k10".to_vec(),
            &b"k80".to_vec(),
            IterOrder::Asc,
            &mut func_iter,
        );
    }

    #[test]
    fn test_aux_commit() {
        let path = thread::current().name().unwrap().to_owned();
        let mut fdb = TempFinDB::open(path).expect("failed to open db");

        // put data
        fdb.put_batch(vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ])
        .unwrap();

        // commit data without flushing to disk
        fdb.commit(vec![(b"height".to_vec(), Some(b"25".to_vec()))], false)
            .unwrap();

        let cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);
        let height_aux = cs.get_aux(&b"height".to_vec()).unwrap();
        let height = cs.get(&b"height".to_vec());

        //Make sure height was saved to auxiliary section of the db.
        assert_eq!(height_aux, Some(b"25".to_vec()));

        //Make sure the height isn't accessible from the main merkle tree.
        assert_ne!(height.unwrap(), Some(b"25".to_vec()));
    }

    #[test]
    fn test_root_hash() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path).expect("failed to open db");

        let batch = vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k70".to_vec(), Some(b"v70".to_vec())),
            (b"k80".to_vec(), Some(b"v80".to_vec())),
        ];

        //Create new Chain State with new database
        let mut cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);
        let (root_hash1, _) = cs.commit(batch, 32, false).unwrap();

        let batch2 = vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k70".to_vec(), Some(b"v20".to_vec())),
            (b"k80".to_vec(), Some(b"v30".to_vec())),
        ];

        let (root_hash2, _) = cs.commit(batch2, 33, false).unwrap();
        assert_ne!(root_hash1, root_hash2);

        let (root_hash3, _) = cs.commit(vec![], 34, false).unwrap();
        assert_eq!(root_hash2, root_hash3);
    }

    #[test]
    fn test_height() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path).expect("failed to open db");

        let batch = vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k70".to_vec(), Some(b"v70".to_vec())),
            (b"k80".to_vec(), Some(b"v80".to_vec())),
        ];

        //Create new Chain State with new database
        let mut cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);

        assert_eq!(cs.height().unwrap(), 0u64);

        let (_, _) = cs.commit(batch, 32, false).unwrap();
        assert_eq!(cs.height().unwrap(), 32);

        let batch = vec![(b"k10".to_vec(), Some(b"v60".to_vec()))];

        let (_, _) = cs.commit(batch, 33, false).unwrap();
        assert_eq!(cs.height().unwrap(), 33);

        let batch = vec![(b"k10".to_vec(), Some(b"v100".to_vec()))];

        let (_, _) = cs.commit(batch, 34, false).unwrap();
        assert_eq!(cs.height().unwrap(), 34);
    }

    #[test]
    fn test_build_aux_batch() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path).expect("failed to open db");
        let number_of_batches = 21;
        let batch_size = 7;
        //Create new Chain State with new database
        let mut cs = chain_state::ChainState::new(fdb, "test_db".to_string(), 10);

        //Create Several batches (More than Window size) with different keys and values
        for i in 1..number_of_batches {
            let mut batch: KVBatch = KVBatch::new();
            for j in 0..batch_size {
                let key = format!("key-{}", j);
                let val = format!("val-{}", i);
                batch.push((Vec::from(key), Some(Vec::from(val))));
            }

            //Commit the new batch
            let _ = cs.commit(batch, i as u64, false);

            //After each commit verify the values by using get_aux
            for k in 0..batch_size {
                let key = format!("{}_key-{}", i, k);
                let value = format!("val-{}", i);
                assert_eq!(
                    cs.get_aux(key.as_bytes()).unwrap().unwrap().as_slice(),
                    value.as_bytes()
                )
            }
        }
    }

    #[test]
    fn test_prune_aux_batch() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path).expect("failed to open db");
        let number_of_batches = 21;
        let batch_size = 7;
        //Create new Chain State with new database
        let mut cs = chain_state::ChainState::new(fdb, "test_db".to_string(), 10);

        //Create Several batches (More than Window size) with different keys and values
        for i in 1..number_of_batches {
            let mut batch: KVBatch = KVBatch::new();
            for j in 0..batch_size {
                let key = format!("key-{}", j);
                let val = format!("val-{}", i);
                batch.push((Vec::from(key), Some(Vec::from(val))));
            }

            //Add a KV to the batch at a random height, 5 in this case
            if i == 5 {
                batch.push((b"random-key".to_vec(), Some(b"random-value".to_vec())));
            }

            //Commit the new batch
            let _ = cs.commit(batch, i as u64, false);

            //After each commit verify the values by using get_aux
            for k in 0..batch_size {
                let key = format!("{}_key-{}", i, k);
                let value = format!("val-{}", i);
                assert_eq!(
                    cs.get_aux(key.as_bytes()).unwrap().unwrap().as_slice(),
                    value.as_bytes()
                )
            }
        }

        //Make sure random key is found within the current window.
        //This will be current height - window size = 10 in this case.
        assert_eq!(
            cs.get_aux(b"10_random-key").unwrap(),
            Some(b"random-value".to_vec())
        );

        //Query aux values that are older than the window size to confirm batches were pruned
        for i in 1..10 {
            for k in 0..batch_size {
                let key = format!("{}_key-{}", i, k);
                assert_eq!(cs.get_aux(key.as_bytes()).unwrap(), None)
            }
        }
    }
}
