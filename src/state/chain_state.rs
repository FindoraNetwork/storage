/// ChainState is a storage for latest blockchain state data
///
/// This Structure will be the main interface to the persistence layer provided by MerkleDB
/// and RocksDB backend.
///
use crate::db::{IterOrder, KVBatch, KValue, MerkleDB};
use crate::state::cache::KVMap;
use crate::store::Prefix;
use merk::tree::{Tree, NULL_HASH};
use ruc::*;
use std::str;

const HEIGHT_KEY: &[u8; 6] = b"Height";
const SPLIT_BGN: &str = "_";
const TOMBSTONE: [u8; 1] = [206u8];

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
        let mut db_name = String::from("chain-state");
        if !name.is_empty() {
            db_name = name;
        }

        let mut cs = ChainState {
            name: db_name,
            ver_window,
            db,
        };
        cs.clean_aux_db();

        cs
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
        let window_start_height = Self::height_str(height - self.ver_window);
        let pruning_height = Self::height_str(height - self.ver_window - 1);

        let new_window_limit = Prefix::new("VER".as_bytes()).push(window_start_height.as_bytes());
        let old_window_limit = Prefix::new("VER".as_bytes()).push(pruning_height.as_bytes());

        //Range all auxiliary keys at pruning height
        self.iterate_aux(
            &old_window_limit.begin(),
            &old_window_limit.end(),
            IterOrder::Asc,
            &mut |(k, v)| -> bool {
                let raw_key = Self::get_raw_versioned_key(&k).unwrap_or_default();
                if raw_key.is_empty() {
                    return false;
                }
                //If the key doesn't already exist in the window start height, need to add it
                if !self
                    .exists_aux(new_window_limit.push(raw_key.as_bytes()).as_ref())
                    .unwrap_or(false)
                {
                    // Add the key to new window limit height
                    batch.push((
                        new_window_limit
                            .clone()
                            .push(raw_key.as_ref())
                            .as_ref()
                            .to_vec(),
                        Some(v),
                    ));
                }
                //Delete the key from the batch
                batch.push((
                    old_window_limit
                        .clone()
                        .push(raw_key.as_ref())
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
        let mut aux_batch = KVBatch::new();
        if self.ver_window != 0 {
            // Copy keys from batch to aux batch while prefixing them with the current height
            aux_batch = batch
                .iter()
                .map(|(k, v)| {
                    (
                        Self::versioned_key(k, height),
                        v.clone().map_or(Some(TOMBSTONE.to_vec()), Some),
                    )
                })
                .collect();

            // Prune Aux data in the db
            self.prune_aux_batch(height, &mut aux_batch)?;
        }

        // Store the current height in auxiliary batch
        aux_batch.push((HEIGHT_KEY.to_vec(), Some(height.to_string().into_bytes())));

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

    /// Build key Prefixed with Version height for Auxiliary data
    pub fn versioned_key(key: &[u8], height: u64) -> Vec<u8> {
        Prefix::new("VER".as_bytes())
            .push(Self::height_str(height).as_bytes())
            .push(key)
            .as_ref()
            .to_vec()
    }

    /// Build a height string for versioning history
    fn height_str(height: u64) -> String {
        format!("{:020}", height)
    }

    /// Deconstruct versioned key and return parsed raw key
    fn get_raw_versioned_key(key: &[u8]) -> Result<String> {
        let key: Vec<_> = str::from_utf8(key)
            .c(d!("key parse error"))?
            .split(SPLIT_BGN)
            .collect();
        if key.len() < 3 {
            return Err(eg!("invalid key pattern"));
        }
        Ok(key[2..].join(SPLIT_BGN))
    }

    /// Returns the Name of the ChainState
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// This function will prune the tree of spent transaction outputs to reduce memory usage
    pub fn prune_tree() {
        unimplemented!()
    }

    /// Build the chain-state from height 1 to height H
    ///
    /// Returns a batch with KV pairs valid at height H
    pub fn build_state(&self, height: u64) -> KVBatch {
        //New map to store KV pairs
        let mut map = KVMap::new();

        let lower = Prefix::new("VER".as_bytes());
        let upper = Prefix::new("VER".as_bytes()).push(Self::height_str(height + 1).as_bytes());

        self.iterate_aux(
            lower.begin().as_ref(),
            upper.as_ref(),
            IterOrder::Asc,
            &mut |(k, v)| -> bool {
                let raw_key = Self::get_raw_versioned_key(&k).unwrap_or_default();
                if raw_key.is_empty() {
                    return false;
                }
                //If value was deleted in the version history, delete it in the map
                if v.eq(TOMBSTONE.to_vec().as_slice()) {
                    map.remove(raw_key.as_bytes());
                } else {
                    //update map with current KV
                    map.insert(raw_key.as_bytes().to_vec(), Some(v));
                }
                false
            },
        );

        let kvs: Vec<_> = map
            .iter()
            .map(|(k, v)| (Self::versioned_key(k, height), v.clone()))
            .collect();
        kvs
    }

    /// Get the value of a key at a given height
    ///
    /// Returns the value of the given key at a particular height
    /// Returns None if the key was deleted or invalid at height H
    pub fn get_ver(&self, key: &[u8], height: u64) -> Result<Option<Vec<u8>>> {
        //Iterate in descending order from upper bound until a value is found
        let mut result = None;
        for h in (1..height + 1).rev() {
            let key = Self::versioned_key(key, h);
            if let Some(val) = self.get_aux(&key).c(d!("error reading aux value"))? {
                if val.eq(&TOMBSTONE) {
                    break;
                }
                result = Some(val);
            }
        }
        Ok(result)
    }

    /// When creating a new chain-state instance, any residual aux data outside the current window
    /// needs to be cleared as to not waste memory or disrupt the versioning behaviour.
    fn clean_aux_db(&mut self) {
        //Get current height
        let current_height = self.height().unwrap_or(0);
        if current_height == 0 {
            return;
        }
        if current_height < self.ver_window + 1 {
            return;
        }

        //Get batch for state at H = current_height - ver_window
        let batch = self.build_state(current_height - self.ver_window);
        //Commit this batch at base height H
        if self.db.commit(batch, true).is_err() {
            println!("error building base chain state");
            return;
        }

        //Define upper and lower bounds for iteration
        let lower = Prefix::new("VER".as_bytes());
        let upper = Prefix::new("VER".as_bytes())
            .push(Self::height_str(current_height - self.ver_window).as_bytes());

        //Create an empty batch
        let mut batch = KVBatch::new();

        //Iterate aux data and delete keys within bounds
        self.iterate_aux(
            lower.begin().as_ref(),
            upper.as_ref(),
            IterOrder::Asc,
            &mut |(k, _v)| -> bool {
                //Delete the key from aux db
                batch.push((k, None));
                false
            },
        );

        //commit aux batch
        let _ = self.db.commit(batch, true);
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{FinDB, IterOrder, KVBatch, KValue, MerkleDB, TempFinDB};
    use crate::state::{chain_state, ChainState};
    use rand::Rng;
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
                let key =
                    ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
                let value = format!("val-{}", i);
                assert_eq!(
                    cs.get_aux(key.as_slice()).unwrap().unwrap(),
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
                batch.push((b"random_key".to_vec(), Some(b"random-value".to_vec())));
            }

            //Commit the new batch
            let _ = cs.commit(batch, i as u64, false);

            //After each commit verify the values by using get_aux
            for k in 0..batch_size {
                let key =
                    ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
                let value = format!("val-{}", i);
                assert_eq!(
                    cs.get_aux(key.as_slice()).unwrap().unwrap().as_slice(),
                    value.as_bytes()
                )
            }
        }

        //Make sure random key is found within the current window.
        //This will be current height - window size = 10 in this case.
        assert_eq!(
            cs.get_aux(ChainState::<TempFinDB>::versioned_key(b"random_key", 10).as_slice())
                .unwrap(),
            Some(b"random-value".to_vec())
        );

        //Query aux values that are older than the window size to confirm batches were pruned
        for i in 1..10 {
            for k in 0..batch_size {
                let key =
                    ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
                assert_eq!(cs.get_aux(key.as_slice()).unwrap(), None,)
            }
        }
    }

    #[test]
    fn test_build_state() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path).expect("failed to open db");
        let mut cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);

        let mut rng = rand::thread_rng();
        let mut keys = Vec::with_capacity(10);
        for i in 0..10 {
            keys.push(format!("key_{}", i));
        }

        //Apply several batches with select few keys at random
        for h in 1..21 {
            let mut batch = KVBatch::new();

            //Build a random batch
            for _ in 0..5 {
                let rnd_key_idx = rng.gen_range(0..10);
                let rnd_val = format!("val-{}", rng.gen_range(0..10));
                batch.push((
                    keys[rnd_key_idx].clone().into_bytes(),
                    Some(rnd_val.into_bytes()),
                ));
            }

            let _ = cs.commit(batch, h, false);
        }

        //Confirm the build_state function produces the same keys and values as the latest state.
        let mut cs_batch = KVBatch::new();
        let bound = crate::store::Prefix::new("key".as_bytes());
        cs.iterate(
            &bound.begin(),
            &bound.end(),
            IterOrder::Asc,
            &mut |(k, v)| -> bool {
                //Delete the key from aux db
                cs_batch.push((k, Some(v)));
                false
            },
        );

        let built_batch: Vec<_> = cs
            .build_state(20)
            .iter()
            .map(|(k, v)| {
                (
                    ChainState::<TempFinDB>::get_raw_versioned_key(k)
                        .unwrap()
                        .into_bytes(),
                    v.clone(),
                )
            })
            .collect();

        assert!(cs_batch.eq(&built_batch))
    }

    #[test]
    fn test_clean_aux_db() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = FinDB::open(path.clone()).expect("failed to open db");
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
        }

        //Open db with new chain-state - window half the size of the previous
        //Simulate Node restart
        std::mem::drop(cs);
        let new_window_size = 5;
        let fdb_new = TempFinDB::open(path.clone()).expect("failed to open db");
        let cs_new = chain_state::ChainState::new(fdb_new, "test_db".to_string(), new_window_size);

        //Confirm keys older than new window size have been deleted
        for i in 1..(number_of_batches - new_window_size - 1) {
            for k in 0..batch_size {
                let key =
                    ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
                assert_eq!(cs_new.get_aux(key.as_slice()).unwrap(), None)
            }
        }

        //Confirm keys within new window size still exist
        for i in (number_of_batches - new_window_size)..number_of_batches {
            for k in 0..batch_size {
                let key =
                    ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
                assert!(cs_new.exists_aux(key.as_slice()).unwrap())
            }
        }
    }

    #[test]
    fn test_get_ver() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempFinDB::open(path.clone()).expect("failed to open db");
        //Create new Chain State with new database
        let mut cs = chain_state::ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);

        //Commit a single key at different heights and values
        for height in 1..21 {
            let mut batch = KVBatch::new();
            if height == 3 {
                batch.push((b"test_key".to_vec(), Some(b"test-val1".to_vec())));
            }
            if height == 7 {
                //Deleted key at height 7
                batch.push((b"test_key".to_vec(), None));
            }
            if height == 15 {
                batch.push((b"test_key".to_vec(), Some(b"test-val2".to_vec())));
            }

            let _ = cs.commit(batch, height, false);
        }

        //Query the key at each version it was updated
        assert_eq!(
            cs.get_ver(b"test_key", 3).unwrap(),
            Some(b"test-val1".to_vec())
        );
        assert_eq!(cs.get_ver(b"test_key", 7).unwrap(), None);
        assert_eq!(
            cs.get_ver(b"test_key", 15).unwrap(),
            Some(b"test-val2".to_vec())
        );

        //Query the key between update versions
        assert_eq!(
            cs.get_ver(b"test_key", 5).unwrap(),
            Some(b"test-val1".to_vec())
        );
        assert_eq!(
            cs.get_ver(b"test_key", 17).unwrap(),
            Some(b"test-val2".to_vec())
        );
        assert_eq!(cs.get_ver(b"test_key", 10).unwrap(), None);

        //Query the key at a version it didn't exist
        assert_eq!(cs.get_ver(b"test_key", 2).unwrap(), None);

        //Query the key after it's been deleted
        assert_eq!(cs.get_ver(b"test_key", 8).unwrap(), None);
    }
}
