/// Definition of RocksState structure containing the data defining the current state of the
/// blockchain. The struct wraps an interface to the persistence layer as well as a cache.
///
use crate::db::{IRocksDB, IterOrder, KVBatch, KValue};
use crate::state::{RocksChainState, SessionedCache};
use parking_lot::RwLock;
use ruc::*;
use std::sync::Arc;

/// RocksState Definition used by all stores
///
/// Contains a Reference to the RocksChainState and a Session Cache used for collecting batch data
/// and transaction simulation.
pub struct RocksState<D: IRocksDB> {
    chain_state: Arc<RwLock<RocksChainState<D>>>,
    cache: SessionedCache,
}

/// Implementation of concrete RocksState struct
impl<D: IRocksDB> RocksState<D> {
    /// Creates a RocksState with a new cache and reference to the RocksChainState
    pub fn new(cs: Arc<RwLock<RocksChainState<D>>>) -> Self {
        RocksState {
            chain_state: cs,
            cache: SessionedCache::new(),
        }
    }

    /// Returns the chain state of the store.
    pub fn chain_state(&self) -> Arc<RwLock<RocksChainState<D>>> {
        self.chain_state.clone()
    }

    /// Gets a value for the given key.
    ///
    /// First checks the cache for the latest value for that key.
    /// Returns the value if found, otherwise queries the chainState.
    ///
    /// Can either return None or a Vec<u8> as the value.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        //Check if value was deleted
        if self.cache.deleted(key) {
            return Ok(None);
        }
        //Check if key has a value
        if self.cache.hasv(key) {
            return Ok(self.cache.getv(key));
        }

        //If the key isn't found in the cache then query the chain state directly
        let cs = self.chain_state.read();
        cs.get(key)
    }

    /// Queries whether a key exists in the current state.
    ///
    /// First Checks the cache, returns true if found otherwise queries the chainState.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        //Check if the key exists in the cache otherwise check the chain state
        let val = self.cache.getv(key);
        if val.is_some() {
            return Ok(true);
        }
        let cs = self.chain_state.read();
        cs.exists(key)
    }

    /// Sets a key value pair in the cache
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) {
        self.cache.put(key, value);
    }

    /// Deletes a key from the RocksState.
    ///
    /// The deletion of a key is represented by setting the value to None for a given key.
    ///
    /// When attempting to delete a key that doesn't exist in the RocksChainState, the IRocksDB
    /// will panic.
    ///
    /// To avoid this case, the RocksChainState is first queried for the key. If the key is found,
    /// the deletion proceeds as usual. If it isn't found in the RocksChainState but exists in the
    /// cache, then the key value record will be removed from the cache.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let cs = self.chain_state.read();
        match cs.get(key).c(d!())? {
            //Mark key as deleted
            Some(_) => self.cache.delete(key),
            //Remove key from cache
            None => self.cache.remove(key),
        }
        Ok(())
    }

    /// Iterates the RocksChainState for the given range of keys
    pub fn iterate(
        &self,
        lower: &[u8],
        upper: &[u8],
        order: IterOrder,
        func: &mut dyn FnMut(KValue) -> bool,
    ) -> bool {
        let cs = self.chain_state.read();
        cs.iterate(lower, upper, order, func)
    }

    /// Commits the current state to the DB with the given height
    ///
    /// The cache gets persisted to the IRocksDB and then cleared
    pub fn commit(&mut self, height: u64) -> Result<u64> {
        let mut cs = self.chain_state.write();
        //Get batch for current block
        let kv_batch = self.cache.commit();
        //Clear the cache from the current state
        self.cache = SessionedCache::new();

        //Commit batch to db
        cs.commit(kv_batch, height)
    }

    /// Commits the cache of the current session.
    ///
    /// The Base cache gets updated with the current cache.
    pub fn commit_session(&mut self) -> KVBatch {
        self.cache.commit()
    }

    /// Discards the current session cache.
    ///
    /// The current cache is rebased.
    pub fn discard_session(&mut self) {
        self.cache.discard()
    }

    /// Returns whether or not a key has been modified in the current block
    pub fn touched(&self, key: &[u8]) -> bool {
        self.cache.touched(key)
    }

    /// Return the current height of the Merkle tree
    pub fn height(&self) -> Result<u64> {
        let cs = self.chain_state.read();
        cs.height()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{KValue, TempRocksDB};
    use std::thread;

    #[test]
    fn test_get() {
        //Setup
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempRocksDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(RocksChainState::new(
            fdb,
            "test_db".to_string(),
        )));
        let mut state = RocksState::new(cs.clone());

        //Set some kv pairs
        assert_eq!(state.height().unwrap(), 0);
        state.set(b"prefix_validator_1", b"v10".to_vec());
        state.set(b"prefix_delegator_1", b"v20".to_vec());

        //Get the values
        assert_eq!(
            state.get(b"prefix_validator_1").unwrap(),
            Some(b"v10".to_vec())
        );
        assert_eq!(
            state.get(b"prefix_delegator_1").unwrap(),
            Some(b"v20".to_vec())
        );
        assert_eq!(state.get(b"prefix_validator_2").unwrap(), None);

        //Commit and create new state - Simulate new block
        let _res = state.commit(89);
        state = RocksState::new(cs);

        //Should get this value from the chain state as the state cache is empty
        assert_eq!(
            state.get(b"prefix_validator_1").unwrap(),
            Some(b"v10".to_vec())
        );
        assert_eq!(state.height().unwrap(), 89);
    }

    #[test]
    fn test_exists() {
        //Setup
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempRocksDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(RocksChainState::new(
            fdb,
            "test_db".to_string(),
        )));
        let mut state = RocksState::new(cs);

        //Set some kv pairs
        state.set(b"prefix_validator_1", b"v10".to_vec());
        state.set(b"prefix_delegator_1", b"v20".to_vec());

        //Get the values
        assert_eq!(state.exists(b"prefix_validator_1").unwrap(), true);
        assert_eq!(state.exists(b"prefix_delegator_1").unwrap(), true);
        assert_eq!(state.exists(b"prefix_validator_2").unwrap(), false);

        //Commit and create new state - Simulate new block
        let _res = state.commit(89);

        //Should get this value from the chain state as the state cache is empty
        assert_eq!(state.exists(b"prefix_validator_1").unwrap(), true);
        assert_eq!(state.height().unwrap(), 89);
    }

    #[test]
    fn test_set() {
        //Setup
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempRocksDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(RocksChainState::new(
            fdb,
            "test_db".to_string(),
        )));
        let mut state = RocksState::new(cs);

        //Set some kv pairs
        state.set(b"prefix_validator_1", b"v10".to_vec());
        state.set(b"prefix_delegator_1", b"v20".to_vec());

        //Get the values
        assert_eq!(
            state.get(b"prefix_validator_1").unwrap(),
            Some(b"v10".to_vec())
        );
        assert_eq!(
            state.get(b"prefix_delegator_1").unwrap(),
            Some(b"v20".to_vec())
        );
        assert_eq!(state.get(b"prefix_validator_2").unwrap(), None);
    }

    #[test]
    fn test_delete() {
        //Setup
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempRocksDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(RocksChainState::new(
            fdb,
            "test_db".to_string(),
        )));
        let mut state = RocksState::new(cs);

        //Set some kv pairs
        state.set(b"prefix_validator_1", b"v10".to_vec());
        state.set(b"prefix_validator_2", b"v20".to_vec());
        state.set(b"prefix_validator_3", b"v30".to_vec());

        //Get the values
        assert_eq!(
            state.get(b"prefix_validator_1").unwrap(),
            Some(b"v10".to_vec())
        );
        assert_eq!(
            state.get(b"prefix_validator_2").unwrap(),
            Some(b"v20".to_vec())
        );
        assert_eq!(
            state.get(b"prefix_validator_3").unwrap(),
            Some(b"v30".to_vec())
        );

        // ----------- Commit and clear cache - Simulate new block -----------
        let _res = state.commit(89);

        //Should get this value from the chain state as the state cache is empty
        assert_eq!(
            state.get(b"prefix_validator_1").unwrap(),
            Some(b"v10".to_vec())
        );
        assert_eq!(state.height().unwrap(), 89);

        state.set(b"prefix_validator_4", b"v40".to_vec());
        let _res = state.delete(b"prefix_validator_4");

        println!(
            "test_delete Batch after delete: {:?}",
            state.commit_session()
        );

        //Delete key from chain state
        let _res2 = state.delete(b"prefix_validator_3");

        // ----------- Commit and clear cache - Simulate new block -----------
        let _res1 = state.commit(90);

        //Should get this value from the chain state as the state cache is empty
        assert_eq!(
            state.get(b"prefix_validator_1").unwrap(),
            Some(b"v10".to_vec())
        );
        assert_eq!(
            state.get(b"prefix_validator_2").unwrap(),
            Some(b"v20".to_vec())
        );
        assert_eq!(state.get(b"prefix_validator_3").unwrap(), None);
        assert_eq!(state.height().unwrap(), 90);
    }

    #[test]
    fn test_get_deleted() {
        //Setup
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempRocksDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(RocksChainState::new(
            fdb,
            "test_db".to_string(),
        )));
        let mut state = RocksState::new(cs);

        //Set some kv pairs
        state.set(b"prefix_validator_1", b"v10".to_vec());
        state.set(b"prefix_validator_2", b"v20".to_vec());
        state.set(b"prefix_validator_3", b"v30".to_vec());

        let _res = state.commit(89);

        //Should detect the key as deleted from the cache and return None without querying db
        let _res2 = state.delete(b"prefix_validator_3");
        assert_eq!(state.get(b"prefix_validator_3").unwrap(), None);
    }

    #[test]
    fn test_commit() {
        //Setup
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempRocksDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(RocksChainState::new(
            fdb,
            "test_db".to_string(),
        )));
        let mut state = RocksState::new(cs);

        //Set some kv pairs
        state.set(b"prefix_validator_1", b"v10".to_vec());
        state.set(b"prefix_validator_2", b"v10".to_vec());

        //Commit state to db
        let height1 = state.commit(90).unwrap();

        //Modify a value in the db
        state.set(b"prefix_validator_2", b"v20".to_vec());
        assert_eq!(height1, 90);
        assert_eq!(state.height().unwrap(), 90);

        //Commit state to db
        let height2 = state.commit(91).unwrap();
        assert_eq!(height2, 91);
        assert_eq!(state.height().unwrap(), 91);

        //Commit state to db
        let height3 = state.commit(92).unwrap();
        assert_eq!(height3, 92);
        assert_eq!(state.height().unwrap(), 92);
    }

    #[test]
    fn test_iterate() {
        let path = thread::current().name().unwrap().to_owned();
        let fdb = TempRocksDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(RocksChainState::new(
            fdb,
            "test_db".to_string(),
        )));
        let mut state = RocksState::new(cs);

        let mut count = 0;

        state.set(b"prefix_validator_1", b"v10".to_vec());
        state.set(b"prefix_validator_2", b"v10".to_vec());
        state.set(b"prefix_3", b"v10".to_vec());
        state.set(b"prefix_4", b"v10".to_vec());
        state.set(b"prefix_validator_5", b"v10".to_vec());
        state.set(b"prefix_validator_6", b"v10".to_vec());
        state.set(b"prefix_7", b"v10".to_vec());
        state.set(b"prefix_validator_8", b"v10".to_vec());

        // ----------- Commit state to db and clear cache -----------
        let h1 = state.commit(55).unwrap();
        assert_eq!(h1, 55);
        assert_eq!(state.height().unwrap(), 55);

        let mut func_iter = |entry: KValue| {
            println!("Key: {:?}, Value: {:?}", entry.0, entry.1);
            count += 1;
            false
        };
        state.iterate(
            &b"prefix_validator_".to_vec(),
            &b"prefix_validator~".to_vec(),
            IterOrder::Asc,
            &mut func_iter,
        );
        assert_eq!(count, 5);
    }
}
