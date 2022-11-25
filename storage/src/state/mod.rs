/// Definition of State structure containing the data defining the current state of the
/// blockchain. The struct wraps an interface to the persistence layer as well as a cache.
///
pub mod cache;
pub mod chain_state;

use crate::db::{IterOrder, KValue, MerkleDB};
pub use cache::{KVMap, KVecMap, SessionedCache};
pub use chain_state::{ChainState, ChainStateOpts};
use parking_lot::RwLock;
use ruc::*;
use std::sync::Arc;

/// State Definition used by all stores
///
/// Contains a Reference to the ChainState and a Session Cache used for collecting batch data
/// and transaction simulation.
pub struct State<D: MerkleDB> {
    chain_state: Arc<RwLock<ChainState<D>>>,
    cache: SessionedCache,
    height_cap: Option<u64>,
}

impl<D: MerkleDB> Drop for State<D> {
    fn drop(&mut self) {
        if let Some(height) = self.height_cap {
            self.chain_state.write().unpin_at(height);
            self.height_cap = None;
        }
    }
}

impl<D: MerkleDB> State<D> {
    pub fn cache_mut(&mut self) -> &mut SessionedCache {
        &mut self.cache
    }

    pub fn substate(&self) -> Self {
        Self {
            chain_state: self.chain_state.clone(),
            cache: self.cache.clone(),
            height_cap: None,
        }
    }

    pub fn stack_push(&mut self) {
        self.cache.stack_push();
    }

    pub fn stack_commit(&mut self) {
        self.cache.stack_commit();
    }

    pub fn stack_discard(&mut self) {
        self.cache.stack_discard();
    }

    /// Creates a State with a new cache and shared ChainState
    pub fn new(cs: Arc<RwLock<ChainState<D>>>, is_merkle: bool) -> Self {
        State {
            // lock whole State object for now
            chain_state: cs,
            cache: SessionedCache::new(is_merkle),
            height_cap: None,
        }
    }

    /// Creates a State with a same cache and shared ChainState
    pub fn copy(&self) -> Self {
        State {
            chain_state: self.chain_state.clone(),
            cache: self.cache.clone(),
            height_cap: None,
        }
    }

    /// Creates a State at specific height
    pub fn state_at(&self, height: u64) -> Result<Self> {
        self.chain_state.write().pin_at(height)?;
        Ok(State {
            chain_state: self.chain_state.clone(),
            cache: SessionedCache::new(self.cache.is_merkle()),
            height_cap: Some(height),
        })
    }

    /// Returns the chain state of the store.
    pub fn chain_state(&self) -> Arc<RwLock<ChainState<D>>> {
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
        match self.height_cap {
            Some(height) => cs.get_ver(key, height),
            None => cs.get(key),
        }
    }

    pub fn get_ver(&self, key: &[u8], height: u64) -> Result<Option<Vec<u8>>> {
        let query_at = match self.height_cap {
            Some(cap) if cap < height => cap,
            _ => height,
        };
        self.chain_state.read().get_ver(key, query_at)
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
        match self.height_cap {
            Some(height) => cs.get_ver(key, height).map(|v| v.is_some()),
            None => cs.exists(key),
        }
    }

    /// Sets a key value pair in the cache
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        if self.cache.put(key, value) {
            Ok(())
        } else {
            Err(eg!("Invalid key-value pair detected."))
        }
    }

    /// Deletes a key from the State.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.cache.delete(key);
        Ok(())
    }

    // Deprecated and replaced by `delete`
    pub fn delete_v0(&mut self, key: &[u8]) -> Result<()> {
        let cs = self.chain_state.read();
        match cs.get(key).c(d!())? {
            //Mark key as deleted
            Some(_) => self.cache.delete(key),
            //Remove key from cache
            None => self.cache.remove(key),
        }
        Ok(())
    }

    /// Iterates the ChainState for the given range of keys
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

    /// Iterates the cache for a given prefix
    pub(crate) fn iterate_cache(&self, prefix: &[u8], map: &mut KVecMap) {
        self.cache.iter_prefix(prefix, map);
    }

    /// Commits the current state to the DB with the given height
    ///
    /// The cache gets persisted to the MerkleDB and then cleared
    pub fn commit(&mut self, height: u64) -> Result<(Vec<u8>, u64)> {
        if self.height_cap.is_some() {
            return Err(eg!("Not support commit a state with height cap"));
        }
        let mut cs = self.chain_state.write();

        //Get batch for current block and remove uncessary DELETE.
        //Note: DB will panic if it doesn't contain the key being deleted.
        let mut kv_batch = self.cache.commit();
        kv_batch.retain(|(k, v)| match cs.exists(k).unwrap() {
            true => true,
            false => v.is_some(),
        });

        //Clear the cache from the current state
        self.cache = SessionedCache::new(self.cache.is_merkle());

        //Commit batch to db
        cs.commit(kv_batch, height, true)
    }

    /// Commits the cache of the current session.
    ///
    /// The Base cache gets updated with the current cache.
    pub fn commit_session(&mut self) {
        self.cache.commit_only();
    }

    /// Discards the current session cache.
    ///
    /// The current cache is rebased.
    pub fn discard_session(&mut self) {
        self.cache.discard()
    }

    /// Export a copy of chain state on a specific height.
    ///
    /// * `cs` - The target chain state that holds the copy.
    /// * `height` - On which height the copy will be taken.
    ///
    pub fn export(&self, cs: &mut ChainState<D>, height: u64) -> Result<()> {
        self.chain_state.read().export(cs, height)
    }

    /// Returns whether or not a key has been modified in the current block
    pub fn touched(&self, key: &[u8]) -> bool {
        self.cache.touched(key)
    }

    /// Return the current height of the Merkle tree
    pub fn height(&self) -> Result<u64> {
        let cs = self.chain_state.read();
        let current = cs.height()?;
        Ok(match self.height_cap {
            Some(cap) if cap < current => cap,
            _ => current,
        })
    }

    /// Returns the root hash of the last commit
    pub fn root_hash(&self) -> Vec<u8> {
        if self.height_cap.is_some() {
            vec![]
        } else {
            let cs = self.chain_state.read();
            cs.root_hash()
        }
    }
}
