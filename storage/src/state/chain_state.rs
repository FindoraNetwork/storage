/// ChainState is a storage for latest blockchain state data
///
/// This Structure will be the main interface to the persistence layer provided by MerkleDB
/// and RocksDB backend.
///
use crate::{
    db::{IterOrder, KVBatch, KVEntry, KValue, MerkleDB},
    state::cache::KVMap,
    store::Prefix,
};
use ruc::*;
use std::{ops::Range, path::Path, str};

const HEIGHT_KEY: &[u8; 6] = b"Height";
const AUX_VERSION: &[u8; 10] = b"AuxVersion";
const CUR_AUX_VERSION: u64 = 0x01;
const SPLIT_BGN: &str = "_";
const TOMBSTONE: [u8; 1] = [206u8];

/// The length of a `Hash` (in bytes). same with fmerk.
pub const HASH_LENGTH: usize = 32;

/// A zero-filled `Hash`. same with fmerk.
pub const NULL_HASH: [u8; HASH_LENGTH] = [0; HASH_LENGTH];

/// Concrete ChainState struct containing a reference to an instance of MerkleDB, a name and
/// current tree height.
pub struct ChainState<D: MerkleDB> {
    name: String,
    ver_window: u64,
    version: u64,
    db: D,
}

/// Implementation of of the concrete ChainState struct
impl<D: MerkleDB> ChainState<D> {
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
            version: Default::default(),
            db,
        };

        cs.version = cs.get_aux_version().expect("Need a valid version");

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

    /// Get aux database version
    ///
    /// The default version is ox00
    pub fn get_aux_version(&self) -> Result<u64> {
        if let Some(version) = self.get_aux(AUX_VERSION.to_vec().as_ref())? {
            let ver_str = String::from_utf8(version).c(d!("Invalid aux version string"))?;
            return ver_str
                .parse::<u64>()
                .c(d!("aux version should be a valid 64-bit long integer"));
        }

        Ok(0x00)
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

            let entry = self.db.decode_kv(kv_pair);
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
        let pruning_height = Self::height_str(height - self.ver_window - 1);
        let pruning_prefix = Prefix::new("VER".as_bytes()).push(pruning_height.as_bytes());

        // move key-value pairs of left window side to baseline
        self.iterate_aux(
            &pruning_prefix.begin(),
            &pruning_prefix.end(),
            IterOrder::Asc,
            &mut |(k, v)| -> bool {
                let raw_key = Self::get_raw_versioned_key(&k).unwrap_or_default();
                if raw_key.is_empty() {
                    return false;
                }
                // Merge(update/remove) to baseline
                let base_key = Self::base_key(raw_key.as_bytes());
                if v.ne(&TOMBSTONE) {
                    batch.push((base_key, Some(v)));
                } else if self.exists_aux(&base_key).unwrap_or(false) {
                    batch.push((base_key, None));
                }
                //Delete the key from the batch
                batch.push((k, None));
                false
            },
        );

        Ok(())
    }

    /// Builds a new batch which is a copy of the original commit with the current height
    /// prefixed to each key.
    ///
    /// This is to keep a versioned history of KV pairs.
    fn build_aux_batch(&self, height: u64, batch: &[KVEntry]) -> Result<KVBatch> {
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
        let aux = self.build_aux_batch(height, &batch).c(d!())?;

        self.db.put_batch(batch).c(d!())?;
        self.db.commit(aux, flush).c(d!())?;

        Ok((self.root_hash(), height))
    }

    /// Export a copy of chain state on a specific height.
    ///
    /// * `cs` - The target chain state that holds the copy.
    /// * `height` - On which height the copy will be taken. It MUST be in range `[cur_height - ver_window, cur_height]`.\
    ///    Notes: Exported chain state holds less historical commits because `height <= cur_height`. `snapshot` is the
    ///    preferred method to export a copy on current height.
    ///
    pub fn export(&self, cs: &mut Self, height: u64) -> Result<()> {
        // Height must be in version window
        let cur_height = self.height().c(d!())?;
        let ver_range = (cur_height - self.ver_window)..=cur_height;
        if !ver_range.contains(&height) {
            return Err(eg!(format!(
                "height MUST be in the range: [{}, {}].",
                ver_range.start(),
                ver_range.end()
            )));
        }

        // Replay historical commit, if any, on every height
        for h in *ver_range.start()..=height {
            let mut kvs = KVMap::new();

            // setup bounds
            let lower = Prefix::new("VER".as_bytes()).push(Self::height_str(h).as_bytes());
            let upper = Prefix::new("VER".as_bytes()).push(Self::height_str(h + 1).as_bytes());

            // collect commits on this height
            self.iterate_aux(
                &lower.begin(),
                &upper.begin(),
                IterOrder::Asc,
                &mut |(k, v)| -> bool {
                    let raw_key = Self::get_raw_versioned_key(&k).unwrap_or_default();
                    if raw_key.is_empty() {
                        return false;
                    }

                    if v.eq(&TOMBSTONE) {
                        kvs.insert(raw_key.as_bytes().to_vec(), None);
                    } else {
                        kvs.insert(raw_key.as_bytes().to_vec(), Some(v));
                    }
                    false
                },
            );

            // commit this batch
            let batch = kvs.into_iter().collect::<Vec<_>>();
            if cs.commit(batch, h, true).is_err() {
                let msg = format!("Replay failed on height {}", h);
                return Err(eg!(msg));
            }
        }

        Ok(())
    }

    /// Take a snapshot of chain state on a specific height.
    ///
    /// * `path` - The path of database that holds the snapshot.
    ///
    pub fn snapshot<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.db.snapshot(path)
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

    /// Build a prefix for a versioned key
    pub fn versioned_key_prefix(height: u64) -> Prefix {
        Prefix::new("VER".as_bytes()).push(Self::height_str(height).as_bytes())
    }

    /// Build key Prefixed with Version height for Auxiliary data
    pub fn versioned_key(key: &[u8], height: u64) -> Vec<u8> {
        Self::versioned_key_prefix(height)
            .push(key)
            .as_ref()
            .to_vec()
    }

    /// Build a height string for versioning history
    fn height_str(height: u64) -> String {
        format!("{:020}", height)
    }

    /// Build a prefix for a base key
    pub fn base_key_prefix() -> Prefix {
        Prefix::new("BASE".as_bytes()).push(Self::height_str(0).as_bytes())
    }

    /// build key Prefixed with Baseline for Auxiliary data
    pub fn base_key(key: &[u8]) -> Vec<u8> {
        Self::base_key_prefix().push(key).as_ref().to_vec()
    }

    /// Deconstruct versioned key and return parsed raw key
    pub fn get_raw_versioned_key(key: &[u8]) -> Result<String> {
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
    ///
    /// The fn is NOT building a full chainstate any more after BASE introduced, it's now just building the delta
    /// - Option-1: Considering renaming as `build_state_delta()` in future
    /// - Option-2: Considering add a flag `delta_or_full` parameter in future
    pub fn build_state(&self, height: u64, prefix: Option<Prefix>) -> KVBatch {
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
                if v.eq(&TOMBSTONE) {
                    map.remove(raw_key.as_bytes());
                } else {
                    //update map with current KV
                    map.insert(raw_key.as_bytes().to_vec(), Some(v));
                }
                false
            },
        );

        if let Some(prefix) = prefix {
            let kvs: Vec<_> = map
                .into_iter()
                .map(|(k, v)| (prefix.push(&k).as_ref().to_vec(), v))
                .collect();
            kvs
        } else {
            map.into_iter().collect::<Vec<_>>()
        }
    }

    /// Get the value of a key at a given height
    ///
    /// Returns the value of the given key at a particular height
    /// Returns None if the key was deleted or invalid at height H
    pub fn get_ver(&self, key: &[u8], height: u64) -> Result<Option<Vec<u8>>> {
        //Make sure that this key exists to avoid expensive query
        let val = self.get(key).c(d!("error getting value"))?;
        if val.is_none() {
            return Ok(None);
        }

        //Need to set lower and upper bound as the height can get very large
        let mut lower_bound = 1;
        let upper_bound = height;
        let cur_height = self.height().c(d!("error reading current height"))?;
        if height >= cur_height {
            return Ok(val);
        }
        if cur_height > self.ver_window {
            lower_bound = cur_height.saturating_sub(self.ver_window);
        }

        // Iterate in descending order from upper bound until a value is found
        let mut val: Result<Option<Vec<u8>>> = Ok(None);
        let mut res = false;
        let lower_key = Self::versioned_key(key, lower_bound);
        let upper_key = Self::versioned_key(key, upper_bound.saturating_add(1));
        let _ = self.iterate_aux(&lower_key, &upper_key, IterOrder::Desc, &mut |(
            ver_k,
            v,
        )| {
            match Self::get_raw_versioned_key(&ver_k) {
                Ok(k) => {
                    if k.as_bytes().eq(key) {
                        if !v.eq(&TOMBSTONE) {
                            val = Ok(Some(v));
                        }
                        res = true;
                        return true;
                    }
                    return false;
                }
                Err(e) => {
                    val = Err(e).c(d!("error reading aux value"));
                    res = true;
                    return true;
                }
            }
        });

        if res {
            return val;
        }

        // //Iterate in descending order from upper bound until a value is found
        // for h in (lower_bound..upper_bound.saturating_add(1)).rev() {
        //     let key = Self::versioned_key(key, h);
        //     // Return if found a value matching key pattern
        //     if let Some(val) = self.get_aux(&key).c(d!("error reading aux value"))? {
        //         if val.eq(&TOMBSTONE) {
        //             return Ok(None);
        //         } else {
        //             return Ok(Some(val));
        //         }
        //     }
        // }

        // Search it in baseline if never versioned
        let key = Self::base_key(key);
        if let Some(val) = self.get_aux(&key).c(d!("error reading aux value"))? {
            Ok(Some(val))
        } else {
            Ok(None)
        }
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
        let mut batch = self.build_state(
            current_height - self.ver_window,
            Some(Self::base_key_prefix()),
        );
        // Update aux version if needed
        if self.version != CUR_AUX_VERSION {
            batch.push((
                AUX_VERSION.to_vec(),
                Some(CUR_AUX_VERSION.to_string().into_bytes()),
            ));
        }
        //Commit this batch at base height H
        if self.db.commit(batch, true).is_err() {
            println!("error building base chain state");
            return;
        }
        // Read back to make sure previous commit works well and update in-memory field
        self.version = self.get_aux_version().expect("cannot read back version");

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

    /// Gets current versioning range of the chain-state
    ///
    /// returns a range of the current versioning window [lower, upper)
    pub fn get_ver_range(&self) -> Result<Range<u64>> {
        let upper = self.height().c(d!("error reading current height"))?;
        let mut lower = 1;
        if upper > self.ver_window {
            lower = upper.saturating_sub(self.ver_window);
        }
        Ok(lower..upper)
    }
}
