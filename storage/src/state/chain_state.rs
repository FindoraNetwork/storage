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
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    ops::Range,
    path::Path,
    str,
};

const HEIGHT_KEY: &[u8; 6] = b"Height";
const BASE_HEIGHT_KEY: &[u8; 10] = b"BaseHeight";
const SNAPSHOT_KEY: &[u8; 8] = b"Snapshot";
const AUX_VERSION: &[u8; 10] = b"AuxVersion";
const AUX_VERSION_00: u64 = 0x00;
const AUX_VERSION_01: u64 = 0x01;
const AUX_VERSION_02: u64 = 0x02;
const SPLIT_BGN: &str = "_";
const TOMBSTONE: [u8; 1] = [206u8];

/// The length of a `Hash` (in bytes). same with fmerk.
pub const HASH_LENGTH: usize = 32;

/// A zero-filled `Hash`. same with fmerk.
pub const NULL_HASH: [u8; HASH_LENGTH] = [0; HASH_LENGTH];

#[derive(Debug, Clone)]
pub struct SnapShotInfo {
    pub start: u64,
    pub end: u64,
    pub count: u64,
}

/// Concrete ChainState struct containing a reference to an instance of MerkleDB, a name and
/// current tree height.
pub struct ChainState<D: MerkleDB> {
    name: String,
    ver_window: u64,
    interval: u64,
    snapshot_info: VecDeque<SnapShotInfo>,
    // the min height of the versioned keys
    min_height: u64,
    pinned_height: BTreeMap<u64, u64>,
    version: u64,
    db: D,
}

/// Configurable options
#[derive(Default, Clone, Debug)]
pub struct ChainStateOpts {
    pub name: Option<String>,
    pub ver_window: u64,
    pub interval: u64,
    pub cleanup_aux: bool,
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
        let opts = ChainStateOpts {
            name: if name.is_empty() { None } else { Some(name) },
            ver_window,
            cleanup_aux: false,
            ..Default::default()
        };

        Self::create_with_opts(db, opts)
    }
    /// Create a new instance of ChainState with user specified options
    ///
    pub fn create_with_opts(db: D, opts: ChainStateOpts) -> Self {
        let db_name = opts.name.unwrap_or_else(|| String::from("chain-state"));

        if opts.interval == 1 {
            panic!("snapshot interval cannot be One")
        }

        if opts.ver_window < opts.interval {
            panic!("version window is smaller than snapshot interval");
        }
        // ver_window is larger than snapshot_interval
        // ver_window should align at snapshot_interval
        if opts.interval != 0 && opts.ver_window % opts.interval != 0 {
            panic!("ver_window should align at snapshot interval");
        }

        if opts.ver_window == 0 && opts.cleanup_aux {
            panic!("perform an cleanup_aux and construct base on a no-version chain");
        }

        let mut cs = ChainState {
            name: db_name,
            ver_window: opts.ver_window,
            interval: opts.interval,
            snapshot_info: Default::default(),
            min_height: 0,
            pinned_height: Default::default(),
            version: Default::default(),
            db,
        };

        if opts.cleanup_aux {
            cs.clean_aux().unwrap();
            // move all keys to base
            cs.construct_base();
        }

        let mut base_height = None;
        let mut prev_interval = 0;

        match cs.get_aux_version().expect("Need a valid version") {
            None => {
                // initializing
                // version will be updated in `commit_db_with_meta`
                cs.version = AUX_VERSION_00;
            }
            Some(AUX_VERSION_01) => {
                // Version_01
                // 1. versioned keys are seperated into two sections: `base` and `VER`

                let h = cs.height().expect("Failed to get height");
                base_height = match h.cmp(&opts.ver_window) {
                    Ordering::Greater => Some(h.saturating_sub(opts.ver_window)),
                    _ => None,
                };
                // version will be updated in `commit_db_with_meta`
                cs.version = AUX_VERSION_01;
            }
            Some(AUX_VERSION_02) => {
                // Version_02
                // 1. `base_height` is persistent in aux db
                // 2. add snapshots for speedup get_ver
                base_height = cs
                    .base_height()
                    .expect("Failed to read base_height from aux db");
                prev_interval = cs
                    .snapshot_meta()
                    .expect("Failed to read snapshot meta from aux db")
                    .expect("missing snapshot meta");

                cs.version = AUX_VERSION_02;
            }
            Some(_) => {
                panic!("Invalid db version");
            }
        }

        println!(
            "{} {} {:?} {}",
            cs.name, cs.version, base_height, prev_interval
        );

        let mut batch = KVBatch::new();
        cs.clean_aux_db(&mut base_height, &mut batch);
        cs.build_snapshots(base_height, prev_interval, opts.interval, &mut batch);
        cs.commit_db_with_meta(batch);
        cs
    }

    /// Pin the ChainState at specified height
    ///
    pub fn pin_at(&mut self, height: u64) -> Result<()> {
        let current = self.height()?;
        if current < height {
            return Err(eg!("pin at future height"));
        }
        if height < self.min_height {
            return Err(eg!("pin at too old height"));
        }
        if self.ver_window == 0 {
            return Err(eg!("pin on non-versioned chain"));
        }

        let entry = self.pinned_height.entry(height).or_insert(0);
        *entry = entry.saturating_add(1);
        Ok(())
    }

    /// Unpin the ChainState at specified height
    ///
    pub fn unpin_at(&mut self, height: u64) {
        let remove = match self.pinned_height.get_mut(&height) {
            Some(count) if *count > 0 => {
                *count = count.saturating_sub(1);
                *count == 0
            }
            _ => unreachable!(),
        };
        if remove {
            assert_eq!(self.pinned_height.remove(&height), Some(0));
        }
    }

    /// Gets a value for the given key from the primary data section in RocksDB
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key)
    }

    // ver_window == 0 -> ver_window = 100
    // current height = 10000, cf internal
    // [0,10000] -> base prefix saved to aux

    // [9900, 10000] versioned, [0,9899] -> base prefix saved to aux

    /// Gets a value for the given key from the auxiliary data section in RocksDB.
    ///
    /// This section of data is not used for root hash calculations.
    pub fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_aux(key)
    }

    /// Get aux database version
    ///
    /// The default version is ox00
    fn get_aux_version(&self) -> Result<Option<u64>> {
        if let Some(version) = self.get_aux(AUX_VERSION.to_vec().as_ref())? {
            let ver_str = String::from_utf8(version).c(d!("Invalid aux version string"))?;
            let ver = ver_str
                .parse::<u64>()
                .c(d!("aux version should be a valid 64-bit long integer"))?;
            Ok(Some(ver))
        } else {
            Ok(None)
        }
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

    pub fn all_iterator(&self, order: IterOrder, func: &mut dyn FnMut(KValue) -> bool) -> bool {
        // Get DB iterator
        let mut db_iter = self.db.db_all_iterator(order);
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
    fn build_aux_batch(&mut self, height: u64, batch: &[KVEntry]) -> Result<KVBatch> {
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
            let upper = self.pinned_height.keys().min().map_or(height, |min| *min);
            let last_upper = self.min_height.saturating_add(self.ver_window);
            // the versioned keys before H = upper - ver_window - 1 are moved to base, H is included
            for h in last_upper..=upper {
                self.prune_aux_batch(h, &mut aux_batch)?;
            }

            let last_min_height = self.min_height;
            // update the left side of version window
            self.min_height = if upper > self.ver_window {
                upper.saturating_sub(self.ver_window)
            } else {
                // we only build base if height > ver_window
                0
            };
            if last_min_height > self.min_height {
                self.min_height = last_min_height;
            } else if self.min_height > 0 {
                // Store the base height in auxiliary batch
                aux_batch.push((
                    BASE_HEIGHT_KEY.to_vec(),
                    Some((self.min_height - 1).to_string().into_bytes()),
                ));
            }

            self.build_snapshots_at_height(height, last_min_height, &mut aux_batch);
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

    // Get max height of keys stored in `base`
    fn base_height(&self) -> Result<Option<u64>> {
        let height = self.db.get_aux(BASE_HEIGHT_KEY).c(d!())?;
        if let Some(value) = height {
            let height_str = String::from_utf8(value).c(d!())?;
            let height = height_str.parse::<u64>().c(d!())?;

            Ok(Some(height))
        } else {
            Ok(None)
        }
    }

    // Get the snapshot metadata
    fn snapshot_meta(&self) -> Result<Option<u64>> {
        let raw = self.db.get_aux(SNAPSHOT_KEY).c(d!())?;
        if let Some(value) = raw {
            let meta_str = String::from_utf8(value).c(d!())?;
            let meta = meta_str.parse::<u64>().c(d!())?;

            Ok(Some(meta))
        } else {
            Ok(None)
        }
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

    /// Build a prefix for a snapshot key
    pub(crate) fn snapshot_key_prefix(height: u64) -> Prefix {
        Prefix::new("SNAPSHOT".as_bytes()).push(Self::height_str(height).as_bytes())
    }

    /// Build a prefix for a base key
    pub(crate) fn base_key_prefix() -> Prefix {
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

    /// Build the chain-state from height 1 to height H
    ///
    /// Returns a batch with KV pairs valid at height H
    ///
    /// The fn is NOT building a full chainstate any more after BASE introduced, it's now just building the delta
    /// - Option-1: Considering renaming as `build_state_delta()` in future
    /// - Option-2: Considering add a flag `delta_or_full` parameter in future
    pub fn build_state(&self, height: u64, prefix: Option<Prefix>) -> KVBatch {
        self.build_state_to(None, height, prefix, false)
    }

    // height range is [s, e]
    // build versioned keys between [s,e] and save them under `prefix`
    fn build_state_to(
        &self,
        s: Option<u64>,
        e: u64,
        prefix: Option<Prefix>,
        keep_tombstone: bool,
    ) -> KVBatch {
        //New map to store KV pairs
        let mut map = KVMap::new();

        let lower = Prefix::new("VER".as_bytes());
        if let Some(start) = s {
            lower.push(Self::height_str(start).as_bytes());
        }
        let upper = Prefix::new("VER".as_bytes()).push(Self::height_str(e + 1).as_bytes());

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
                if !keep_tombstone && v.eq(&TOMBSTONE) {
                    map.remove(raw_key.as_bytes());
                } else {
                    //update map with current KV
                    if let Some(prefix) = &prefix {
                        map.insert(prefix.push(raw_key.as_bytes()).as_ref().to_vec(), Some(v));
                    } else {
                        map.insert(raw_key.as_bytes().to_vec(), Some(v));
                    }
                }
                false
            },
        );

        map.into_iter().collect::<Vec<_>>()
    }

    // Remove versioned keys before `height(included)`
    // Need a `commit` to actually remove these keys from persistent storage
    fn remove_versioned_keys_before(&self, height: u64) -> KVBatch {
        //Define upper and lower bounds for iteration
        let lower = Prefix::new("VER".as_bytes());
        let upper = Prefix::new("VER".as_bytes()).push(Self::height_str(height + 1).as_bytes());

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

        batch
    }

    /// Get the value of a key at a given height
    ///
    /// Returns the value of the given key at a particular height
    /// Returns None if the key was deleted or invalid at height H
    #[cfg(feature = "optimize_get_ver")]
    pub fn get_ver(&self, key: &[u8], height: u64) -> Result<Option<Vec<u8>>> {
        if self.ver_window == 0 {
            return Err(eg!("non-versioned chain"));
        }

        let cur_height = self.height().c(d!("error reading current height"))?;

        if self.interval != 0 {
            let height = if cur_height <= height {
                cur_height
            } else {
                height
            };
            return self.find_versioned_key_with_snapshots(key, height);
        }
        //Make sure that this key exists to avoid expensive query
        let val = self.get(key).c(d!("error getting value"))?;
        if val.is_none() {
            return Ok(None);
        }

        //Need to set lower and upper bound as the height can get very large
        let mut lower_bound = 1;
        let upper_bound = height;
        if height >= cur_height {
            return Ok(val);
        }
        if cur_height > self.ver_window {
            lower_bound = cur_height.saturating_sub(self.ver_window);
        }

        if lower_bound > self.min_height {
            lower_bound = self.min_height
        }

        match lower_bound.cmp(&height.saturating_add(1)) {
            Ordering::Greater => {
                // The keys at querying height are moved to base and override by later height
                // We cannot determine version info of the querying key
                return Err(eg!("height too old, no versioning info"));
            }
            Ordering::Equal => {
                // Search it in baseline if the querying height is moved to base but not override
                let key = Self::base_key(key);
                return self.get_aux(&key).c(d!("error reading aux value"));
            }
            _ => {
                // Perform another search in versioned keys
            }
        }

        // Iterate in descending order from upper bound until a value is found
        let mut val: Result<Option<Vec<u8>>> = Ok(None);
        let mut stop = false;
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
                        stop = true;
                        return true;
                    }
                    false
                }
                Err(e) => {
                    val = Err(e).c(d!("error reading aux value"));
                    stop = true;
                    true
                }
            }
        });

        if stop {
            return val;
        }

        // Search it in baseline
        let key = Self::base_key(key);
        self.get_aux(&key).c(d!("error reading aux value"))
    }

    /// Get the value of a key at a given height
    ///
    /// Returns the value of the given key at a particular height
    /// Returns None if the key was deleted or invalid at height H
    #[cfg(not(feature = "optimize_get_ver"))]
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

        if lower_bound > self.min_height {
            lower_bound = self.min_height
        }

        // The keys at querying height are moved to base and override by later height
        // So we cannot determine version info of the querying key
        if lower_bound > height.saturating_add(1) {
            return Err(eg!("height too old, no versioning info"));
        }

        //Iterate in descending order from upper bound until a value is found
        for h in (lower_bound..upper_bound.saturating_add(1)).rev() {
            let key = Self::versioned_key(key, h);
            // Return if found a value matching key pattern
            if let Some(val) = self.get_aux(&key).c(d!("error reading aux value"))? {
                if val.eq(&TOMBSTONE) {
                    return Ok(None);
                } else {
                    return Ok(Some(val));
                }
            }
        }

        // Search it in baseline if never versioned
        let key = Self::base_key(key);
        if let Some(val) = self.get_aux(&key).c(d!("error reading aux value"))? {
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    // simple commit to db
    fn commit_db_with_meta(&mut self, mut batch: KVBatch) {
        // Update aux version if needed
        if self.version != AUX_VERSION_02 {
            batch.push((
                AUX_VERSION.to_vec(),
                Some(AUX_VERSION_02.to_string().into_bytes()),
            ));
        }

        //Commit this batch to db
        if self.db.commit(batch, true).is_err() {
            println!("error building base chain state");
            return;
        }

        // Read back to make sure previous commit works well and update in-memory field
        self.version = self
            .get_aux_version()
            .expect("cannot read back version")
            .expect("Need a valid version");
    }

    fn build_snapshots_at_height(
        &mut self,
        height: u64,
        last_min_height: u64,
        aux_batch: &mut KVBatch,
    ) {
        if self.interval < 2 {
            // snapshot is disabled when interval == 0
            // snapshot interval can not be One
            return;
        }

        // Versioned keys before height `min_height` have been pruned and moved to `base`,
        // if there is a snapshot at height `min_height-1`, it should be removed too.
        // This could be multiple removals if `unpin` operations occurred.
        for snapshot_at in last_min_height..self.min_height {
            if snapshot_at > 0 && snapshot_at % self.interval == 0 {
                let mut batch = self.remove_snapshot(snapshot_at);
                aux_batch.append(&mut batch);
                while let Some(last) = self.snapshot_info.front() {
                    if last.end <= snapshot_at {
                        self.snapshot_info.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }

        // create last snapshot if necessary
        if height > 1 && height.saturating_sub(1) % self.interval == 0 {
            let e = height.saturating_sub(1);
            let s = if e == self.interval {
                0
            } else {
                e - self.interval + 1
            };
            let mut batch = self.create_snapshot(s, e);
            let count = batch.len() as u64;
            self.snapshot_info.push_back(SnapShotInfo {
                start: s,
                end: e,
                count,
            });
            aux_batch.append(&mut batch);
        }
    }

    fn build_snapshots(
        &mut self,
        base_height: Option<u64>,
        prev_interval: u64,
        interval: u64,
        batch: &mut KVBatch,
    ) {
        let height = self.height().expect("Failed to read chain height");

        batch.push((
            SNAPSHOT_KEY.to_vec(),
            Some(interval.to_string().into_bytes()),
        ));

        if prev_interval != 0 && prev_interval != interval {
            // remove all previous snapshots
            let mut snapshot_at = prev_interval;
            while snapshot_at <= height {
                let mut snapshot = self.remove_snapshot(snapshot_at);
                snapshot_at = snapshot_at.saturating_add(prev_interval);
                if !batch.is_empty() {
                    batch.append(&mut snapshot);
                }
            }
        }

        if interval != 0 {
            //create snapshots
            let mut s = base_height.map(|v| v.saturating_add(1)).unwrap_or_default();
            let mut e = if s != 0 && s % interval == 0 {
                s
            } else {
                (s / interval + 1) * interval
            };

            while e <= height {
                // create snapshot
                let count = if prev_interval != interval {
                    let mut snapshot = self.create_snapshot(s, e);
                    let count = snapshot.len();
                    batch.append(&mut snapshot);
                    count as u64
                } else {
                    // reconstruct snapshot info
                    self.count_in_snapshot(e)
                };
                self.snapshot_info.push_back(SnapShotInfo {
                    start: s,
                    end: e,
                    count,
                });
                s = e;
                e = e.saturating_add(interval);
            }
        }
    }

    /// When creating a new chain-state instance, any residual aux data outside the current window
    /// needs to be cleared as to not waste memory or disrupt the versioning behaviour.
    fn clean_aux_db(&mut self, base_height: &mut Option<u64>, batch: &mut KVBatch) {
        // A ChainState with pinned height, should never call this function
        assert!(self.pinned_height.is_empty());

        //Get current height
        let current_height = self.height().expect("failed to get chain height");
        if current_height == 0 {
            return;
        }
        if current_height < self.ver_window + 1 {
            // ver_window is increased, just update the min_height
            if let Some(h) = base_height {
                self.min_height = h.saturating_add(1);
            }
            return;
        }

        self.min_height = current_height - self.ver_window;
        //Get batch for state at H = current_height - ver_window - 1, H is included
        let mut base_batch = self.build_state(self.min_height - 1, Some(Self::base_key_prefix()));
        let current_base = match base_height {
            Some(h) if *h >= self.min_height => {
                assert!(base_batch.is_empty());
                self.min_height = h.saturating_add(1);
                *h
            }
            _ => self.min_height.saturating_sub(1),
        };
        batch.append(&mut base_batch);
        // Store the base height in auxiliary batch
        batch.push((
            BASE_HEIGHT_KEY.to_vec(),
            Some(current_base.to_string().into_bytes()),
        ));
        *base_height = Some(current_base);

        // Remove the versioned keys before H = current_height - self.ver_window - 1, H is included.
        let mut removed_keys = self.remove_versioned_keys_before(self.min_height - 1);

        batch.append(&mut removed_keys);
    }

    fn construct_base(&mut self) {
        let height = self.height().expect("Failed to get chain height");

        let mut batch = vec![
            (
                AUX_VERSION.to_vec(),
                Some(AUX_VERSION_02.to_string().into_bytes()),
            ),
            (
                BASE_HEIGHT_KEY.to_vec(),
                Some(height.to_string().into_bytes()),
            ),
            (SNAPSHOT_KEY.to_vec(), Some(0.to_string().into_bytes())),
        ];
        println!("{} construct base {}", self.name, height);

        self.all_iterator(IterOrder::Asc, &mut |(k, v)| -> bool {
            let base_key = Self::base_key(&k);
            batch.push((base_key, Some(v)));
            false
        });

        //Commit this batch to db
        self.db
            .commit(batch, true)
            .expect("error constructing chain base state");
    }

    /// Gets current versioning range of the chain-state
    ///
    /// returns a range of the current versioning window [lower, upper)
    pub fn get_ver_range(&self) -> Result<Range<u64>> {
        let upper = self.height().c(d!("error reading current height"))?;
        let mut lower = 0;
        if upper > self.ver_window {
            lower = upper.saturating_sub(self.ver_window);
        }
        if let Some(&pinned) = self.pinned_height.keys().min() {
            if pinned < lower {
                lower = pinned;
            }
        }
        Ok(lower..upper)
    }

    pub fn clean_aux(&mut self) -> Result<()> {
        let height = self.height().expect("Failed to read chain height");
        let batch = vec![(HEIGHT_KEY.to_vec(), Some(height.to_string().into_bytes()))];

        self.db.clean_aux()?;
        self.db.commit(batch, true)
    }

    /// get current pinned height
    ///
    pub fn current_pinned_height(&self) -> Vec<u64> {
        self.pinned_height.keys().cloned().collect()
    }

    /// Get current version window in database
    pub fn current_window(&self) -> Result<(u64, u64)> {
        if self.ver_window == 0 {
            return Err(eg!("Not supported for an non-versioned chain"));
        }
        let current = self.height()?;

        Ok((self.min_height, current))
    }

    // create an new snapshot at height `end` including versioned keys in height range [start, end]
    // snapshot prefix "SNAPSHOT-{end}"
    fn create_snapshot(&self, start: u64, end: u64) -> KVBatch {
        self.build_state_to(Some(start), end, Some(Self::snapshot_key_prefix(end)), true)
    }

    fn remove_snapshot(&self, height: u64) -> KVBatch {
        let mut map = KVMap::new();

        let lower = Prefix::new("SNAPSHOT".as_bytes()).push(Self::height_str(height).as_bytes());
        let upper =
            Prefix::new("SNAPSHOT".as_bytes()).push(Self::height_str(height + 1).as_bytes());

        self.iterate_aux(
            lower.as_ref(),
            upper.as_ref(),
            IterOrder::Asc,
            &mut |(k, _)| -> bool {
                // Only remove versioned kv pairs
                let raw_key = Self::get_raw_versioned_key(&k).unwrap_or_default();
                if raw_key.is_empty() {
                    return false;
                }
                // Mark this key to be deleted
                map.insert(k, None);
                false
            },
        );

        map.into_iter().collect::<Vec<_>>()
    }

    fn count_in_snapshot(&self, height: u64) -> u64 {
        let lower = Prefix::new("SNAPSHOT".as_bytes()).push(Self::height_str(height).as_bytes());
        let upper =
            Prefix::new("SNAPSHOT".as_bytes()).push(Self::height_str(height + 1).as_bytes());

        let mut count = 0u64;

        self.iterate_aux(
            lower.as_ref(),
            upper.as_ref(),
            IterOrder::Asc,
            &mut |(k, _v)| -> bool {
                let raw_key = Self::get_raw_versioned_key(&k).unwrap_or_default();
                if raw_key.is_empty() {
                    return false;
                }
                count = count.saturating_add(1);
                false
            },
        );

        count
    }

    fn find_versioned_key_with_range(
        &self,
        lower: Vec<u8>,
        upper: Vec<u8>,
        key: &[u8],
        order: IterOrder,
    ) -> Result<(bool, Option<Vec<u8>>)> {
        let mut val = Ok((false, None));

        let _ = self.iterate_aux(&lower, &upper, order, &mut |(ver_k, v)| {
            if let Ok(k) = Self::get_raw_versioned_key(&ver_k) {
                if k.as_bytes().eq(key) {
                    val = Ok((true, if !v.eq(&TOMBSTONE) { Some(v) } else { None }));
                    return true;
                }
            }
            false
        });
        val
    }

    fn last_snapshot(&self, height: u64) -> Option<usize> {
        let mut last = None;
        for (i, ss) in self.snapshot_info.iter().enumerate() {
            if ss.start > height {
                assert_eq!(i, 0);
                return None;
            } else if ss.end == height {
                return Some(i);
            } else if ss.end < height {
                last = Some(i);
                continue;
            } else {
                assert!(ss.start <= height && ss.end > height);
                return if i > 0 {
                    Some(i.saturating_sub(1))
                } else {
                    None
                };
            }
        }
        last
    }

    /// Get snapshot info
    pub fn get_snapshots_info(&self) -> Vec<SnapShotInfo> {
        self.snapshot_info.iter().cloned().collect()
    }

    /// The height of last snapshot before `height(included)`
    pub fn last_snapshot_before(&self, height: u64) -> Option<u64> {
        let interval = self.interval;
        if interval >= 2 {
            Some(if height % interval == 0 {
                height
            } else {
                height / interval * interval
            })
        } else {
            None
        }
    }

    /// The height of oldest snapshot
    pub fn oldest_snapshot(&self) -> Option<u64> {
        let interval = self.interval;
        let min_height = self.get_ver_range().ok()?.start;
        if interval >= 2 {
            Some(if min_height % interval == 0 {
                min_height
            } else {
                (min_height / interval + 1) * interval
            })
        } else {
            None
        }
    }

    fn find_versioned_key_with_snapshots(
        &self,
        key: &[u8],
        height: u64,
    ) -> Result<Option<Vec<u8>>> {
        // The keys at querying height are moved to base and override by later height
        // So we cannot determine version info of the querying key
        if self.min_height > height {
            return if self.min_height == height.saturating_add(1) {
                // search in the `base`
                let key = Self::base_key(key);
                self.get_aux(&key).c(d!("error reading aux value"))
            } else {
                Err(eg!("height too old, no versioning info"))
            };
        }

        let last = self.last_snapshot(height);

        let s = if let Some(idx) = last {
            let ss = self
                .snapshot_info
                .get(idx)
                .ok_or(eg!("cannot find snapshot information!"))?;
            ss.end
        } else {
            // the query height is less than all of the snapshots
            self.min_height
        };

        if s <= height {
            // search versioned key which beyonds snapshots
            let lower = Self::versioned_key(key, s);
            let upper = Self::versioned_key(key, height.saturating_add(1));
            let (stop, val) =
                self.find_versioned_key_with_range(lower, upper, key, IterOrder::Desc)?;
            if stop {
                return Ok(val);
            }
        }

        if let Some(last) = last {
            for idx in (0..=last).rev() {
                let ss = self
                    .snapshot_info
                    .get(idx)
                    .ok_or(eg!("cannot find snapshot info!"))?;
                let height = ss.end;
                if ss.count != 0 {
                    if let Some(v) =
                        self.get_aux(Self::snapshot_key_prefix(height).push(key).as_ref())?
                    {
                        return Ok(if v.eq(&TOMBSTONE) { None } else { Some(v) });
                    }
                }
            }
        }

        // search in base
        let key = Self::base_key(key);
        self.get_aux(&key).c(d!("error reading aux value"))
    }

    // hMove all the data before the specified height to base
    pub fn height_internal_to_base(&mut self, height: u64) -> Result<()> {
        let mut batch = KVBatch::new();

        self.all_iterator(IterOrder::Asc, &mut |(k, v)| -> bool {
            let base_key = Self::base_key(&k);
            batch.push((base_key, Some(v)));
            false
        });

        batch.push((
            BASE_HEIGHT_KEY.to_vec(),
            Some(height.to_string().into_bytes()),
        ));
        if self.db.commit(batch, true).is_err() {
            panic!("error move before a certain height chain state");
        }
        Ok(())
    }
}
