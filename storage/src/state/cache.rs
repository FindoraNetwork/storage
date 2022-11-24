use crate::db::KVBatch;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "iterator")]
use std::iter::Iterator;

/// key-value map
pub type KVMap = BTreeMap<Vec<u8>, Option<Vec<u8>>>;
pub type KVecMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// size limits on KEY and VALUE
const MAX_MERK_KEY_LEN: u8 = u8::MAX;
const MAX_MERK_VAL_LEN: u16 = u16::MAX;

/// cache iterator
#[cfg(feature = "iterator")]
pub struct CacheIter<'a> {
    cache: &'a SessionedCache,
    layer: usize, // base is layer 0, delta is the last layer
    iter: std::collections::btree_map::Iter<'a, Vec<u8>, Option<Vec<u8>>>,
}

#[cfg(feature = "iterator")]
impl<'a> Iterator for CacheIter<'a> {
    type Item = (&'a Vec<u8>, &'a Option<Vec<u8>>);
    fn next(&mut self) -> Option<Self::Item> {
        let max = self.cache.stack.len() + 1;
        loop {
            // Iterates self.base, then self.stack, and self.delta
            // KVs that modified later will be skipped when iterating.
            if let Some(item) = self.iter.next() {
                if !self.cache.touched_since(item.0, self.layer + 1) {
                    break Some(item);
                }
            } else if self.layer < max - 1 {
                // switch to next layer on stack
                let msg = format!("missing stack? {}/{}", self.layer, max);
                let msg = msg.as_str();
                // `expect` in here is safe unless something is very wrong when panic should be a better choice
                self.iter = self.cache.stack.get(self.layer).expect(msg).iter();
                self.layer += 1;
            } else if self.layer == max - 1 {
                // switch to self.delta, the last layer
                self.iter = self.cache.delta.iter();
                self.layer += 1;
            } else {
                break None;
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Copy)]
pub enum StackStatus {
    Good,
    OverCommit,
    OverDiscard,
}

/// sessioned KV cache
#[derive(Clone)]
pub struct SessionedCache {
    delta: KVMap,
    base: KVMap,
    stack: Vec<KVMap>,
    status: StackStatus,
    is_merkle: bool,
}

#[allow(clippy::new_without_default)]
impl SessionedCache {
    pub fn new(is_merkle: bool) -> Self {
        SessionedCache {
            delta: KVMap::new(),
            base: KVMap::new(),
            stack: vec![],
            status: StackStatus::Good,
            is_merkle,
        }
    }

    pub fn stack_push(&mut self) {
        // push current delta (self.delta) to stack
        self.stack.push(std::mem::take(&mut self.delta));
    }

    pub fn stack_discard(&mut self) {
        // drop current delta (self.delta) and restore the last (stack head)
        if let Some(delta) = self.stack.pop() {
            self.delta = delta;
        } else {
            // nothing to discard
            self.status = StackStatus::OverDiscard;
        }
    }

    pub fn stack_commit(&mut self) {
        // merge last delta (stack head) into current (self.delta)
        if let Some(mut delta) = self.stack.pop() {
            delta.append(&mut self.delta);
            self.delta = delta;
        } else {
            // nothing to commit
            self.status = StackStatus::OverCommit;
        }
    }

    /// It's user's responsibility to pair ```stack_push/commit/discard```
    /// - If you never used stack APIs: NO NEED to call this function at all.
    /// - If you have  used stack APIs: NO NEED to call this function at all if you paired push/commit/discard.
    /// - If you have  used stack APIs: Call this function ONLY WHEN you don't know the push/commit/discard are well paired or not.
    ///
    /// Call ```discard``` to recover if it returns false
    pub fn good2_commit(&self) -> bool {
        self.stack.is_empty() && self.status == StackStatus::Good
    }

    /// put/update value by key
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> bool {
        if Self::check_kv(key, &value, self.is_merkle) {
            self.delta.insert(key.to_owned(), Some(value));
            return true;
        }
        false
    }

    /// delete key-pair (regardless of existence in DB) by marking as None
    /// - The `key` may or may not exist in DB, but we keep the intention of deletion regardless.
    pub fn delete(&mut self, key: &[u8]) {
        self.delta.insert(key.to_owned(), None);
    }

    /// Remove key-pair (when NOT EXIST in db) from cache
    ///
    /// Deprecated and replaced by `delete`
    pub fn remove(&mut self, key: &[u8]) {
        // exist in delta
        if let Some(Some(_)) = self.delta.get(key) {
            self.delta.remove(key);
        } else if let Some(Some(_)) = self.base.get(key) {
            // exist only in base
            self.delta.insert(key.to_owned(), None);
        }
    }

    /// commits pending KVs in session
    pub fn commit(&mut self) -> KVBatch {
        // Merge delta into the base version
        self.rebase();

        // Return updated values
        self.values()
    }

    /// commits pending KVs in session without return them
    pub fn commit_only(&mut self) {
        // Merge delta into the base version
        self.rebase();
    }

    /// discards pending KVs in session since last commit
    ///
    /// rollback to base and reset status
    pub fn discard(&mut self) {
        self.delta.clear();
        self.stack.clear();
        self.status = StackStatus::Good;
    }

    /// KV touched or not so far
    ///
    /// KV is touched whever it gets updated or deleted
    ///
    /// KV is touched even when value stays same
    ///
    /// use case: when KV is not allowed to change twice in one block
    pub fn touched(&self, key: &[u8]) -> bool {
        let touched_on_stack = || self.stack.iter().rev().any(|delta| delta.contains_key(key));
        self.delta.contains_key(key) || touched_on_stack() || self.base.contains_key(key)
    }

    // `since` is included
    pub fn touched_since(&self, key: &[u8], since: usize) -> bool {
        let max = self.stack.len() + 1;

        if since > max {
            false
        } else {
            let in_delta = self.delta.contains_key(key);
            if since == max {
                return in_delta;
            }
            let on_stack_or_in_delta = in_delta
                || self
                    .stack
                    .iter()
                    .skip(if since > 0 { since - 1 } else { 0 })
                    .any(|delta| delta.contains_key(key));
            if since > 0 {
                return on_stack_or_in_delta;
            }
            on_stack_or_in_delta || self.base.contains_key(key)
        }
    }

    /// Returns whether the cache is used for MerkDB or RocksDB
    pub fn is_merkle(&self) -> bool {
        self.is_merkle
    }

    /// KV deleted or not
    ///
    /// use case: stop reading KV from db if already deleted
    pub fn deleted(&self, key: &[u8]) -> bool {
        if self.delta.get(key) == Some(&None) {
            return true;
        }
        for delta in self.stack.iter().rev() {
            if let Some(v) = delta.get(key).map(|v| v.is_none()) {
                return v;
            }
        }
        if self.base.get(key) == Some(&None) {
            return true;
        }
        false
    }

    /// keys that have been touched
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys: BTreeSet<_> = self.base.keys().cloned().collect();
        for delta in &self.stack {
            let mut delta_keys = delta.keys().cloned().collect();
            keys.append(&mut delta_keys);
        }
        let mut delta = self.delta.keys().cloned().collect();
        keys.append(&mut delta);

        keys.into_iter().collect()
    }

    /// get all KVs
    pub fn values(&self) -> KVBatch {
        let mut kvs = self.base.clone();
        for x in &self.stack {
            kvs.append(&mut x.clone());
        }
        kvs.append(&mut self.delta.clone());
        kvs.into_iter().collect()
    }

    /// has value or not
    ///
    /// returns true  if new KV inserted
    ///
    /// returns true  if existing KV gets updated
    ///
    /// returns false if existing KV gets deleted
    ///
    /// use case: get from cache instead of db whenever hasv() returns true.
    pub fn hasv(&self, key: &[u8]) -> bool {
        match self.delta.get(key) {
            Some(Some(_)) => true, // has value in delta
            Some(None) => false,   // deleted in delta
            None => {
                // check if key exists on stack
                for delta in self.stack.iter().rev() {
                    match delta.get(key) {
                        Some(Some(_)) => return true,
                        Some(None) => return false,
                        None => {}
                    }
                }
                // check if key exists in base
                match self.base.get(key) {
                    Some(Some(_)) => true, // has value in base
                    Some(None) => false,   // deleted in delta
                    None => false,
                }
            }
        }
    }

    /// get value by key
    ///
    /// returns Some(value) if available
    ///
    /// returns None otherwise
    pub fn getv(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.delta.get(key) {
            Some(Some(value)) => Some(value.clone()),
            Some(None) => None,
            None => {
                // find if key exists on stack
                for delta in self.stack.iter().rev() {
                    match delta.get(key) {
                        Some(Some(value)) => return Some(value.clone()),
                        Some(None) => return None,
                        None => {}
                    }
                }
                // find if key exists in base
                match self.base.get(key) {
                    Some(Some(value)) => Some(value.clone()),
                    Some(None) => None,
                    None => None,
                }
            }
        }
    }

    /// get value by key
    ///
    /// returns Some(Some(value)) if available
    ///
    /// returns Some(None)  if deleted
    ///
    /// returns None otherwise. Note: Now used for test only.
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self.delta.get(key) {
            Some(Some(value)) => Some(Some(value.clone())),
            Some(None) => Some(None),
            None => {
                // find if the key exists on stack
                for delta in self.stack.iter().rev() {
                    match delta.get(key) {
                        Some(Some(value)) => return Some(Some(value.clone())),
                        Some(None) => return Some(None),
                        None => {}
                    }
                }
                // find if the key exists in base
                match self.base.get(key) {
                    Some(Some(value)) => Some(Some(value.clone())),
                    Some(None) => Some(None),
                    None => None,
                }
            }
        }
    }

    /// iterator
    #[cfg(feature = "iterator")]
    pub fn iter(&self) -> CacheIter {
        CacheIter {
            iter: self.base.iter(),
            layer: 0,
            cache: self,
        }
    }

    /// prefix iterator
    pub fn iter_prefix(&self, prefix: &[u8], map: &mut KVecMap) {
        // insert/update new KVs and remove deleted KVs
        let mut update = |k: &Vec<u8>, v: &Option<Vec<u8>>| {
            if k.starts_with(prefix) {
                if let Some(v) = v {
                    map.insert(k.to_owned(), v.to_owned());
                } else {
                    map.remove(k.as_slice());
                }
            }
        };
        #[cfg(feature = "iterator")]
        for (k, v) in self.iter() {
            update(k, v);
        }
        #[cfg(not(feature = "iterator"))]
        {
            // search in self.base
            for (k, v) in &self.base {
                update(k, v);
            }
            // search on stack
            for delta in &self.stack {
                for (k, v) in delta {
                    update(k, v);
                }
            }
            // search in self.delta
            for (k, v) in &self.delta {
                update(k, v);
            }
        }
    }

    /// rebases delta onto base
    /// make sure stack is empty before calling me
    fn rebase(&mut self) {
        self.base.append(&mut self.delta);
    }

    /// checks key value ranges
    ///
    /// if this cache is built on a MerkDB then ranges are enforced otherwise ranges are ignored.
    fn check_kv(key: &[u8], value: &[u8], is_merkle: bool) -> bool {
        if is_merkle {
            return MerkChecker::check_kv(key, value);
        }
        NoneChecker::check_kv(key, value)
    }
}

/// KV checker
pub trait KVChecker {
    fn check_kv(_key: &[u8], _value: &[u8]) -> bool {
        true
    }
}

pub struct NoneChecker;
impl KVChecker for NoneChecker {}

pub struct MerkChecker;
impl KVChecker for MerkChecker {
    fn check_kv(key: &[u8], value: &[u8]) -> bool {
        // check key
        if key.len() > MAX_MERK_KEY_LEN as usize {
            let key_str = String::from_utf8(key.to_vec()).map_or("non-utf8-key".to_owned(), |k| k);
            println!("Invalid key length: {}", key_str);
            return false;
        }

        // check value
        if value.len() > MAX_MERK_VAL_LEN as usize {
            println!("Invalid value length: {}", value.len());
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::SessionedCache;
    use crate::state::cache::KVecMap;

    #[test]
    fn cache_put_n_get() {
        let mut cache = SessionedCache::new(true);

        // put data
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());

        // stack_push should not effect `get`, `keys`, `hasv` interface
        cache.stack_push();

        // verify touched() flag
        assert!(cache.touched(b"k10"));
        assert!(cache.touched(b"k20"));

        // verify deleted() flag
        assert!(!cache.deleted(b"k10"));
        assert!(!cache.deleted(b"k20"));

        cache.stack_push();

        // verify keys
        assert_eq!(cache.keys(), vec![b"k10".to_vec(), b"k20".to_vec()]);

        cache.stack_push();

        // verify hasv() flag
        assert!(cache.hasv(b"k10"));
        assert!(cache.hasv(b"k20"));

        // getv and compare
        assert_eq!(cache.getv(b"k10").unwrap(), b"v10".to_vec());
        assert_eq!(cache.getv(b"k20").unwrap(), b"v20".to_vec());

        cache.stack_push();

        // get and compare
        assert_eq!(cache.get(b"k10").unwrap().unwrap(), b"v10".to_vec());
        assert_eq!(cache.get(b"k20").unwrap().unwrap(), b"v20".to_vec());
    }

    #[test]
    fn cache_put_n_update() {
        let mut cache = SessionedCache::new(true);

        // put data
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());

        cache.stack_push();

        // update data
        cache.put(b"k10", b"v11".to_vec());
        cache.put(b"k20", b"v21".to_vec());

        // verify touched() flag
        assert!(cache.touched(b"k10"));
        assert!(cache.touched(b"k20"));

        // verify deleted() flag
        assert!(!cache.deleted(b"k10"));
        assert!(!cache.deleted(b"k20"));

        // verify keys
        assert_eq!(cache.keys(), vec![b"k10".to_vec(), b"k20".to_vec()]);

        // verify hasv() flag
        assert!(cache.hasv(b"k10"));
        assert!(cache.hasv(b"k20"));

        // getv and compare
        assert_eq!(cache.getv(b"k10").unwrap(), b"v11".to_vec());
        assert_eq!(cache.getv(b"k20").unwrap(), b"v21".to_vec());

        // get and compare
        assert_eq!(cache.get(b"k10").unwrap().unwrap(), b"v11".to_vec());
        assert_eq!(cache.get(b"k20").unwrap().unwrap(), b"v21".to_vec());
    }

    #[test]
    fn cache_put_n_delete() {
        let mut cache = SessionedCache::new(true);

        // put data
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());

        cache.stack_push();

        // delete data
        cache.delete(b"k10");
        cache.delete(b"k20");

        cache.stack_push();

        // verify touched() flag
        assert!(cache.touched(b"k10"));
        assert!(cache.touched(b"k20"));

        // verify deleted() flag
        assert!(cache.deleted(b"k10"));
        assert!(cache.deleted(b"k20"));

        // verify keys
        assert_eq!(cache.keys(), vec![b"k10".to_vec(), b"k20".to_vec()]);

        // verify hasv() flag
        assert!(!cache.hasv(b"k10"));
        assert!(!cache.hasv(b"k20"));

        // getv and compare
        assert_eq!(cache.getv(b"k10"), None);
        assert_eq!(cache.getv(b"k20"), None);

        // get and compare
        assert_eq!(cache.get(b"k10").unwrap(), None);
        assert_eq!(cache.get(b"k20").unwrap(), None);
    }

    #[test]
    fn cache_put_n_commit() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();

        // put data and commit
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());

        cache.stack_commit();
        cache.commit();

        // verify touched() flag
        assert!(cache.touched(b"k10"));
        assert!(cache.touched(b"k20"));

        // verify deleted() flag
        assert!(!cache.deleted(b"k10"));
        assert!(!cache.deleted(b"k20"));

        // verify keys
        assert_eq!(cache.keys(), vec![b"k10".to_vec(), b"k20".to_vec()]);

        // verify hasv() flag
        assert!(cache.hasv(b"k10"));
        assert!(cache.hasv(b"k20"));

        // getv and compare
        assert_eq!(cache.getv(b"k10").unwrap(), b"v10".to_vec());
        assert_eq!(cache.getv(b"k20").unwrap(), b"v20".to_vec());

        // get and compare
        assert_eq!(cache.get(b"k10").unwrap().unwrap(), b"v10".to_vec());
        assert_eq!(cache.get(b"k20").unwrap().unwrap(), b"v20".to_vec());
    }

    #[test]
    fn cache_commit_n_put_delete_again() {
        let mut cache = SessionedCache::new(true);

        // put data and commit
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.put(b"k30", b"v30".to_vec());
        cache.commit();

        // put/delete data again
        cache.put(b"k10", b"v11".to_vec());
        cache.delete(b"k20");
        cache.delete(b"k30");
        cache.put(b"k40", b"v40".to_vec());

        // verify touched() flag
        assert!(cache.touched(b"k10"));
        assert!(cache.touched(b"k20"));
        assert!(cache.touched(b"k30"));
        assert!(cache.touched(b"k40"));

        // verify deleted() flag
        assert!(!cache.deleted(b"k10"));
        assert!(cache.deleted(b"k20"));
        assert!(cache.deleted(b"k30"));
        assert!(!cache.deleted(b"k40"));

        // verify keys
        assert_eq!(
            cache.keys(),
            vec![
                b"k10".to_vec(),
                b"k20".to_vec(),
                b"k30".to_vec(),
                b"k40".to_vec()
            ]
        );

        // verify hasv() flag
        assert!(cache.hasv(b"k10"));
        assert!(!cache.hasv(b"k20"));
        assert!(!cache.hasv(b"k30"));
        assert!(cache.hasv(b"k40"));

        // getv and compare
        assert_eq!(cache.getv(b"k10").unwrap(), b"v11".to_vec());
        assert_eq!(cache.getv(b"k20"), None);
        assert_eq!(cache.getv(b"k30"), None);
        assert_eq!(cache.getv(b"k40").unwrap(), b"v40".to_vec());

        // get and compare
        assert_eq!(cache.get(b"k10").unwrap().unwrap(), b"v11".to_vec());
        assert_eq!(cache.get(b"k20").unwrap(), None);
        assert_eq!(cache.get(b"k30").unwrap(), None);
        assert_eq!(cache.get(b"k40").unwrap().unwrap(), b"v40".to_vec());
    }

    #[test]
    fn cache_commit_n_put_delete_n_discard() {
        let mut cache = SessionedCache::new(true);

        // put data and commit
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.commit();

        // put/delete data again
        cache.put(b"k10", b"v11".to_vec());
        cache.delete(b"k20");
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k40", b"v40".to_vec());

        // discard this session
        cache.discard();

        // verify touched() flag
        assert!(cache.touched(b"k10"));
        assert!(cache.touched(b"k20"));
        assert!(!cache.touched(b"k30"));
        assert!(!cache.touched(b"k40"));

        // verify deleted() flag
        assert!(!cache.deleted(b"k10"));
        assert!(!cache.deleted(b"k20"));
        assert!(!cache.deleted(b"k30"));
        assert!(!cache.deleted(b"k40"));

        // verify keys
        assert_eq!(cache.keys(), vec![b"k10".to_vec(), b"k20".to_vec()]);

        // verify hasv() flag
        assert!(cache.hasv(b"k10"));
        assert!(cache.hasv(b"k20"));
        assert!(!cache.hasv(b"k30"));
        assert!(!cache.hasv(b"k40"));

        // getv and compare
        assert_eq!(cache.getv(b"k10").unwrap(), b"v10".to_vec());
        assert_eq!(cache.getv(b"k20").unwrap(), b"v20".to_vec());
        assert_eq!(cache.getv(b"k30"), None);
        assert_eq!(cache.getv(b"k40"), None);

        // get and compare
        assert_eq!(cache.get(b"k10").unwrap().unwrap(), b"v10".to_vec());
        assert_eq!(cache.get(b"k20").unwrap().unwrap(), b"v20".to_vec());
        assert_eq!(cache.get(b"k30"), None);
        assert_eq!(cache.get(b"k40"), None);
    }

    #[test]
    fn cache_put_n_iterate() {
        let mut cache = SessionedCache::new(true);

        // put data in random order
        cache.put(b"k10", b"v10".to_vec());
        cache.stack_push();
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.stack_push();

        // iterate
        let actual = cache.values();
        let expected = vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
        ];

        // check
        assert_eq!(actual, expected);
    }

    #[test]
    fn cache_put_delete_n_iterate() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();
        // put data in random order
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k40", b"v40".to_vec());
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());

        cache.stack_push();
        // delete some and double-delete shouldn't hurt
        cache.delete(b"k10");
        cache.delete(b"k10");
        cache.delete(b"k30");

        // iterate
        let actual = cache.values();
        let expected = vec![
            (b"k10".to_vec(), None),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), None),
            (b"k40".to_vec(), Some(b"v40".to_vec())),
        ];

        // check
        assert_eq!(actual, expected);
    }

    #[test]
    fn cache_commit_n_put_delete_discard_n_iterate() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();
        // put data in random order and delete one
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k40", b"v40".to_vec());
        cache.stack_push();
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.stack_commit();
        cache.delete(b"k10");
        cache.stack_commit();
        cache.commit();

        cache.stack_push();
        // put/delete data again
        cache.put(b"k10", b"v11".to_vec());
        cache.delete(b"k20");
        cache.delete(b"k30");
        cache.stack_push();
        cache.put(b"k40", b"v41".to_vec());
        cache.put(b"k50", b"v50".to_vec());

        // iterate and check
        let actual = cache.values();
        let expected = vec![
            (b"k10".to_vec(), Some(b"v11".to_vec())),
            (b"k20".to_vec(), None),
            (b"k30".to_vec(), None),
            (b"k40".to_vec(), Some(b"v41".to_vec())),
            (b"k50".to_vec(), Some(b"v50".to_vec())),
        ];
        assert_eq!(actual, expected);

        cache.stack_commit();
        cache.stack_commit();
        // discard this session
        cache.discard();

        // iterate and check
        let actual = cache.values();
        let expected = vec![
            (b"k10".to_vec(), None),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
            (b"k40".to_vec(), Some(b"v40".to_vec())),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_rebase() {
        // =======================Test case==========================
        // base:  [(k1, v1),  (k2, v2), (k3, None), (k4, None)]
        // delta: [(k1, v11), (k2, None), (k3, v3), (k4, None), (k5, v5)]
        // after: [(k1, v11), (k2, None), (k3, v3), (k4, None), (k5, v5)]
        // ==========================================================
        let mut cache = SessionedCache::new(true);

        //Put some date into cache
        cache.put(b"k1", b"v1".to_vec());
        cache.put(b"k2", b"v2".to_vec());
        cache.delete(b"k3");
        cache.delete(b"k4");
        cache.rebase();
        //Add some delta values and rebase
        cache.put(b"k1", b"v11".to_vec());
        cache.delete(b"k2");
        cache.put(b"k3", b"v3".to_vec());
        cache.put(b"k5", b"v5".to_vec());
        cache.delete(b"k4");
        cache.rebase();

        //Check
        assert_eq!(cache.get(b"k1").unwrap(), Some(b"v11".to_vec()));
        assert_eq!(cache.get(b"k2").unwrap(), None);
        assert_eq!(cache.get(b"k3").unwrap(), Some(b"v3".to_vec()));
        assert_eq!(cache.get(b"k4").unwrap(), None);
        assert_eq!(cache.get(b"k5").unwrap(), Some(b"v5".to_vec()));
    }

    #[test]
    fn test_iterate_prefix() {
        let mut cache = SessionedCache::new(true);
        let mut my_cache = KVecMap::new();

        cache.stack_push();
        //Put some date into cache
        cache.put(b"validator_1", b"v10".to_vec());
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.stack_push();
        cache.put(b"validator_5", b"v50".to_vec());
        cache.put(b"validator_3", b"v30".to_vec());
        cache.put(b"validator_2", b"v20".to_vec());
        cache.put(b"validator_4", b"v40".to_vec());

        cache.stack_push();
        //Del two of validators
        cache.delete(b"validator_1");
        cache.delete(b"validator_3");

        cache.iter_prefix(b"validator", &mut my_cache);

        let expected = vec![
            (b"validator_2".to_vec(), b"v20".to_vec()),
            (b"validator_4".to_vec(), b"v40".to_vec()),
            (b"validator_5".to_vec(), b"v50".to_vec()),
        ];

        let values: Vec<_> = my_cache
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        assert_eq!(values, expected);
    }
    #[test]
    fn cache_stack_push() {
        let mut cache = SessionedCache::new(true);

        cache.put(b"test_key_0", b"test_value_0".to_vec());
        cache.put(b"test_key_1", b"test_value_1".to_vec());

        cache.stack_push();

        cache.put(b"test_key_2", b"test_value_2".to_vec());
        cache.put(b"test_key_3", b"test_value_3".to_vec());

        cache.stack_push();

        cache.delete(b"test_key_1");

        cache.stack_push();

        cache.put(b"test_key_2", b"value_2".to_vec());

        let expected = vec![
            (b"test_key_0".to_vec(), Some(b"test_value_0".to_vec())),
            (b"test_key_1".to_vec(), None),
            (b"test_key_2".to_vec(), Some(b"value_2".to_vec())),
            (b"test_key_3".to_vec(), Some(b"test_value_3".to_vec())),
        ];

        let values = cache.values();

        assert_eq!(expected, values);
    }

    #[test]
    fn cache_stack_discard() {
        let mut cache = SessionedCache::new(true);

        cache.put(b"test_key_0", b"test_value_0".to_vec());
        cache.put(b"test_key_1", b"test_value_1".to_vec());

        cache.stack_push();

        cache.delete(b"test_key_1");
        cache.put(b"test_key_2", b"test_value_2".to_vec());

        assert_eq!(cache.getv(b"test_key_0"), Some(b"test_value_0".to_vec()));
        assert_eq!(cache.getv(b"test_key_1"), None);
        assert_eq!(cache.getv(b"test_key_2"), Some(b"test_value_2".to_vec()));

        assert!(!cache.good2_commit());

        // will drop changes since last stack_push
        cache.stack_discard();

        assert_eq!(cache.getv(b"test_key_0"), Some(b"test_value_0".to_vec()));
        assert_eq!(cache.getv(b"test_key_1"), Some(b"test_value_1".to_vec()));
        assert_eq!(cache.getv(b"test_key_2"), None);

        assert!(cache.good2_commit());
        cache.commit();
    }

    #[test]
    fn cache_stack_commit() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();

        cache.put(b"test_key_0", b"test_value_0".to_vec());
        cache.put(b"test_key_1", b"test_value_1".to_vec());
        cache.delete(b"test_key_2");

        // all stack should be committed or discarded before calling cache.commit
        assert!(!cache.good2_commit());

        cache.stack_commit();
        // All key-values are stored in self.delta now,
        // we need commit the cache to rebase the cache
        assert!(cache.good2_commit());
        cache.commit();

        assert_eq!(cache.getv(b"test_key_0"), Some(b"test_value_0".to_vec()));
        assert_eq!(cache.getv(b"test_key_1"), Some(b"test_value_1".to_vec()));
        assert_eq!(cache.getv(b"test_key_2"), None);

        // OverDiscard will not effect key-values before stack_commit
        cache.stack_discard();
        assert!(!cache.good2_commit());
        cache.discard();

        let expected = vec![
            (b"test_key_0".to_vec(), Some(b"test_value_0".to_vec())),
            (b"test_key_1".to_vec(), Some(b"test_value_1".to_vec())),
            (b"test_key_2".to_vec(), None),
        ];

        let kvs = cache.values();

        assert_eq!(expected, kvs);

        assert!(cache.good2_commit());
        cache.commit();
    }

    #[test]
    fn cache_exist_since() {
        let mut cache = SessionedCache::new(true);

        // store in same layer self.delta, layer 1
        cache.put(b"key0", b"value0".to_vec());
        cache.put(b"key1", b"value1".to_vec());
        cache.put(b"key2", b"value2".to_vec());

        assert!(cache.touched_since(b"key0", 0));
        assert!(cache.touched_since(b"key1", 1));
        assert!(!cache.touched_since(b"key2", 2));

        cache.stack_push();

        // store in self.stack[0]
        // self.delta is layer 2 now, and it's empty
        assert!(cache.touched_since(b"key0", 0));
        assert!(cache.touched_since(b"key1", 1));
        assert!(!cache.touched_since(b"key2", 2));

        // store in self.delta, layer 1
        cache.stack_commit();
        // store in self.base, layer 0
        cache.commit_only();

        assert!(cache.touched_since(b"key0", 0));
        assert!(!cache.touched_since(b"key1", 1));
        assert!(!cache.touched_since(b"key2", 2));
    }

    // Case 1: A(commit)------------------>B(commit)---------------------------C(commit)
    // Expected behavior: commit A+B+C
    #[test]
    fn cache_nested_stack_commit_discard_case1() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();
        cache.put(b"key0", b"value0".to_vec());
        cache.put(b"key1", b"value1".to_vec());

        {
            cache.stack_push();
            cache.put(b"key0", b"another_value0".to_vec());
            {
                cache.stack_push();
                cache.delete(b"key1");
                cache.stack_commit();
            }
            cache.stack_commit();
        }

        cache.stack_commit();

        assert_eq!(cache.getv(b"key0"), Some(b"another_value0".to_vec()));
        assert_eq!(cache.getv(b"key1"), None);
    }
    // Case 2: A(commit)------------------>B(commit)---------------------------C(revert/discard)
    // Expected behavior: commit A+B
    #[test]
    fn cache_nested_stack_commit_discard_case2() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();
        cache.put(b"key0", b"value0".to_vec());
        cache.put(b"key1", b"value1".to_vec());
        {
            cache.stack_push();
            cache.put(b"key0", b"another_value0".to_vec());
            {
                cache.stack_push();
                cache.delete(b"key1");
                cache.stack_discard();
            }
            cache.stack_commit();
        }

        cache.stack_commit();

        assert_eq!(cache.getv(b"key0"), Some(b"another_value0".to_vec()));
        assert_eq!(cache.getv(b"key1"), Some(b"value1".to_vec()));
    }

    // Case 3: A(commit)----------------->B(revert/discard)-------------------C(commit/revert/discard)
    // Expected behavior: commit A
    #[test]
    fn cache_nested_stack_commit_discard_case3() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();
        cache.put(b"key0", b"value0".to_vec());
        cache.put(b"key1", b"value1".to_vec());

        {
            cache.stack_push();
            cache.put(b"key0", b"another_value0".to_vec());
            {
                cache.stack_push();
                cache.delete(b"key1");
                cache.stack_discard();

                cache.stack_push();
                cache.put(b"key2", b"value2".to_vec());
                cache.stack_commit();
            }
            cache.stack_discard();
        }

        cache.stack_commit();

        assert_eq!(cache.getv(b"key0"), Some(b"value0".to_vec()));
        assert_eq!(cache.getv(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(cache.getv(b"key2"), None);
        assert!(!cache.touched(b"key2"));
    }

    // Case 4: A(revert/discard)-------->B(commit/revert/discard)---------C(commit/revert/discard)
    // Expected behavior: commit nothing
    #[test]
    fn cache_nested_stack_commit_discard_case4() {
        let mut cache = SessionedCache::new(true);

        cache.stack_push();
        cache.put(b"key0", b"value0".to_vec());
        cache.put(b"key1", b"value1".to_vec());
        {
            cache.stack_push();
            cache.put(b"key0", b"another_value0".to_vec());
            {
                cache.stack_push();
                cache.delete(b"key1");
                cache.stack_discard();

                cache.stack_push();
                cache.put(b"key2", b"value2".to_vec());
                cache.stack_commit();
            }
            cache.stack_discard();
        }
        {
            cache.stack_push();
            cache.put(b"key0", b"another_value0".to_vec());
            {
                cache.stack_push();
                cache.delete(b"key1");
                cache.stack_discard();

                cache.stack_push();
                cache.put(b"key3", b"value3".to_vec());
                cache.stack_commit();
            }
            cache.stack_commit();
        }

        cache.stack_discard();

        assert!(!cache.touched(b"key0"));
        assert!(!cache.touched(b"key1"));
        assert!(!cache.touched(b"key2"));
    }
    #[test]
    fn cache_nested_stack_commit_discard() {
        let mut cache = SessionedCache::new(true);

        // no stack_push before these updates
        cache.put(b"key0", b"value0".to_vec());
        cache.put(b"key1", b"value1".to_vec());

        // nested commit in discard
        {
            cache.stack_push();
            cache.put(b"key7", b"value7".to_vec());
            {
                cache.stack_push();
                cache.put(b"key8", b"value8".to_vec());
                cache.delete(b"key0");
                cache.stack_commit();
                assert_eq!(cache.getv(b"key8"), Some(b"value8".to_vec()));
                assert_eq!(cache.getv(b"key0"), None);
            }
            cache.stack_discard();

            assert_eq!(cache.getv(b"key0"), Some(b"value0".to_vec()));
            assert_eq!(cache.getv(b"key1"), Some(b"value1".to_vec()));
            assert_eq!(cache.getv(b"key7"), None);
            assert_eq!(cache.getv(b"key8"), None);
        }

        // nested discard in commit
        {
            cache.stack_push();
            cache.put(b"key2", b"value2".to_vec());

            {
                cache.stack_push();
                cache.delete(b"key0");
                cache.put(b"key1", b"another_value1".to_vec());
                cache.put(b"key2", b"another_value2".to_vec());
                cache.stack_discard();
            }

            cache.stack_commit();
            assert_eq!(cache.getv(b"key0"), Some(b"value0".to_vec()));
            assert_eq!(cache.getv(b"key1"), Some(b"value1".to_vec()));
            assert_eq!(cache.getv(b"key2"), Some(b"value2".to_vec()));
        }
    }
}
