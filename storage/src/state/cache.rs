use crate::db::KVBatch;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::Iterator;

/// key-value map
pub type KVMap = BTreeMap<Vec<u8>, Option<Vec<u8>>>;
pub type KVecMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// size limits on KEY and VALUE
const MAX_MERK_KEY_LEN: u8 = u8::MAX;
const MAX_MERK_VAL_LEN: u16 = u16::MAX;

/// cache iterator
pub struct CacheIter<'a> {
    cache: &'a SessionedCache,
    in_base: bool,
    iter: std::collections::btree_map::Iter<'a, Vec<u8>, Option<Vec<u8>>>,
}

impl<'a> Iterator for CacheIter<'a> {
    type Item = (&'a Vec<u8>, &'a Option<Vec<u8>>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.iter.next() {
                if !self.in_base // iterating delta
                    || (!self.cache.delta.contains_key(item.0)
                        && !self.cache.removed.contains_key(item.0))
                {
                    break Some(item);
                }
            } else if self.in_base {
                self.in_base = false;
                self.iter = self.cache.delta.iter();
            } else {
                break None;
            }
        }
    }
}

/// sessioned KV cache
#[derive(Clone)]
pub struct SessionedCache {
    delta: KVMap,
    removed: KVMap,
    base: KVMap,
    is_merkle: bool,
}

#[allow(clippy::new_without_default)]
impl SessionedCache {
    pub fn new(is_merkle: bool) -> Self {
        SessionedCache {
            delta: KVMap::new(),
            removed: KVMap::new(),
            base: KVMap::new(),
            is_merkle,
        }
    }

    pub fn merge(&mut self, o: &mut Self) {
        o.rebase();
        self.base.append(&mut o.base);
    }

    /// put/update value by key
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> bool {
        if Self::check_kv(key, &value, self.is_merkle) {
            self.delta.insert(key.to_owned(), Some(value));
            self.removed.remove(key);
            return true;
        }
        false
    }

    /// delete key-pair by marking as None
    pub fn delete(&mut self, key: &[u8]) {
        self.delta.insert(key.to_owned(), None);
        self.removed.remove(key);
    }

    /// Remove Key from cur
    ///
    /// key may still exist in base after removal
    pub fn remove(&mut self, key: &[u8]) {
        self.delta.remove(key);
        self.removed.insert(key.to_owned(), None);
    }

    /// commits pending KVs in session
    pub fn commit(&mut self) -> KVBatch {
        // Merge current key value updates to the base version
        self.rebase();

        // Return current batch
        self.values()
    }

    /// commits pending KVs in session without return them
    pub fn commit_only(&mut self) {
        // Merge current key value updates to the base version
        self.rebase();
    }

    /// discards pending KVs in session
    ///
    /// rollback to base
    pub fn discard(&mut self) {
        self.delta.clear();
        self.removed.clear();
    }

    /// KV touched or not so far
    ///
    /// KV is touched whever it gets updated or deleted
    ///
    /// KV is touched even when value stays same
    ///
    /// use case: when KV is not allowed to updated twice
    pub fn touched(&self, key: &[u8]) -> bool {
        self.delta.contains_key(key)
            || (!self.removed.contains_key(key) && self.base.contains_key(key))
    }

    /// Returns whether the cache is used for MerkDB or RocksDB
    pub fn is_merkle(&self) -> bool {
        self.is_merkle
    }

    /// KV deleted or not
    ///
    /// use case: stop reading KV from db if already deleted
    pub fn deleted(&self, key: &[u8]) -> bool {
        if self.delta.get(key) == Some(&None)
            || (!self.removed.contains_key(key) && self.base.get(key) == Some(&None))
        {
            return true;
        }
        false
    }

    /// keys that have been touched
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys: BTreeSet<_> = self.base.keys().chain(self.delta.keys()).cloned().collect();
        self.removed.keys().for_each(|k| {
            keys.remove(k);
        });
        keys.into_iter().collect()
    }

    /// get all KVs
    pub fn values(&self) -> KVBatch {
        let mut kvs = self.base.clone();
        kvs.append(&mut self.delta.clone());
        self.removed.keys().for_each(|k| {
            kvs.remove(k);
        });
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
            // has value
            Some(Some(_)) => true,
            // deleted
            Some(None) => false,
            None => {
                // false: removed already or never see it
                // true: found in base and not removed
                !self.removed.contains_key(key) && self.base.contains_key(key)
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
                if self.removed.contains_key(key) {
                    None
                } else {
                    match self.base.get(key) {
                        Some(value) => value.clone(),
                        None => None,
                    }
                }
            }
        }
    }

    /// get value by key
    ///
    /// returns Some(Some(value)) if available
    ///
    /// returns Some(Some(None))  if deleted
    ///
    /// returns None otherwise
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self.delta.get(key) {
            Some(Some(value)) => Some(Some(value.clone())),
            Some(None) => Some(None),
            None => {
                if self.removed.contains_key(key) {
                    None
                } else {
                    match self.base.get(key) {
                        Some(Some(value)) => Some(Some(value.clone())),
                        Some(None) => Some(None),
                        None => None,
                    }
                }
            }
        }
    }

    /// iterator
    pub fn iter(&self) -> CacheIter {
        CacheIter {
            iter: self.base.iter(),
            in_base: true,
            cache: self,
        }
    }

    /// prefix iterator
    pub fn iter_prefix(&self, prefix: &[u8], map: &mut KVecMap) {
        // insert/update new KVs and remove deleted KVs
        for (k, v) in self.iter() {
            if k.starts_with(prefix) {
                if let Some(v) = v {
                    map.insert(k.to_owned(), v.to_owned());
                } else {
                    map.remove(k.as_slice());
                }
            }
        }
    }

    /// rebases cur KVs onto base
    fn rebase(&mut self) {
        self.base.append(&mut self.delta);
        for (k, _) in self.removed.iter() {
            self.base.remove(k);
        }
        self.delta.clear();
        self.removed.clear();
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
    fn cache_put_n_update() {
        let mut cache = SessionedCache::new(true);

        // put data
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());

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

        // delete data
        cache.delete(b"k10");
        cache.delete(b"k20");

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

        // put data and commit
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k20", b"v20".to_vec());
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
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());

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

        // put data in random order
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k40", b"v40".to_vec());
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());

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

        // put data in random order and delete one
        cache.put(b"k10", b"v10".to_vec());
        cache.put(b"k40", b"v40".to_vec());
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.delete(b"k10");
        cache.commit();

        // put/delete data again
        cache.put(b"k10", b"v11".to_vec());
        cache.delete(b"k20");
        cache.delete(b"k30");
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
    fn test_remove() {
        let mut cache = SessionedCache::new(true);

        //Put some date into cache
        cache.put(b"k40", b"v40".to_vec());
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.commit();

        //Remove one of the values
        cache.remove(b"k40");
        assert_eq!(cache.get(b"k40"), None);

        //Roll back above removal
        cache.discard();
        assert_eq!(cache.get(b"k40").unwrap(), Some(b"v40".to_vec()));

        //Remove it again and commit
        cache.remove(b"k40");
        cache.commit();
        assert_eq!(cache.get(b"k40"), None);

        //Remove a value that doesn't exist
        cache.remove(b"k50");
        assert_eq!(cache.get(b"k50"), None);
    }

    #[test]
    fn test_iterate_prefix() {
        let mut cache = SessionedCache::new(true);
        let mut my_cache = KVecMap::new();

        //Put some date into cache
        cache.put(b"validator_1", b"v10".to_vec());
        cache.put(b"k30", b"v30".to_vec());
        cache.put(b"k20", b"v20".to_vec());
        cache.put(b"validator_5", b"v50".to_vec());
        cache.put(b"validator_3", b"v30".to_vec());
        cache.put(b"validator_2", b"v20".to_vec());
        cache.put(b"validator_4", b"v40".to_vec());

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
}
