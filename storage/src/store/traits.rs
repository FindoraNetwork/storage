use crate::db::{IterOrder, KValue, MerkleDB};
use crate::state::{KVecMap, State};
use crate::store::Prefix;
use ruc::*;
use serde::{de, Serialize};
use std::collections::btree_map::IntoIter;

/// statable
pub trait Stated<'a, D: MerkleDB> {
    /// set state
    fn set_state(&mut self, state: &'a mut State<D>);

    /// get state
    fn state(&self) -> &State<D>;

    /// get mut state
    fn state_mut(&mut self) -> &mut State<D>;

    /// get base prefix
    fn prefix(&self) -> Prefix;
}

/// store for mempool/consensus/query connection
pub trait Store<'a, D>: Stated<'a, D>
where
    D: MerkleDB,
{
    //===========================read=============================
    fn with_state(&mut self, state: &'a mut State<D>) -> &Self {
        self.set_state(state);
        self
    }

    /// get object by key
    ///
    /// returns deserialized object if key exists or None otherwise
    fn get_obj<T>(&self, key: &[u8]) -> Result<Option<T>>
    where
        T: de::DeserializeOwned,
    {
        match self.get(key).c(d!())? {
            Some(value) => {
                let obj = Self::from_vec(&value).c(d!())?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// get object by key
    ///
    /// returns deserialized object if key exists or None otherwise
    fn get_obj_v<T>(&self, key: &[u8], height: u64) -> Result<Option<T>>
    where
        T: de::DeserializeOwned,
    {
        match self.get_v(key, height).c(d!())? {
            Some(value) => {
                let obj = Self::from_vec(&value).c(d!())?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// get object by key
    ///
    /// return deserialized object if key exists or default object otherwise
    fn get_obj_or<T>(&self, key: &[u8], default: T) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        match self.get_obj(key).c(d!())? {
            Some(value) => Ok(value),
            None => Ok(default),
        }
    }

    /// get versioned object by key
    ///
    /// return deserialized object if key exists or default object otherwise
    fn get_obj_v_or<T>(&self, key: &[u8], default: T, height: u64) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        match self.get_obj_v(key, height).c(d!())? {
            Some(value) => Ok(value),
            None => Ok(default),
        }
    }

    /// deserialize object from Vec<u8>
    fn from_vec<T>(value: &[u8]) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        let obj = serde_json::from_slice::<T>(value).c(d!())?;
        Ok(obj)
    }

    /// get value. Returns None if deleted
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.state().get(key)
    }

    /// get value by version.
    fn get_v(&self, key: &[u8], height: u64) -> Result<Option<Vec<u8>>> {
        self.state().get_ver(key, height)
    }

    /// iterate db only
    fn iter_db(&self, prefix: Prefix, asc: bool, func: &mut dyn FnMut(KValue) -> bool) -> bool {
        let mut iter_order = IterOrder::Desc;
        if asc {
            iter_order = IterOrder::Asc
        }
        self.state()
            .iterate(&prefix.begin(), &prefix.end(), iter_order, func)
    }

    /// iterate db AND cache combined
    fn iter_cur(&self, prefix: Prefix) -> IntoIter<Vec<u8>, Vec<u8>> {
        // Iterate chain state
        let mut kv_map = KVecMap::new();
        self.state().iterate(
            &prefix.begin(),
            &prefix.end(),
            IterOrder::Asc,
            &mut |(k, v)| -> bool {
                kv_map.insert(k, v);
                false
            },
        );

        // Iterate cache
        self.state().iterate_cache(prefix.as_ref(), &mut kv_map);
        kv_map.into_iter()
    }

    /// key exists or not. Returns false if deleted
    fn exists(&self, key: &[u8]) -> Result<bool> {
        self.state().exists(key)
    }

    /// KV touched or not in current block
    fn touched(&self, key: &[u8]) -> bool {
        self.state().touched(key)
    }

    /// get current height
    fn height(&self) -> Result<u64> {
        self.state().height()
    }

    /// dump data to json string. TBD!!
    fn dump_state() -> Result<String> {
        Ok(String::from(""))
    }

    //===========================write=============================
    fn with_state_mut(&mut self, state: &'a mut State<D>) -> &mut Self {
        self.set_state(state);
        self
    }

    /// put/update object by key
    fn set_obj<T>(&mut self, key: &[u8], obj: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let value = serde_json::to_vec(obj).c(d!())?;
        self.set(key.as_ref(), value)
    }

    /// put/update KV
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.state_mut().set(key, value)
    }

    /// delete KV. Nothing happens if key not found
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.state_mut().delete(key)
    }

    /// deprecated and replaced by `delete`
    fn delete_v0(&mut self, key: &[u8]) -> Result<()> {
        self.state_mut().delete_v0(key)
    }
}

/// A trait that implements the same functionality above without the requirement of owning a state
/// reference
pub trait StatelessStore {
    //===========================read=============================
    /// get object by key
    ///
    /// returns deserialized object if key exists or None otherwise
    fn get_obj<T, D>(state: &State<D>, key: &[u8]) -> Result<Option<T>>
    where
        T: de::DeserializeOwned,
        D: MerkleDB,
    {
        match state.get(key).c(d!())? {
            Some(value) => {
                let obj = serde_json::from_slice::<T>(&value).c(d!())?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// get versioned object by key
    ///
    /// returns deserialized object if key exists or None otherwise
    fn get_obj_v<T, D>(state: &State<D>, key: &[u8], height: u64) -> Result<Option<T>>
    where
        T: de::DeserializeOwned,
        D: MerkleDB,
    {
        match state.get_ver(key, height).c(d!())? {
            Some(value) => {
                let obj = serde_json::from_slice::<T>(&value).c(d!())?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// get object by key
    ///
    /// return deserialized object if key exists or default object otherwise
    fn get_obj_or<T, D: MerkleDB>(state: &State<D>, key: &[u8], default: T) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        match Self::get_obj(state, key).c(d!())? {
            Some(value) => Ok(value),
            None => Ok(default),
        }
    }

    /// get versioned object by key
    ///
    /// return deserialized object if key exists or default object otherwise
    fn get_obj_v_or<T, D: MerkleDB>(
        state: &State<D>,
        key: &[u8],
        default: T,
        height: u64,
    ) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        match Self::get_obj_v(state, key, height).c(d!())? {
            Some(value) => Ok(value),
            None => Ok(default),
        }
    }

    /// get value. Returns None if deleted
    fn get<T: MerkleDB>(state: &State<T>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        state.get(key)
    }

    /// get value by version.
    fn get_v<T: MerkleDB>(state: &State<T>, key: &[u8], height: u64) -> Result<Option<Vec<u8>>> {
        state.get_ver(key, height)
    }

    /// iterate db only
    fn iter_db<D: MerkleDB>(
        state: &State<D>,
        prefix: Prefix,
        asc: bool,
        func: &mut dyn FnMut(KValue) -> bool,
    ) -> bool {
        let mut iter_order = IterOrder::Desc;
        if asc {
            iter_order = IterOrder::Asc
        }
        state.iterate(&prefix.begin(), &prefix.end(), iter_order, func)
    }

    /// iterate db AND cache combined
    fn iter_cur<D: MerkleDB>(state: &State<D>, prefix: Prefix) -> IntoIter<Vec<u8>, Vec<u8>> {
        // Iterate chain state
        let mut kv_map = KVecMap::new();
        state.iterate(
            &prefix.begin(),
            &prefix.end(),
            IterOrder::Asc,
            &mut |(k, v)| -> bool {
                kv_map.insert(k, v);
                false
            },
        );

        // Iterate cache
        state.iterate_cache(prefix.as_ref(), &mut kv_map);
        kv_map.into_iter()
    }

    /// key exists or not. Returns false if deleted
    fn exists<D: MerkleDB>(state: &State<D>, key: &[u8]) -> Result<bool> {
        state.exists(key)
    }

    /// KV touched or not in current block
    fn touched<D: MerkleDB>(state: &State<D>, key: &[u8]) -> bool {
        state.touched(key)
    }

    /// get current height
    fn height<D: MerkleDB>(state: &State<D>) -> Result<u64> {
        state.height()
    }

    /// dump data to json string. TBD!!
    fn dump_state() -> Result<String> {
        Ok(String::from(""))
    }

    //===========================write=============================
    /// put/update object by key
    fn set_obj<T, D>(state: &mut State<D>, key: &[u8], obj: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
        D: MerkleDB,
    {
        let value = serde_json::to_vec(obj).c(d!())?;
        state.set(key.as_ref(), value)
    }

    /// put/update KV
    fn set<D: MerkleDB>(state: &mut State<D>, key: &[u8], value: Vec<u8>) -> Result<()> {
        state.set(key, value)
    }

    /// delete KV. Nothing happens if key not found
    fn delete<D: MerkleDB>(state: &mut State<D>, key: &[u8]) -> Result<()> {
        state.delete(key)
    }

    /// deprecated and replaced by `delete`
    fn delete_v0<D: MerkleDB>(state: &mut State<D>, key: &[u8]) -> Result<()> {
        state.delete_v0(key)
    }
}
