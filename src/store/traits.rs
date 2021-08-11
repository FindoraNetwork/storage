use crate::db::{MerkleDB, IterOrder, KValue};
use crate::state::{State, KVecMap};
use crate::store::Prefix;
use std::collections::btree_map::IntoIter;
use ruc::*;
use serde::{de, Serialize};


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
                let obj = self.from_vec(&value).c(d!())?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// deserialize object from Vec<u8>
    fn from_vec<T>(&self, value: &[u8]) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        let obj = serde_json::from_slice::<T>(value).c(d!())?;
        Ok(obj)
    }

    /// get object by key
    ///
    /// return deserialized object if key exists or default object otherwise
    fn get_obj_or<T>(&self, key: &[u8], default: T) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        match self.get(key).c(d!())? {
            Some(value) => {
                let obj = serde_json::from_slice::<T>(value.as_ref()).c(d!())?;
                Ok(obj)
            }
            None => Ok(default),
        }
    }

    /// get value. Returns None if deleted
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.state().get(key)
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
        self.set(key.as_ref(), value);
        Ok(())
    }

    /// put/update KV
    fn set(&mut self, key: &[u8], value: Vec<u8>) {
        self.state_mut().set(key, value);
    }

    /// delete KV. Nothing happens if key not found
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.state_mut().delete(key)
    }
}


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

    /// get object by key
    ///
    /// return deserialized object if key exists or default object otherwise
    fn get_obj_or<T, D: MerkleDB>(state: &State<D>, key: &[u8], default: T) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        match state.get(key).c(d!())? {
            Some(value) => {
                let obj = serde_json::from_slice::<T>(value.as_ref()).c(d!())?;
                Ok(obj)
            }
            None => Ok(default),
        }
    }

    /// get value. Returns None if deleted
    fn get<T: MerkleDB>(state: &State<T>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        state.get(key)
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
    fn iter_cur<D: MerkleDB>(
        state: &State<D>,
        prefix: Prefix,
    ) -> IntoIter<Vec<u8>, Vec<u8>> {
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
        state.set(key.as_ref(), value);
        Ok(())
    }

    /// put/update KV
    fn set<D: MerkleDB>(state: &mut State<D>, key: &[u8], value: Vec<u8>) {
        state.set(key, value);
    }

    /// delete KV. Nothing happens if key not found
    fn delete<D: MerkleDB>(state: &mut State<D>, key: &[u8]) -> Result<()> {
        state.delete(key)
    }
}