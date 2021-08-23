# storage

*A sessioned Merkle key/value store*

The crate was designed to be the blockchain state database. It provides persistent storage layer for key-value pairs on top of Nomic Merk.

### FEATURES:
- Key-value updates(in a transaction) applied to storage can be reverted if transaction fails.
- Key-value updates(in a block) applied to storage can be reverted if block failes to commit.
- Prefix-based key-value iteration on commited state.
- Prefix-based key-value iteration on latest state.
- Root hash calculation.

**Example:**
```rust
extern crate storage;
use storage::state::ChainState;
use storage::db::FinDB;
use storage::store::PrefixedStore;
use parking_lot::RwLock;
use std::sync::Arc;

fn prefixed_store() {
        // create store
        let path = thread::current().name().unwrap().to_owned();
        let fdb = FinDB::open(path).expect("failed to open db");
        let cs = Arc::new(RwLock::new(ChainState::new(fdb, "test_db".to_string())));
        let mut state = State::new(cs);
        let mut store = PrefixedStore::new("my_store", &mut state);
        let hash0 = store.state().root_hash();

        // set kv pairs and commit
        store.set(b"k10", b"v10".to_vec());
        store.set(b"k20", b"v20".to_vec());
        let (hash1, _height) = store.state_mut().commit(1).unwrap();

        // verify
        assert_eq!(store.get(b"k10").unwrap(), Some(b"v10".to_vec()));
        assert_eq!(store.get(b"k20").unwrap(), Some(b"v20".to_vec()));
        assert_ne!(hash0, hash1);

        // add, delete and update
        store.set(b"k10", b"v15".to_vec());
        store.delete(b"k20").unwrap();
        store.set(b"k30", b"v30".to_vec());

        // verify
        assert_eq!(store.get(b"k10").unwrap(), Some(b"v15".to_vec()));
        assert_eq!(store.get(b"k20").unwrap(), None);
        assert_eq!(store.get(b"k30").unwrap(), Some(b"v30".to_vec()));

        // rollback and verify
        store.state_mut().discard_session();
        assert_eq!(store.get(b"k10").unwrap(), Some(b"v10".to_vec()));
        assert_eq!(store.get(b"k20").unwrap(), Some(b"v20".to_vec()));
        assert_eq!(store.get(b"k30").unwrap(), None);
}
```
