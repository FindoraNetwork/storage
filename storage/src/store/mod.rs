use crate::db::MerkleDB;
use crate::state::State;
pub use traits::{Stated, Store};
pub use util::Prefix;

pub mod traits;
mod util;

/// Merkle-based prefixed store
pub struct PrefixedStore<'a, D: MerkleDB> {
    pfx: Prefix,
    state: &'a mut State<D>,
}

impl<'a, D: MerkleDB> Stated<'a, D> for PrefixedStore<'a, D> {
    fn set_state(&mut self, state: &'a mut State<D>) {
        self.state = state;
    }

    fn state(&self) -> &State<D> {
        self.state
    }

    fn state_mut(&mut self) -> &mut State<D> {
        self.state
    }

    fn prefix(&self) -> Prefix {
        self.pfx.clone()
    }
}

impl<'a, D: MerkleDB> Store<'a, D> for PrefixedStore<'a, D> {}

impl<'a, D: MerkleDB> PrefixedStore<'a, D> {
    pub fn new(prefix: &str, state: &'a mut State<D>) -> Self {
        PrefixedStore {
            pfx: Prefix::new(prefix.as_bytes()),
            state,
        }
    }
}

/// Merkle-based prefixed store
pub struct ImmutablePrefixedStore<'a, D: MerkleDB> {
    pfx: Prefix,
    state: &'a State<D>,
}

impl<'a, D: MerkleDB> Stated<'a, D> for ImmutablePrefixedStore<'a, D> {
    fn set_state(&mut self, _state: &'a mut State<D>) {
        panic!("State in immutable in this store")
    }

    fn state(&self) -> &State<D> {
        self.state
    }

    fn state_mut(&mut self) -> &mut State<D> {
        panic!("State in immutable in this store")
    }

    fn prefix(&self) -> Prefix {
        self.pfx.clone()
    }
}

impl<'a, D: MerkleDB> Store<'a, D> for ImmutablePrefixedStore<'a, D> {}

impl<'a, D: MerkleDB> ImmutablePrefixedStore<'a, D> {
    pub fn new(prefix: &str, state: &'a State<D>) -> Self {
        ImmutablePrefixedStore {
            pfx: Prefix::new(prefix.as_bytes()),
            state,
        }
    }
}
