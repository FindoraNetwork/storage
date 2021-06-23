# storage

*A sessioned Merkle key/value store*

The crate was designed to be the blockchain state database. It provides persistent storage layer for key-value pairs on top of Nomic Merk.

### FEATURES:
- Key-value updates(in a transaction) applied to storage can be reverted if transaction fails.
- Key-value updates(in a block) applied to storage can be reverted if block failes to commit.
- Prefix-based key-value iteration on commited state.
- Prefix-based key-value iteration on latest state.
- Root hash calculation.
