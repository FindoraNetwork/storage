use std::collections::LinkedList;
use std::path::{Path, PathBuf};
use std::{collections::HashSet};

use failure::bail;
use rocksdb::ColumnFamilyDescriptor;

use crate::error::Result;
use crate::proofs::encode_into;
use crate::tree::{Batch, Commit, Fetch, Hash, Link, Op, RefWalker, Tree, Walker, NULL_HASH};

const ROOT_KEY_KEY: [u8; 4] = *b"root";

/// A handle to a Merkle key/value store backed by RocksDB.
pub struct Merk {
    pub(crate) tree: Option<Tree>,
    pub(crate) db: rocksdb::DB,
    pub(crate) path: PathBuf,
    deleted_keys: HashSet<Vec<u8>>,
}

impl Merk {
    /// Opens a store with the specified file path. If no store exists at that
    /// path, one will be created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Merk> {
        let db_opts = Merk::default_db_opts();
        Merk::open_opt(path, db_opts)
    }

    /// Opens a store with the specified file path and the given options. If no
    /// store exists at that path, one will be created.
    pub fn open_opt<P>(path: P, db_opts: rocksdb::Options) -> Result<Merk>
    where
        P: AsRef<Path>,
    {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let cfs = vec![
            // TODO: clone opts or take args
            ColumnFamilyDescriptor::new("aux", Merk::default_db_opts()),
            ColumnFamilyDescriptor::new("internal", Merk::default_db_opts()),
        ];
        let db = rocksdb::DB::open_cf_descriptors(&db_opts, &path_buf, cfs)?;

        let tree = Merk::load_root_node_from_db(&db)?;

        Ok(Merk {
            tree: tree,
            db,
            path: path_buf,
            deleted_keys: Default::default(),
        })
    }

    pub fn default_db_opts() -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.increase_parallelism(num_cpus::get() as i32);
        // opts.set_advise_random_on_open(false);
        opts.set_allow_mmap_writes(true);
        opts.set_allow_mmap_reads(true);
        opts.create_missing_column_families(true);
        opts.set_atomic_flush(true);
        // TODO: tune
        opts
    }

    fn load_root_node_from_db(db: &rocksdb::DB) -> Result<Option<Tree>> {
        // try to load root node
        let internal_cf = db.cf_handle("internal").unwrap();
        let tree = match db.get_pinned_cf(internal_cf, ROOT_KEY_KEY)? {
            Some(root_key) => Some(fetch_existing_node(&db, &root_key)?),
            None => None,
        };

        Ok(tree)
    }

    /// Gets an auxiliary value.
    pub fn get_aux(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let aux_cf = self.db.cf_handle("aux");
        Ok(self.db.get_cf(aux_cf.unwrap(), key)?)
    }

    /// Gets a value for the given key. If the key is not found, `None` is
    /// returned.
    ///
    /// Note that this is essentially the same as a normal RocksDB `get`, so
    /// should be a fast operation and has almost no tree overhead.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.use_tree(|maybe_tree| {
            let mut cursor = match maybe_tree {
                None => return Ok(None), // empty tree
                Some(tree) => tree,
            };

            loop {
                if key == cursor.key() {
                    return Ok(Some(cursor.value().to_vec()));
                }

                let left = key < cursor.key();
                let link = match cursor.link(left) {
                    None => return Ok(None), // not found
                    Some(link) => link,
                };

                let maybe_child = link.tree();
                match maybe_child {
                    None => break,                 // value is pruned, fall back to fetching from disk
                    Some(child) => cursor = child, // traverse to child
                }
            }

            // TODO: ignore other fields when reading from node bytes
            fetch_node(&self.db, key).map(|maybe_node| maybe_node.map(|node| node.value().to_vec()))
        })
    }

    /// Returns the root hash of the tree (a digest for the entire store which
    /// proofs can be checked against). If the tree is empty, returns the null
    /// hash (zero-filled).
    pub fn root_hash(&self) -> Hash {
        self.use_tree(|tree| tree.map_or(NULL_HASH, |tree| tree.hash()))
    }

    /// Applies a batch of operations (puts and deletes) to the tree.
    ///
    /// This will fail if the keys in `batch` are not sorted and unique. This
    /// check creates some overhead, so if you are sure your batch is sorted and
    /// unique you can use the unsafe `apply_unchecked` for a small performance
    /// gain.
    ///
    /// # Example
    /// ```
    /// # let mut store = fmerk::test_utils::TempMerk::new().unwrap();
    /// # store.apply(&[(vec![4,5,6], Op::Put(vec![0]))]).unwrap();
    ///
    ///
    /// use fmerk::Op;
    ///
    /// let batch = &[
    ///     (vec![1, 2, 3], Op::Put(vec![4, 5, 6])), // puts value [4,5,6] to key [1,2,3]
    ///     (vec![4, 5, 6], Op::Delete) // deletes key [4,5,6]
    /// ];
    /// store.apply(batch).unwrap();
    /// ```
    pub fn apply(&mut self, batch: &Batch) -> Result<()> {
        // ensure keys in batch are sorted and unique
        let mut maybe_prev_key = None;
        for (key, _) in batch.iter() {
            if let Some(prev_key) = maybe_prev_key {
                if prev_key > *key {
                    bail!("Keys in batch must be sorted");
                } else if prev_key == *key {
                    bail!("Keys in batch must be unique");
                }
            }
            maybe_prev_key = Some(key.to_vec());
        }

        unsafe { self.apply_unchecked(batch) }
    }

    /// Applies a batch of operations (puts and deletes) to the tree.
    ///
    /// This is unsafe because the keys in `batch` must be sorted and unique -
    /// if they are not, there will be undefined behavior. For a safe version of
    /// this method which checks to ensure the batch is sorted and unique, see
    /// `apply`.
    ///
    /// # Example
    /// ```
    /// # let mut store = fmerk::test_utils::TempMerk::new().unwrap();
    /// # store.apply(&[(vec![4,5,6], Op::Put(vec![0]))]).unwrap();
    ///
    /// use fmerk::Op;
    ///
    /// let batch = &[
    ///     (vec![1, 2, 3], Op::Put(vec![4, 5, 6])), // puts value [4,5,6] to key [1,2,3]
    ///     (vec![4, 5, 6], Op::Delete) // deletes key [4,5,6]
    /// ];
    /// unsafe { store.apply_unchecked(batch).unwrap() };
    /// ```
    pub unsafe fn apply_unchecked(&mut self, batch: &Batch) -> Result<()> {
        let maybe_walker = self
            .tree
            .take()
            .map(|tree| Walker::new(tree, self.source()));

        let (maybe_tree, mut deleted_keys) = Walker::apply_to(maybe_walker, batch)?;
        self.tree = maybe_tree;
        for key in deleted_keys {
            self.deleted_keys.insert(key);
        }
        // Note: we used to commit the tree here, but now it's expected that the user will commit after applying.
        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.deleted_keys.clear();
        self.tree = Merk::load_root_node_from_db(&self.db)?;
        Ok(())
    }

    /// Closes the store and deletes all data from disk.
    pub fn destroy(self) -> Result<()> {
        let opts = Merk::default_db_opts();
        let path = self.path.clone();
        drop(self);
        rocksdb::DB::destroy(&opts, path)?;
        Ok(())
    }

    /// Creates a Merkle proof for the list of queried keys. For each key in the
    /// query, if the key is found in the store then the value will be proven to
    /// be in the tree. For each key in the query that does not exist in the
    /// tree, its absence will be proven by including boundary keys.
    ///
    /// The proof returned is in an encoded format which can be verified with
    /// `merk::verify`.
    ///
    /// This will fail if the keys in `query` are not sorted and unique. This
    /// check adds some overhead, so if you are sure your batch is sorted and
    /// unique you can use the unsafe `prove_unchecked` for a small performance
    /// gain.
    pub fn prove(&mut self, query: &[Vec<u8>]) -> Result<Vec<u8>> {
        // ensure keys in query are sorted and unique
        let mut maybe_prev_key = None;
        for key in query.iter() {
            if let Some(prev_key) = maybe_prev_key {
                if prev_key > *key {
                    bail!("Keys in query must be sorted");
                } else if prev_key == *key {
                    bail!("Keys in query must be unique");
                }
            }
            maybe_prev_key = Some(key.to_vec());
        }

        unsafe { self.prove_unchecked(query) }
    }

    /// Creates a Merkle proof for the list of queried keys. For each key in the
    /// query, if the key is found in the store then the value will be proven to
    /// be in the tree. For each key in the query that does not exist in the
    /// tree, its absence will be proven by including boundary keys.
    ///
    /// The proof returned is in an encoded format which can be verified with
    /// `merk::verify`.
    ///
    /// This is unsafe because the keys in `query` must be sorted and unique -
    /// if they are not, there will be undefined behavior. For a safe version of
    /// this method which checks to ensure the batch is sorted and unique, see
    /// `prove`.
    pub unsafe fn prove_unchecked(&mut self, query: &[Vec<u8>]) -> Result<Vec<u8>> {
        let tree = match self.tree.as_mut() {
            None => bail!("Cannot create proof for empty tree"),
            Some(tree) => tree,
        };

        let source = MerkSource { db: &self.db };
        let mut ref_walker = RefWalker::new(tree, source);
        let (proof, _) = ref_walker.create_proof(query)?;

        let mut bytes = Vec::with_capacity(128);
        encode_into(proof.iter(), &mut bytes);
        Ok(bytes)
    }

    pub fn flush(&self) -> Result<()> {
        Ok(self.db.flush()?)
    }

    pub fn commit(&mut self, aux: &Batch) -> Result<()> {
        let internal_cf = self.db.cf_handle("internal").unwrap();
        let aux_cf = self.db.cf_handle("aux").unwrap();

        let mut batch = rocksdb::WriteBatch::default();
        let mut res_batch: Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> = match self.tree.as_mut() {
            // TODO: concurrent commit
            Some(tree) => {
                // TODO: configurable committer
                let mut committer = MerkCommitter::new(tree.height(), 1);
                tree.commit(&mut committer)?;

                // update pointer to root node
                batch.put_cf(internal_cf, ROOT_KEY_KEY, tree.key());

                Ok(committer.batch)
            }
            None => {
                // empty tree, delete pointer to root
                batch.delete_cf(internal_cf, ROOT_KEY_KEY);

                Ok(vec![])
            }
        };
        let mut to_batch = res_batch?;

        // TODO: move this to MerkCommitter impl?
        for key in self.deleted_keys.drain() {
            to_batch.push((key, None));
        }
        to_batch.sort_by(|a, b| a.0.cmp(&b.0));
        for (key, maybe_value) in to_batch {
            if let Some(value) = maybe_value {
                batch.put(key, value);
            } else {
                batch.delete(key);
            }
        }

        // update aux cf
        let aux_cf = self.db.cf_handle("aux").unwrap();
        for (key, value) in aux {
            match value {
                Op::Put(value) => batch.put_cf(aux_cf, key, value),
                Op::Delete => batch.delete_cf(aux_cf, key),
            };
        }

        // write to db
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(false);
        // TODO: disable WAL once we can ensure consistency with transactions
        self.db.write_opt(batch, &opts)?;

        Ok(())
    }

    pub fn snapshot<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let cp = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        cp.create_checkpoint(&path)?;
        Ok(())
    }

    pub fn walk<T>(&mut self, f: impl FnOnce(Option<RefWalker<MerkSource>>) -> T) -> T {
        let mut tree = self.tree.take();
        let maybe_walker = tree
            .as_mut()
            .map(|tree| RefWalker::new(tree, self.source()));
        let res = f(maybe_walker);
        self.tree = tree;
        res
    }

    pub fn raw_iter(&self) -> rocksdb::DBRawIterator {
        let internal_cf = self.db.cf_handle("internal").unwrap();
        self.db.raw_iterator_cf(internal_cf)
    }

    pub fn iter_opt(&self, mode: rocksdb::IteratorMode, readopts: rocksdb::ReadOptions) -> rocksdb::DBIterator {
        self.db.iterator_opt(mode, readopts)
    }

    pub fn iter_opt_aux(&self, mode: rocksdb::IteratorMode, readopts: rocksdb::ReadOptions) -> rocksdb::DBIterator {
        let aux_cf = self.db.cf_handle("aux").unwrap();
        self.db.iterator_cf_opt(aux_cf, readopts, mode)
    }

    fn source(&self) -> MerkSource {
        MerkSource { db: &self.db }
    }

    fn use_tree<T>(&self, mut f: impl FnMut(Option<&Tree>) -> T) -> T {
        //let tree = self.tree.take();
        let res = f(self.tree.as_ref());
        //self.tree.set(tree);
        res
    }

    fn use_tree_mut<T>(&mut self, mut f: impl FnMut(Option<&mut Tree>) -> T) -> T {
        //let mut tree = self.tree.take();
        let res = f(self.tree.as_mut());
        //self.tree.set(tree);
        res
    }
}

impl Drop for Merk {
    fn drop(&mut self) {
        self.db.flush().unwrap();
    }
}

#[derive(Clone)]
pub struct MerkSource<'a> {
    db: &'a rocksdb::DB,
}

impl<'a> Fetch for MerkSource<'a> {
    fn fetch(&self, link: &Link) -> Result<Tree> {
        fetch_existing_node(self.db, link.key())
    }
}

struct MerkCommitter {
    batch: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    height: u8,
    levels: u8,
}

impl MerkCommitter {
    fn new(height: u8, levels: u8) -> Self {
        MerkCommitter {
            batch: Vec::with_capacity(10000),
            height,
            levels,
        }
    }
}

impl Commit for MerkCommitter {
    fn write(&mut self, tree: &Tree) -> Result<()> {
        let mut buf = Vec::with_capacity(tree.encoding_length());
        tree.encode_into(&mut buf);
        self.batch.push((tree.key().to_vec(), Some(buf)));
        Ok(())
    }

    fn prune(&self, tree: &Tree) -> (bool, bool) {
        // keep N top levels of tree
        let prune = (self.height - tree.height()) >= self.levels;
        (prune, prune)
    }
}

fn fetch_node(db: &rocksdb::DB, key: &[u8]) -> Result<Option<Tree>> {
    let bytes = db.get_pinned(key)?;
    if let Some(bytes) = bytes {
        Ok(Some(Tree::decode(key.to_vec(), &bytes)))
    } else {
        Ok(None)
    }
}

fn fetch_existing_node(db: &rocksdb::DB, key: &[u8]) -> Result<Tree> {
    match fetch_node(db, key)? {
        None => bail!("key not found: {:?}", key),
        Some(node) => Ok(node),
    }
}

#[cfg(test)]
mod test {
    use super::Merk;
    use crate::test_utils::*;
    use crate::Op;
    use crate::tree;
    use std::thread;

    // TODO: Close and then reopen test

    fn assert_invariants(merk: &TempMerk) {
        merk.use_tree(|maybe_tree| {
            let tree = maybe_tree.expect("expected tree");
            assert_tree_invariants(tree);
        })
    }

    #[test]
    fn simple_insert_apply() {
        let batch_size = 20;

        let path = thread::current().name().unwrap().to_owned();
        let mut merk = TempMerk::open(path).expect("failed to open merk");

        let batch = make_batch_seq(0..batch_size);
        merk.apply(&batch).expect("apply failed");
        merk.commit(&[]).expect("commit failed");

        assert_invariants(&merk);
        assert_eq!(
            merk.root_hash(),
            [
                95, 202, 255, 82, 51, 192, 17, 216, 113, 188, 91, 15, 28, 0, 76, 243, 114, 206,
                127, 120, 103, 38, 8, 215, 74, 27, 72, 32, 36, 55, 250, 218
            ]
        );
    }

    #[test]
    fn insert_uncached() {
        let batch_size = 20;

        let path = thread::current().name().unwrap().to_owned();
        let mut merk = TempMerk::open(path).expect("failed to open merk");

        let batch = make_batch_seq(0..batch_size);
        merk.apply(&batch).expect("apply failed");
        merk.commit(&[]).expect("commit failed");
        assert_invariants(&merk);

        let batch = make_batch_seq(batch_size..(batch_size * 2));
        merk.apply(&batch).expect("apply failed");
        merk.commit(&[]).expect("commit failed");
        assert_invariants(&merk);
    }

    #[test]
    fn insert_rand() {
        let tree_size = 40;
        let batch_size = 4;

        let path = thread::current().name().unwrap().to_owned();
        let mut merk = TempMerk::open(path).expect("failed to open merk");

        for i in 0..(tree_size / batch_size) {
            println!("i:{}", i);
            let batch = make_batch_rand(batch_size, i);
            merk.apply(&batch).expect("apply failed");
            merk.commit(&[]).expect("commit failed");
        }
    }

    #[test]
    fn actual_deletes() {
        let path = thread::current().name().unwrap().to_owned();
        let mut merk = TempMerk::open(path).expect("failed to open merk");

        let batch = make_batch_rand(10, 1);
        merk.apply(&batch).expect("apply failed");
        merk.commit(&[]).expect("commit failed");

        let key = batch.first().unwrap().0.clone();
        merk.apply(&[(key.clone(), Op::Delete)]).unwrap();
        merk.commit(&[]).expect("commit failed");

        let value = merk.db.get(key.as_slice()).unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn aux_data() {
        let path = thread::current().name().unwrap().to_owned();
        let mut merk = TempMerk::open(path).expect("failed to open merk");
        merk.apply(&[]).expect("apply failed");
        merk.commit(&[(vec![1, 2, 3], Op::Put(vec![4, 5, 6]))])
            .expect("commit failed");
        let val = merk.get_aux(&[1, 2, 3]).unwrap();
        assert_eq!(val, Some(vec![4, 5, 6]));
    }

    #[test]
    #[ignore]
    fn simulated_crash() {
        let path = thread::current().name().unwrap().to_owned();
        let mut merk = CrashMerk::open(path).expect("failed to open merk");

        merk.apply(&[(vec![0], Op::Put(vec![1]))])
            .expect("apply failed");
        merk.commit(&[(vec![2], Op::Put(vec![3]))])
            .expect("commit failed");

        // make enough changes so that main column family gets auto-flushed
        for i in 0..250 {
            merk.apply(&make_batch_seq(i * 2_000..(i + 1) * 2_000))
                .expect("apply failed");

            merk.commit(&[]).expect("commit failed");
        }

        merk.crash().unwrap();

        assert_eq!(merk.get_aux(&[2]).unwrap(), Some(vec![3]));
        merk.destroy().unwrap();
    }

    #[test]
    fn get_not_found() {
        let path = thread::current().name().unwrap().to_owned();
        let mut merk = TempMerk::open(path).expect("failed to open merk");

        // no root
        assert!(merk.get(&[1, 2, 3]).unwrap().is_none());

        // cached
        merk.apply(&[(vec![5, 5, 5], Op::Put(vec![]))]).unwrap();
        merk.commit(&[]).expect("commit failed");
        assert!(merk.get(&[1, 2, 3]).unwrap().is_none());

        // uncached
        merk.apply(&[
            (vec![0, 0, 0], Op::Put(vec![])),
            (vec![1, 1, 1], Op::Put(vec![])),
            (vec![2, 2, 2], Op::Put(vec![])),
        ])
        .unwrap();

        merk.commit(&[]).expect("commit failed");
        assert!(merk.get(&[3, 3, 3]).unwrap().is_none());
    }

    #[test]
    fn iter_opt_range() {
        let path = thread::current().name().unwrap().to_owned();
        let mut merk = TempMerk::open(path).expect("failed to open merk");

        // apply data
        merk.apply(
            &[
                (b"k10".to_vec(), Op::Put(b"v10".to_vec())),
                (b"k20".to_vec(), Op::Put(b"v20".to_vec())),
                (b"k30".to_vec(), Op::Put(b"v30".to_vec())),
                (b"k40".to_vec(), Op::Put(b"v40".to_vec())),
                (b"k50".to_vec(), Op::Put(b"v50".to_vec())),
            ]
        )
        .unwrap();

        // commit with aux
        merk.commit(
            &[
                (b"k11".to_vec(), Op::Put(b"v11".to_vec())),
                (b"k21".to_vec(), Op::Put(b"v21".to_vec())),
                (b"k31".to_vec(), Op::Put(b"v31".to_vec())),
                (b"k41".to_vec(), Op::Put(b"v41".to_vec())),
                (b"k51".to_vec(), Op::Put(b"v51".to_vec())),
            ]
        ).unwrap();

        // iterate on range ["k20", "k50")
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(b"k20".to_vec());
        readopts.set_iterate_upper_bound(b"k50".to_vec());

        let iter = merk.iter_opt(rocksdb::IteratorMode::Start, readopts);
        let expected = vec![
            (b"k20".to_vec(), b"v20".to_vec()), 
            (b"k30".to_vec(), b"v30".to_vec()), 
            (b"k40".to_vec(), b"v40".to_vec())];
        let actual = iter.map(|(k, v) | {
            let kv = tree::Tree::decode(k.to_vec(), &v);
            (kv.key().to_vec(), kv.value().to_vec())
        }).collect::<Vec<_>>();
        assert_eq!(expected, actual);

        // iterate aux on range ["k21", "k51")
        let mut readopts_aux = rocksdb::ReadOptions::default();
        readopts_aux.set_iterate_lower_bound(b"k21".to_vec());
        readopts_aux.set_iterate_upper_bound(b"k51".to_vec());

        let iter_aux = merk.iter_opt_aux(rocksdb::IteratorMode::Start, readopts_aux);
        let expected_aux = vec![
            (b"k21".to_vec(), b"v21".to_vec()), 
            (b"k31".to_vec(), b"v31".to_vec()), 
            (b"k41".to_vec(), b"v41".to_vec())];
        let actual_aux = iter_aux.map(|(k, v) | (k.to_vec(), v.to_vec())).collect::<Vec<_>>();
        assert_eq!(expected_aux, actual_aux);
    }

    #[test]
    fn multiple_applies_before_commit() {
        let mut merk = TempMerk::new().expect("Failed to create temp merk");
        merk.apply(&[(vec![5, 5, 5], Op::Put(vec![1]))]).unwrap();
        assert_eq!(merk.get(&[5, 5, 5]).unwrap(), Some(vec![1]));
        merk.apply(&[
            (vec![5, 5, 5], Op::Put(vec![3])),
            (vec![6, 6, 6], Op::Put(vec![2])),
        ])
        .unwrap();
        assert_eq!(merk.get(&[5, 5, 5]).unwrap(), Some(vec![3]));
        assert_eq!(merk.get(&[6, 6, 6]).unwrap(), Some(vec![2]));
        merk.apply(&[(vec![6, 6, 6], Op::Delete)])
            .expect("apply failed");
        assert_eq!(merk.get(&[6, 6, 6]).unwrap(), None);
        merk.commit(&[]).expect("commit failed");
        assert_eq!(merk.get(&[5, 5, 5]).unwrap(), Some(vec![3]));
        assert_eq!(merk.get(&[6, 6, 6]).unwrap(), None);
    }

    #[test]
    fn clear() {
        let mut merk = TempMerk::new().expect("Failed to create temp merk");
        merk.apply(&[(vec![5, 5, 5], Op::Put(vec![1]))]).unwrap();
        assert_eq!(merk.get(&[5, 5, 5]).unwrap(), Some(vec![1]));
        merk.clear().expect("failed to clear");
        assert_eq!(merk.get(&[5, 5, 5]).unwrap(), None);
        merk.apply(&[(vec![6, 6, 6], Op::Put(vec![1]))]).unwrap();
        merk.commit(&[]).expect("failed to commit");
        merk.clear().expect("failed to clear");
        assert_eq!(merk.get(&[5, 5, 5]).unwrap(), None);
        assert_eq!(merk.get(&[6, 6, 6]).unwrap(), Some(vec![1]));
    }
}
