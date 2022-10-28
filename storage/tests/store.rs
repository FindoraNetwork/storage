use parking_lot::RwLock;
use rand::Rng;
use ruc::*;
use std::sync::Arc;
use std::{thread, time};
use storage::db::{IterOrder, KValue, MerkleDB};
use storage::state::{ChainState, State};
use storage::store::{Prefix, PrefixedStore, Stated, Store};
use temp_db::{TempFinDB, TempRocksDB};

const VER_WINDOW: u64 = 100;

// a example store
struct StakeStore<'a, D: MerkleDB> {
    pfx: Prefix,
    state: &'a mut State<D>,
}

// impl Store
impl<'a, D: MerkleDB> Store<'a, D> for StakeStore<'a, D> {}

// impl Stated
impl<'a, D: MerkleDB> Stated<'a, D> for StakeStore<'a, D> {
    fn set_state(&mut self, state: &'a mut State<D>) {
        self.state = state;
    }

    fn state(&self) -> &State<D> {
        &self.state
    }

    fn state_mut(&mut self) -> &mut State<D> {
        self.state
    }

    fn prefix(&self) -> Prefix {
        self.pfx.clone()
    }
}

// impl Store business interfaces
impl<'a, D: MerkleDB> StakeStore<'a, D> {
    pub fn new(prefix: &str, state: &'a mut State<D>) -> Self {
        StakeStore {
            pfx: Prefix::new(prefix.as_bytes()),
            state,
        }
    }

    /// "stake_validator_fraxxxxx" ==> "amount"
    pub fn get_stake(&self, addr: &str) -> Result<u64> {
        let key = self.stake_key(addr);
        let amt = self.get_amount(key.as_ref()).c(d!())?;
        Ok(amt)
    }

    /// "stake_pool" ==> "amount"
    pub fn get_pool(&self) -> Result<u64> {
        let key = self.pool_key();
        let amt = self.get_amount(key.as_ref()).c(d!())?;
        Ok(amt)
    }

    /// "stake_validator_fraxxxxx" ==> "+amount"
    ///
    /// "stake_pool" ================> "+amount"
    pub fn stake(&mut self, addr: &str, amount: u64) -> Result<()> {
        let key = self.stake_key(addr);
        if self.touched(key.as_ref()) {
            return Err(eg!("Staking/Unstaking is alowed only once a block"));
        }
        self.add_amount(key.as_ref(), amount).c(d!())?;

        let key_pool = self.pool_key();
        self.add_amount(key_pool.as_ref(), amount).c(d!())?;
        Ok(())
    }

    /// "stake_validator_fraxxxxx" ==> "-amount"
    ///
    /// "stake_pool" ================> "-amount"
    pub fn unstake(&mut self, addr: &str, amount: u64) -> Result<()> {
        let key = self.stake_key(addr);
        if self.touched(key.as_ref()) {
            return Err(eg!("Staking/Unstaking is alowed only once a block"));
        }
        self.minus_amount(key.as_ref(), amount).c(d!())?;

        let key_pool = self.pool_key();
        self.minus_amount(key_pool.as_ref(), amount).c(d!())?;
        Ok(())
    }

    fn get_amount(&self, key: &[u8]) -> Result<u64> {
        let amt = self.get_obj_or(key.as_ref(), 0_u64).c(d!())?;
        Ok(amt)
    }

    fn add_amount(&mut self, key: &[u8], amount: u64) -> Result<()> {
        let amt = self.get_amount(key).c(d!())?;
        let amt_new = amt.checked_add(amount).c(d!())?;
        self.set_obj(key.as_ref(), &amt_new).c(d!())?;
        Ok(())
    }

    fn minus_amount(&mut self, key: &[u8], amount: u64) -> Result<()> {
        let amt = self.get_amount(key).c(d!())?;
        if amt >= amount {
            self.set_obj(key.as_ref(), &(amt - amount)).c(d!())?;
        } else {
            eg!("low balance");
        }
        Ok(())
    }

    // stake key is range-based
    fn stake_key(&self, addr: &str) -> Prefix {
        self.pfx.push_sub(b"validator", addr.as_ref())
    }

    // pool key isn't range-based
    fn pool_key(&self) -> Prefix {
        self.pfx.push(b"pool")
    }
}

#[test]
fn prefixed_store() {
    // create store
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "test_db".to_string(),
        VER_WINDOW,
    )));
    let mut state = State::new(cs, true);
    let mut store = PrefixedStore::new("my_store", &mut state);
    let hash0 = store.state().root_hash();

    // set kv pairs and commit
    store.set(b"k10", b"v10".to_vec()).unwrap();
    store.set(b"k20", b"v20".to_vec()).unwrap();
    let (hash1, _height) = store.state_mut().commit(1).unwrap();

    // verify
    assert_eq!(store.get(b"k10").unwrap(), Some(b"v10".to_vec()));
    assert_eq!(store.get(b"k20").unwrap(), Some(b"v20".to_vec()));
    assert_ne!(hash0, hash1);

    // add, del and update
    store.set(b"k10", b"v15".to_vec()).unwrap();
    store.delete(b"k20").unwrap();
    store.set(b"k30", b"v30".to_vec()).unwrap();

    // verify
    assert_eq!(store.get(b"k10").unwrap(), Some(b"v15".to_vec()));
    assert_eq!(store.get(b"k20").unwrap(), None);
    assert_eq!(store.get(b"k30").unwrap(), Some(b"v30".to_vec()));

    // revert and verify
    store.state_mut().discard_session();
    assert_eq!(store.get(b"k10").unwrap(), Some(b"v10".to_vec()));
    assert_eq!(store.get(b"k20").unwrap(), Some(b"v20".to_vec()));
    assert_eq!(store.get(b"k30").unwrap(), None);
}

#[test]
fn store_stake() {
    // create State
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "findora_db".to_string(),
        VER_WINDOW,
    )));
    let mut check = State::new(cs, true);
    let mut store = StakeStore::new("stake", &mut check);

    // default stake MUST be zero
    assert_eq!(store.get_stake("fra1111").unwrap(), 0);
    assert_eq!(store.get_pool().unwrap(), 0);

    // stake some coins
    store.stake("fra1111", 100).unwrap();
    store.stake("fra2222", 200).unwrap();
    store.stake("fra3333", 300).unwrap();
    store.stake("fra4455", 400).unwrap();

    // check stakes
    assert_eq!(store.get_stake("fra1111").unwrap(), 100);
    assert_eq!(store.get_stake("fra2222").unwrap(), 200);
    assert_eq!(store.get_stake("fra3333").unwrap(), 300);
    assert_eq!(store.get_stake("fra4455").unwrap(), 400);
    assert_eq!(store.get_pool().unwrap(), 1000);
    store.state_mut().commit(1).unwrap();

    // stake more
    store.stake("fra1111", 10).unwrap();
    store.stake("fra2222", 20).unwrap();
    store.stake("fra3333", 30).unwrap();
    store.stake("fra4455", 40).unwrap();

    // check stakes again
    assert_eq!(store.get_stake("fra1111").unwrap(), 110);
    assert_eq!(store.get_stake("fra2222").unwrap(), 220);
    assert_eq!(store.get_stake("fra3333").unwrap(), 330);
    assert_eq!(store.get_stake("fra4455").unwrap(), 440);
    assert_eq!(store.get_pool().unwrap(), 1100);
}

#[test]
fn store_unstake() {
    // create State
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "findora_db".to_string(),
        VER_WINDOW,
    )));
    let mut check = State::new(cs, true);
    let mut store = StakeStore::new("stake", &mut check);

    // stake some coins
    store.stake("fra1111", 100).unwrap();
    store.stake("fra2222", 200).unwrap();
    store.stake("fra3333", 300).unwrap();
    store.stake("fra4455", 400).unwrap();
    store.state_mut().commit(1).unwrap();

    // unstake
    store.unstake("fra1111", 10).unwrap();
    store.unstake("fra2222", 20).unwrap();
    store.unstake("fra3333", 30).unwrap();
    store.unstake("fra4455", 40).unwrap();

    // check stakes again
    assert_eq!(store.get_stake("fra1111").unwrap(), 90);
    assert_eq!(store.get_stake("fra2222").unwrap(), 180);
    assert_eq!(store.get_stake("fra3333").unwrap(), 270);
    assert_eq!(store.get_stake("fra4455").unwrap(), 360);
    assert_eq!(store.get_pool().unwrap(), 900);
}

#[test]
fn store_stake_unstake_too_fast() {
    // create State
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "findora_db".to_string(),
        VER_WINDOW,
    )));
    let mut check = State::new(cs, true);
    let mut store = StakeStore::new("stake", &mut check);

    // stake some coins at height 1
    store.stake("fra1111", 100).unwrap();
    assert!(store.stake("fra1111", 150).is_err()); // MUST fail
    store.stake("fra2222", 200).unwrap();
    store.stake("fra3333", 300).unwrap();
    assert!(store.stake("fra3333", 250).is_err()); // MUST fail
    store.stake("fra4455", 400).unwrap();
    store.state_mut().commit(1).unwrap();

    // check stakes after commit
    assert_eq!(store.get_stake("fra1111").unwrap(), 100);
    assert_eq!(store.get_stake("fra2222").unwrap(), 200);
    assert_eq!(store.get_stake("fra3333").unwrap(), 300);
    assert_eq!(store.get_stake("fra4455").unwrap(), 400);
    assert_eq!(store.get_pool().unwrap(), 1000);

    // unstake some coins at height 2
    store.unstake("fra1111", 10).unwrap();
    store.unstake("fra2222", 20).unwrap();
    assert!(store.unstake("fra2222", 20).is_err()); // MUST fail
    store.unstake("fra3333", 30).unwrap();
    store.unstake("fra4455", 40).unwrap();
    assert!(store.unstake("fra4455", 40).is_err()); // MUST fail

    // check stakes without commit
    assert_eq!(store.get_stake("fra1111").unwrap(), 90);
    assert_eq!(store.get_stake("fra2222").unwrap(), 180);
    assert_eq!(store.get_stake("fra3333").unwrap(), 270);
    assert_eq!(store.get_stake("fra4455").unwrap(), 360);
    assert_eq!(store.get_pool().unwrap(), 900);
}

#[test]
fn store_iter_db() {
    // create State
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "findora_db".to_string(),
        VER_WINDOW,
    )));
    let mut check = State::new(cs, true);
    let mut store = StakeStore::new("stake", &mut check);

    // stake some coins
    store.stake("fra1111", 100).unwrap();
    store.stake("fra2222", 200).unwrap();
    store.stake("fra3333", 300).unwrap();
    store.stake("fra4455", 400).unwrap();

    // commit block 1
    let (hash1, height) = store.state_mut().commit(1).unwrap();
    assert_eq!(height, 1);

    // unstake some coins in session of block 2
    store.unstake("fra1111", 10).unwrap();
    store.unstake("fra2222", 20).unwrap();
    store.unstake("fra3333", 300).unwrap();
    assert!(store.delete(store.stake_key("fra3333").as_ref()).is_ok());
    store.unstake("fra4455", 40).unwrap();

    // check stakes before committing block 2
    let expected = vec![
        (b"stake_validator_fra1111".to_vec(), 100_u64),
        (b"stake_validator_fra2222".to_vec(), 200_u64),
        (b"stake_validator_fra3333".to_vec(), 300_u64),
        (b"stake_validator_fra4455".to_vec(), 400_u64),
    ];
    let pfx_v = store.prefix().push(b"validator");
    let mut actual = vec![];
    store.iter_db(pfx_v, true, &mut |(k, v)| {
        let amt = StakeStore::<TempFinDB>::from_vec::<u64>(&v);
        actual.push((k, amt.unwrap()));
        false
    });
    assert_eq!(actual, expected);
    assert_eq!(store.get_pool().unwrap(), 630);

    // commit block 2
    let (hash2, height) = store.state_mut().commit(2).unwrap();
    assert_eq!(height, 2);
    assert_ne!(hash1, hash2);

    // check stakes after committing block 2
    let expected = vec![
        (b"stake_validator_fra1111".to_vec(), 90_u64),
        (b"stake_validator_fra2222".to_vec(), 180_u64),
        (b"stake_validator_fra4455".to_vec(), 360_u64),
    ];
    let pfx_v = store.prefix().push(b"validator");
    let mut actual = vec![];
    store.iter_db(pfx_v, true, &mut |(k, v)| {
        let amt = StakeStore::<TempFinDB>::from_vec::<u64>(&v);
        actual.push((k, amt.unwrap()));
        false
    });
    assert_eq!(actual, expected);
    assert_eq!(store.get_pool().unwrap(), 630);
}
#[test]
fn store_iter_cur() {
    // create State
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "findora_db".to_string(),
        VER_WINDOW,
    )));
    let mut check = State::new(cs, true);
    let mut store = StakeStore::new("stake", &mut check);

    // stake some coins and commit block 1
    store.stake("fra1111", 100).unwrap();
    store.state_mut().commit(1).unwrap();

    // stake, unstake, delete and commit tx session
    store.unstake("fra1111", 100).unwrap();
    assert!(store.delete(store.stake_key("fra1111").as_ref()).is_ok());
    store.stake("fra2222", 200).unwrap();
    store.stake("fra3333", 300).unwrap();
    let _res = store.state_mut().commit_session();

    // stake, unstake again
    store.stake("fra44555", 400).unwrap();
    store.stake("fra55667", 500).unwrap();

    // check cached stakes before committing tx session
    let kvs: Vec<_> = store.iter_cur(store.prefix()).collect();
    let expected = vec![
        (b"stake_pool".to_vec(), b"1400".to_vec()),
        (b"stake_validator_fra2222".to_vec(), b"200".to_vec()),
        (b"stake_validator_fra3333".to_vec(), b"300".to_vec()),
        (b"stake_validator_fra44555".to_vec(), b"400".to_vec()),
        (b"stake_validator_fra55667".to_vec(), b"500".to_vec()),
    ];
    assert_eq!(kvs, expected);

    // check cached stakes after committing tx session
    let _res = store.state_mut().commit_session();
    let kvs: Vec<_> = store.iter_cur(store.prefix()).collect();
    assert_eq!(kvs, expected);
}

#[test]
fn store_threading() {
    // create State
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "findora_db".to_string(),
        VER_WINDOW,
    )));
    let mut state = State::new(cs.clone(), true);
    let mut store = StakeStore::new("stake", &mut state);

    // stake initial coins and commit
    let validators = Arc::new(vec![
        "fra1111".to_owned(),
        "fra2222".to_owned(),
        "fra3333".to_owned(),
        "fra4444".to_owned(),
        "fra5555".to_owned(),
    ]);
    for v in validators.iter() {
        store.stake(v, 1000000).unwrap();
    }
    let (hash, height) = store.state_mut().commit(1).unwrap();
    assert_eq!(1, height);

    // read/write times for each thread
    let times = 2500;

    // thread 1: query
    let mut threads = vec![];
    let vldts_1 = validators.clone();
    let cs_1 = cs.clone();
    threads.push(thread::spawn(move || {
        // create store
        let mut query = State::new(cs_1.clone(), true);
        let store = StakeStore::new("stake", &mut query);

        // starts
        let mut rng = rand::thread_rng();
        let mut reads = 0;
        while reads < times {
            thread::sleep(time::Duration::from_micros(1));

            // read height and get a random validator's staking amount
            let height = store.height().unwrap();
            let addr = &vldts_1[rng.gen_range(0..5)];
            let amount = store.get_stake(addr).unwrap();
            let amount_pool = store.get_pool().unwrap();
            assert!(height >= 1);
            // rough check
            assert!(amount_pool > amount);
            reads += 1;
        }
    }));

    // thread 2: query
    let cs_2 = cs.clone();
    threads.push(thread::spawn(move || {
        // create store
        let mut query = State::new(cs_2.clone(), true);
        let store = StakeStore::new("stake", &mut query);

        let mut reads = 0;
        while reads < times {
            thread::sleep(time::Duration::from_micros(1));

            // iterates commited staking amounts
            let mut total = 0_u64;
            let pfx_v = store.prefix().push(b"validator");
            store.iter_db(pfx_v, true, &mut |(_, v)| {
                let amt = StakeStore::<TempFinDB>::from_vec::<u64>(&v);
                total += amt.unwrap();
                false
            });
            let height = store.height().unwrap();
            let amount_pool = store.get_pool().unwrap();
            assert!(height >= 1);
            assert!(total > 0);
            assert!(amount_pool > 0);
            reads += 1;
        }
    }));

    // thread 3: check_tx
    let cs_3 = cs.clone();
    threads.push(thread::spawn(move || {
        // create store
        let mut check = State::new(cs_3.clone(), true);
        let store = StakeStore::new("stake", &mut check);

        let mut reads = 0;
        while reads < times {
            thread::sleep(time::Duration::from_micros(1));

            // iterates commited staking amounts
            let mut total = 0_u64;
            let pfx_v = store.prefix().push(b"validator");
            for (_, v) in store.iter_cur(pfx_v) {
                let amt = StakeStore::<TempFinDB>::from_vec::<u64>(&v);
                total += amt.unwrap();
            }
            let height = store.height().unwrap();
            let amount_pool = store.get_pool().unwrap();
            assert!(height >= 1);
            assert!(total > 0);
            assert!(amount_pool > 0);
            reads += 1;
        }
    }));

    // thread 4: deliver_tx
    let vldts_2 = validators;
    let cs_4 = cs;
    threads.push(thread::spawn(move || {
        // create store
        let mut deliver = State::new(cs_4.clone(), true);
        let mut store = StakeStore::new("stake", &mut deliver);

        // starts
        let mut rng = rand::thread_rng();
        let mut hash = store.state().root_hash();
        let mut height = store.height().unwrap();

        // commit blocks [2...times]
        while height < times {
            thread::sleep(time::Duration::from_micros(1));

            // random validator stake/unstake random amount
            let addr = &vldts_2[rng.gen_range(0..5)];
            let amt = rng.gen_range(1..=100);
            if rng.gen_bool(0.5) {
                assert!(store.stake(addr, amt).is_ok());
            } else {
                assert!(store.unstake(addr, amt).is_ok());
            }
            height += 1;
            let (hash_new, height_new) = store.state_mut().commit(height).unwrap();
            assert_eq!(height, height_new);
            assert_ne!(hash, hash_new);
            hash = hash_new;
        }
    }));

    // join 4 child threads
    for t in threads {
        let _ = t.join();
    }

    // checks height, hash
    assert_eq!(times, store.height().unwrap());
    assert_ne!(hash, store.state().root_hash());

    // check amounts
    let mut total = 0_u64;
    let pfx_v = store.prefix().push(b"validator");
    store.iter_db(pfx_v, true, &mut |(_, v)| {
        let amt = StakeStore::<TempFinDB>::from_vec::<u64>(&v);
        total += amt.unwrap();
        false
    });
    let amount_pool = store.get_pool().unwrap();
    assert_eq!(total, amount_pool);
}

#[test]
fn test_prefixed_store() {
    // create State
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "findora_db".to_string(),
        VER_WINDOW,
    )));
    let mut state = State::new(cs, true);
    let mut store = PrefixedStore::new("testStore", &mut state);

    store.set(b"validator_fra2221", b"200".to_vec()).unwrap();
    store.set(b"validator_fra2222", b"300".to_vec()).unwrap();
    store.set(b"validator_fra2223", b"500".to_vec()).unwrap();

    assert_eq!(
        store.get(b"validator_fra2221").unwrap(),
        Some(b"200".to_vec())
    );

    let (_, _) = store.state_mut().commit(12).unwrap();

    assert_eq!(
        store.get(b"validator_fra2222").unwrap(),
        Some(b"300".to_vec())
    );

    store.set(b"validator_fra2224", b"700".to_vec()).unwrap();
    let prefix = Prefix::new(b"validator");

    let res_iter = store.iter_cur(prefix);
    let list: Vec<_> = res_iter.collect();

    assert_eq!(list.len(), 4);
    assert_eq!(
        store.get(b"validator_fra2224").unwrap(),
        Some(b"700".to_vec())
    );
    assert!(store.exists(b"validator_fra2224").unwrap());

    let _ = store.delete(b"validator_fra2224");
    assert_eq!(store.get(b"validator_fra2224").unwrap(), None);
    assert!(!store.exists(b"validator_fra2224").unwrap());

    let (_, _) = store.state_mut().commit(13).unwrap();
    assert_eq!(
        store.get(b"validator_fra2221").unwrap(),
        Some(b"200".to_vec())
    );
    assert!(store.exists(b"validator_fra2221").unwrap());
}

/// create chain state of `FinDB`
fn gen_cs(path: String) -> Arc<RwLock<ChainState<TempFinDB>>> {
    let fdb = TempFinDB::open(path).expect("failed to open findb");
    let cs = ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);
    Arc::new(RwLock::new(cs))
}

/// create chain state of `RocksDB`
fn gen_cs_rocks(path: String) -> Arc<RwLock<ChainState<TempRocksDB>>> {
    let fdb = TempRocksDB::open(path).expect("failed to open rocksdb");
    let cs = ChainState::new(fdb, "test_db".to_string(), 0);
    Arc::new(RwLock::new(cs))
}

fn test_get_impl<D: MerkleDB>(cs: Arc<RwLock<ChainState<D>>>) {
    //Setup
    let mut state = State::new(cs.clone(), true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_delegator_1", b"v20".to_vec()).unwrap();

    //Get the values
    assert_eq!(
        state.get(b"prefix_validator_1").unwrap(),
        Some(b"v10".to_vec())
    );
    assert_eq!(
        state.get(b"prefix_delegator_1").unwrap(),
        Some(b"v20".to_vec())
    );
    assert_eq!(state.get(b"prefix_validator_2").unwrap(), None);

    //Commit and create new state - Simulate new block
    let _res = state.commit(89);
    state = State::new(cs, true);

    //Should get this value from the chain state as the state cache is empty
    assert_eq!(
        state.get(b"prefix_validator_1").unwrap(),
        Some(b"v10".to_vec())
    );
}

#[test]
fn test_get() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs(path);
    test_get_impl(cs);
}

#[test]
fn test_get_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);
    test_get_impl(cs);
}

fn test_exists_impl<D: MerkleDB>(cs: Arc<RwLock<ChainState<D>>>) {
    //Setup
    let mut state = State::new(cs, true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_delegator_1", b"v20".to_vec()).unwrap();

    //Get the values
    assert!(state.exists(b"prefix_validator_1").unwrap());
    assert!(state.exists(b"prefix_delegator_1").unwrap());
    assert!(!state.exists(b"prefix_validator_2").unwrap());

    //Commit and create new state - Simulate new block
    let _res = state.commit(89);

    //Should get this value from the chain state as the state cache is empty
    assert!(state.exists(b"prefix_validator_1").unwrap());
}

#[test]
fn test_exists() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs(path);
    test_exists_impl(cs);
}

#[test]
fn test_exists_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);
    test_exists_impl(cs);
}

fn test_set_impl<D: MerkleDB>(cs: Arc<RwLock<ChainState<D>>>) {
    //Setup
    let mut state = State::new(cs, true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_delegator_1", b"v20".to_vec()).unwrap();

    //Get the values
    assert_eq!(
        state.get(b"prefix_validator_1").unwrap(),
        Some(b"v10".to_vec())
    );
    assert_eq!(
        state.get(b"prefix_delegator_1").unwrap(),
        Some(b"v20".to_vec())
    );
    assert_eq!(state.get(b"prefix_validator_2").unwrap(), None);
}

#[test]
fn test_set() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs(path);
    test_set_impl(cs);
}

#[test]
fn test_set_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);
    test_set_impl(cs);
}

#[test]
fn test_set_big_kv_checked() {
    // Setup
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "test_db".to_string(),
        VER_WINDOW,
    )));
    let mut state = State::new(cs, true);

    // Set maximum valid key and value
    let max_key = "k".repeat(u8::MAX as usize).as_bytes().to_vec();
    let max_val = "v".repeat(u16::MAX as usize).as_bytes().to_vec();
    state.set(&max_key, max_val.clone()).unwrap();
    assert_eq!(state.get(&max_key).unwrap(), Some(max_val));

    // Set invalid key and value
    let big_key = "k".repeat(u8::MAX as usize + 1).as_bytes().to_vec();
    let big_val = "v".repeat(u16::MAX as usize + 1).as_bytes().to_vec();
    assert!(state.set(&big_key, b"v10".to_vec()).is_err());
    assert!(state.set(b"key10", big_val).is_err());
    assert_eq!(state.get(&big_key).unwrap(), None);
    assert_eq!(state.get(b"key10").unwrap(), None);
}

#[test]
#[should_panic]
fn test_set_big_key_unchecked_panic() {
    // Setup
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "test_db".to_string(),
        VER_WINDOW,
    )));
    let mut state = State::new(cs, true);

    // Set maximum valid key and value
    let max_key = "k".repeat(u8::MAX as usize).as_bytes().to_vec();
    let max_val = "v".repeat(u16::MAX as usize).as_bytes().to_vec();
    state.set(&max_key, max_val.clone()).unwrap();
    assert_eq!(state.get(&max_key).unwrap(), Some(max_val));

    // Set a big key
    let big_key = "k".repeat(u8::MAX as usize + 1).as_bytes().to_vec();
    assert!(state.set(&big_key, b"v10".to_vec()).is_ok());

    // Panic on commit
    state.commit(1).unwrap();
}

#[test]
#[should_panic]
fn test_set_big_value_unchecked_panic() {
    // Setup
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "test_db".to_string(),
        VER_WINDOW,
    )));
    let mut state = State::new(cs, true);

    // Set a big value
    let big_val = "v".repeat(u16::MAX as usize + 1).as_bytes().to_vec();
    assert!(state.set(b"key10", big_val).is_ok());

    // Panic on commit
    state.commit(1).unwrap();
}

#[test]
fn test_rocksdb_set_big_value_unchecked() {
    // Setup
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);

    // Make sure is_merkle flag is false
    let mut state = State::new(cs, false);

    // Set a big value
    let big_val = "v".repeat(u16::MAX as usize + 1).as_bytes().to_vec();
    assert!(state.set(b"key10", big_val).is_ok());

    // commit
    state.commit(1).unwrap();
}

#[test]
fn test_rocksdb_set_big_key_unchecked() {
    // Setup
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);

    // Make sure is_merkle flag is false
    let mut state = State::new(cs, false);

    // Set maximum valid key and value
    let max_key = "k".repeat(u8::MAX as usize).as_bytes().to_vec();
    let max_val = "v".repeat(u16::MAX as usize).as_bytes().to_vec();
    state.set(&max_key, max_val.clone()).unwrap();
    assert_eq!(state.get(&max_key).unwrap(), Some(max_val));

    // Set a big key
    let big_key = "k".repeat(u8::MAX as usize + 1).as_bytes().to_vec();
    assert!(state.set(&big_key, b"v10".to_vec()).is_ok());

    // Panic on commit
    state.commit(1).unwrap();
}

fn test_delete_impl<D: MerkleDB>(cs: Arc<RwLock<ChainState<D>>>) {
    //Setup
    let mut state = State::new(cs, true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_2", b"v20".to_vec()).unwrap();
    state.set(b"prefix_validator_3", b"v30".to_vec()).unwrap();

    //Get the values
    assert_eq!(
        state.get(b"prefix_validator_1").unwrap(),
        Some(b"v10".to_vec())
    );
    assert_eq!(
        state.get(b"prefix_validator_2").unwrap(),
        Some(b"v20".to_vec())
    );
    assert_eq!(
        state.get(b"prefix_validator_3").unwrap(),
        Some(b"v30".to_vec())
    );

    // ----------- Commit and clear cache - Simulate new block -----------
    let _res = state.commit(89);

    //Should get this value from the chain state as the state cache is empty
    assert_eq!(
        state.get(b"prefix_validator_1").unwrap(),
        Some(b"v10".to_vec())
    );

    state.set(b"prefix_validator_4", b"v40".to_vec()).unwrap();
    let _res = state.delete(b"prefix_validator_4");

    println!(
        "test_delete Batch after delete: {:?}",
        state.cache_mut().commit()
    );

    //Delete key from chain state
    let _res2 = state.delete(b"prefix_validator_3");

    // ----------- Commit and clear cache - Simulate new block -----------
    let _res1 = state.commit(90);

    //Should get this value from the chain state as the state cache is empty
    assert_eq!(
        state.get(b"prefix_validator_1").unwrap(),
        Some(b"v10".to_vec())
    );
    assert_eq!(
        state.get(b"prefix_validator_2").unwrap(),
        Some(b"v20".to_vec())
    );
    assert_eq!(state.get(b"prefix_validator_3").unwrap(), None);
}

#[test]
fn test_delete() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs(path);
    test_delete_impl(cs);
}

#[test]
fn test_delete_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);
    test_delete_impl(cs);
}

fn test_get_deleted_impl<D: MerkleDB>(cs: Arc<RwLock<ChainState<D>>>) {
    let mut state = State::new(cs, true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_2", b"v20".to_vec()).unwrap();
    state.set(b"prefix_validator_3", b"v30".to_vec()).unwrap();

    let _res = state.commit(89);

    //Should detect the key as deleted from the cache and return None without querying db
    let _res2 = state.delete(b"prefix_validator_3");
    assert_eq!(state.get(b"prefix_validator_3").unwrap(), None);
}

#[test]
fn test_get_deleted() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs(path);
    test_get_deleted_impl(cs);
}

#[test]
fn test_get_deleted_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);
    test_get_deleted_impl(cs);
}

#[test]
fn test_commit() {
    //Setup
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs(path);
    let mut state = State::new(cs, true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_2", b"v10".to_vec()).unwrap();

    //Commit state to db
    let (app_hash1, height1) = state.commit(90).unwrap();

    //Modify a value in the db
    state.set(b"prefix_validator_2", b"v20".to_vec()).unwrap();
    assert_eq!(height1, 90);

    //Commit state to db
    let (app_hash2, height2) = state.commit(91).unwrap();

    //Root hashes must be different
    assert_ne!(app_hash1, app_hash2);
    assert_eq!(height2, 91);

    //Commit state to db
    let (app_hash3, height3) = state.commit(92).unwrap();

    // Root hashes must be equal
    assert_eq!(app_hash2, app_hash3);
    assert_eq!(height3, 92)
}

#[test]
fn test_commit_rocks() {
    //Setup
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);
    let mut state = State::new(cs, true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_2", b"v10".to_vec()).unwrap();

    //Commit state to db
    let (app_hash1, height1) = state.commit(90).unwrap();

    //Modify a value in the db
    state.set(b"prefix_validator_2", b"v20".to_vec()).unwrap();
    assert_eq!(height1, 90);

    //Commit state to db
    let (app_hash2, height2) = state.commit(91).unwrap();

    //Root hashes must be different
    assert_eq!(app_hash1, app_hash2);
    assert_eq!(height2, 91);

    //Commit state to db
    let (app_hash3, height3) = state.commit(92).unwrap();

    // Root hashes must be equal
    assert_eq!(app_hash2, app_hash3);
    assert_eq!(height3, 92)
}

#[test]
fn test_root_hash() {
    //Setup
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let cs = Arc::new(RwLock::new(ChainState::new(
        fdb,
        "test_db".to_string(),
        VER_WINDOW,
    )));
    let mut state = State::new(cs, true);

    //Set some kv pairs
    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_2", b"v10".to_vec()).unwrap();

    //Commit state to db
    let (app_hash1, _height) = state.commit(90).unwrap();

    //Modify a value in the db
    state.set(b"prefix_validator_2", b"v20".to_vec()).unwrap();

    //Commit state to db
    let (app_hash2, _height) = state.commit(91).unwrap();

    //Root hashes must be different
    assert_ne!(app_hash1, app_hash2);

    //Commit state to db
    let (app_hash3, _height) = state.commit(92).unwrap();

    // Root hashes must be equal
    assert_eq!(app_hash2, app_hash3);
}

fn test_iterate_impl<D: MerkleDB>(cs: Arc<RwLock<ChainState<D>>>) {
    let mut state = State::new(cs, true);
    let mut count = 0;

    state.set(b"prefix_validator_1", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_2", b"v10".to_vec()).unwrap();
    state.set(b"prefix_3", b"v10".to_vec()).unwrap();
    state.set(b"prefix_4", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_5", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_6", b"v10".to_vec()).unwrap();
    state.set(b"prefix_7", b"v10".to_vec()).unwrap();
    state.set(b"prefix_validator_8", b"v10".to_vec()).unwrap();

    // ----------- Commit state to db and clear cache -----------
    let res1 = state.commit(55).unwrap();
    assert_eq!(res1.1, 55);

    let mut func_iter = |entry: KValue| {
        println!("Key: {:?}, Value: {:?}", entry.0, entry.1);
        count += 1;
        false
    };
    state.iterate(
        &b"prefix_validator_".to_vec(),
        &b"prefix_validator~".to_vec(),
        IterOrder::Asc,
        &mut func_iter,
    );
    assert_eq!(count, 5);
}

#[test]
fn test_iterate() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs(path);
    test_iterate_impl(cs);
}

#[test]
fn test_iterate_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let cs = gen_cs_rocks(path);
    test_iterate_impl(cs);
}
