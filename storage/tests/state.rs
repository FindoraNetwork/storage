use fin_db::FinDB;
use mem_db::MemoryDB;
use parking_lot::RwLock;
use rand::Rng;
use std::{sync::Arc, thread};
use storage::{
    db::{IterOrder, KVBatch, KValue, MerkleDB},
    state::{ChainState, ChainStateOpts, State},
    store::Prefix,
};
use temp_db::{TempFinDB, TempRocksDB};

const VER_WINDOW: u64 = 100;

/// create chain state of `FinDB`
fn gen_cs(path: String) -> ChainState<TempFinDB> {
    let fdb = TempFinDB::open(path).expect("failed to open findb");
    ChainState::new(fdb, "test_db".to_string(), VER_WINDOW)
}

/// create chain state of `RocksDB`
fn gen_cs_rocks(path: String) -> ChainState<TempRocksDB> {
    let fdb = TempRocksDB::open(path).expect("failed to open rocksdb");
    ChainState::new(fdb, "test_db".to_string(), 0)
}

/// create chain state of `RocksDB`
fn gen_cs_rocks_fresh(path: String) -> ChainState<TempRocksDB> {
    let fdb = TempRocksDB::open(path).expect("failed to open rocksdb");
    let opts = ChainStateOpts {
        name: Some("test_db".to_string()),
        ver_window: 0,
        interval: 0,
        cleanup_aux: true,
    };
    ChainState::create_with_opts(fdb, opts)
}

#[test]
fn test_new_chain_state() {
    let path = thread::current().name().unwrap().to_owned();
    let _cs = gen_cs(path);
}

#[test]
fn test_new_chain_state_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let _cs = gen_cs_rocks(path);
}

fn test_get_impl<D: MerkleDB>(mut cs: ChainState<D>) {
    // commit data
    cs.commit(
        vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ],
        25,
        true,
    )
    .unwrap();

    // verify
    assert_eq!(cs.get(&b"k10".to_vec()).unwrap(), Some(b"v10".to_vec()));
    assert_eq!(cs.get(&b"k20".to_vec()).unwrap(), Some(b"v20".to_vec()));
    assert_eq!(cs.get(&b"kN/A".to_vec()).unwrap(), None);
    assert_eq!(cs.height().unwrap(), 25);
}

#[test]
fn test_get() {
    let path = thread::current().name().unwrap().to_owned();
    test_get_impl(gen_cs(path));
}

#[test]
fn test_get_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    test_get_impl(gen_cs_rocks(path));
}

fn test_iterate_impl<D: MerkleDB>(mut cs: ChainState<D>) {
    let batch = vec![
        (b"k10".to_vec(), Some(b"v10".to_vec())),
        (b"k20".to_vec(), Some(b"v20".to_vec())),
        (b"k30".to_vec(), Some(b"v30".to_vec())),
        (b"k40".to_vec(), Some(b"v40".to_vec())),
        (b"k50".to_vec(), Some(b"v50".to_vec())),
        (b"k60".to_vec(), Some(b"v60".to_vec())),
        (b"k70".to_vec(), Some(b"v70".to_vec())),
        (b"k80".to_vec(), Some(b"v80".to_vec())),
    ];
    let batch_clone = batch.clone();

    // commit data
    cs.commit(batch, 26, true).unwrap();

    //Create new Chain State with new database
    let mut index = 0;
    let mut func_iter = |entry: KValue| {
        println!("Key: {:?}, Value: {:?}", entry.0, entry.1);
        //Assert Keys are equal
        assert_eq!(entry.0, batch_clone[index].0);
        //Assert Values are equal
        assert_eq!(entry.1, batch_clone[index].1.clone().unwrap());

        index += 1;
        false
    };
    cs.iterate(
        &b"k10".to_vec(),
        &b"k81".to_vec(),
        IterOrder::Asc,
        &mut func_iter,
    );
}

#[test]
fn test_iterate() {
    let path = thread::current().name().unwrap().to_owned();
    test_iterate_impl(gen_cs(path));
}

#[test]
fn test_iterate_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    test_iterate_impl(gen_cs_rocks(path));
}

fn test_exists_impl<D: MerkleDB>(mut cs: ChainState<D>) {
    // commit data
    cs.commit(
        vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ],
        26,
        true,
    )
    .unwrap();

    // verify
    assert!(cs.exists(&b"k10".to_vec()).unwrap());
    assert!(cs.exists(&b"k20".to_vec()).unwrap());
    assert!(!cs.exists(&b"kN/A".to_vec()).unwrap());
}

#[test]
fn test_exists() {
    let path = thread::current().name().unwrap().to_owned();
    test_exists_impl(gen_cs(path));
}

#[test]
fn test_exists_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    test_exists_impl(gen_cs_rocks(path));
}

fn test_commit_impl<D: MerkleDB>(mut cs: ChainState<D>) {
    let batch = vec![
        (b"k10".to_vec(), Some(b"v10".to_vec())),
        (b"k20".to_vec(), Some(b"v20".to_vec())),
        (b"k30".to_vec(), Some(b"v30".to_vec())),
        (b"k40".to_vec(), Some(b"v40".to_vec())),
        (b"k50".to_vec(), Some(b"v50".to_vec())),
        (b"k60".to_vec(), Some(b"v60".to_vec())),
        (b"k70".to_vec(), Some(b"v70".to_vec())),
        (b"k80".to_vec(), Some(b"v80".to_vec())),
    ];
    let batch_clone = batch.clone();

    // Commit batch to db, in production the flush would be true
    let result = cs.commit(batch, 55, false).unwrap();
    assert_eq!(result.1, 55);

    let mut index = 0;
    let mut func_iter = |entry: KValue| {
        //Assert Keys are equal
        assert_eq!(entry.0, batch_clone[index].0);
        //Assert Values are equal
        assert_eq!(entry.1, batch_clone[index].1.clone().unwrap());

        index += 1;
        false
    };
    cs.iterate(
        &b"k10".to_vec(),
        &b"k81".to_vec(),
        IterOrder::Asc,
        &mut func_iter,
    );
}

#[test]
fn test_commit() {
    let path = thread::current().name().unwrap().to_owned();
    test_commit_impl(gen_cs(path));
}

#[test]
fn test_commit_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    test_commit_impl(gen_cs_rocks(path));
}

#[test]
fn test_aux_commit() {
    let path = thread::current().name().unwrap().to_owned();
    let mut cs = gen_cs(path);

    // commit data
    cs.commit(
        vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ],
        25,
        true,
    )
    .unwrap();

    let height_aux = cs.get_aux(&b"Height".to_vec()).unwrap();
    let height = cs.get(&b"Height".to_vec());

    //Make sure height was saved to auxiliary section of the db.
    assert_eq!(height_aux, Some(b"25".to_vec()));

    //Make sure the height isn't accessible from the main merkle tree.
    assert_ne!(height.unwrap(), Some(b"25".to_vec()));
}

#[test]
fn test_aux_commit_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    let mut cs = gen_cs_rocks(path);

    // commit data
    cs.commit(
        vec![
            (b"k10".to_vec(), Some(b"v10".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ],
        25,
        true,
    )
    .unwrap();

    let height_aux = cs.get_aux(&b"Height".to_vec()).unwrap();
    let height = cs.get(&b"Height".to_vec());

    //Make sure the height accessible from the db.
    assert_eq!(height.unwrap(), Some(b"25".to_vec()));

    //Make sure get() and get_aux do the SAME query.
    assert_eq!(height_aux, Some(b"25".to_vec()));
}

#[test]
fn test_root_hash() {
    let path = thread::current().name().unwrap().to_owned();
    let mut cs = gen_cs(path);

    let batch = vec![
        (b"k10".to_vec(), Some(b"v10".to_vec())),
        (b"k70".to_vec(), Some(b"v70".to_vec())),
        (b"k80".to_vec(), Some(b"v80".to_vec())),
    ];

    //Create new Chain State with new database
    let (root_hash1, _) = cs.commit(batch, 32, false).unwrap();

    let batch2 = vec![
        (b"k10".to_vec(), Some(b"v10".to_vec())),
        (b"k70".to_vec(), Some(b"v20".to_vec())),
        (b"k80".to_vec(), Some(b"v30".to_vec())),
    ];

    let (root_hash2, _) = cs.commit(batch2, 33, false).unwrap();
    assert_ne!(root_hash1, root_hash2);

    let (root_hash3, _) = cs.commit(vec![], 34, false).unwrap();
    assert_eq!(root_hash2, root_hash3);
}

#[test]
fn test_root_hash_same_kvs_diff_commits() {
    let path = thread::current().name().unwrap().to_owned();
    let mut cs = gen_cs(path.clone());

    let batch = vec![
        (b"k10".to_vec(), Some(b"v10".to_vec())),
        (b"k70".to_vec(), Some(b"v70".to_vec())),
    ];

    // commit 3 KVs in single commit
    let (root_hash1, _) = cs.commit(batch, 1, false).unwrap();

    // another chain state commit same KVs in 2 commits
    let path2 = format!("{}_2", path);
    let mut cs2 = gen_cs(path2);

    let batch2 = vec![(b"k10".to_vec(), Some(b"v11".to_vec()))];
    let batch3 = vec![
        (b"k10".to_vec(), Some(b"v10".to_vec())),
        (b"k70".to_vec(), Some(b"v70".to_vec())),
    ];

    let (root_hash2, _) = cs2.commit(batch2, 2, false).unwrap();
    let (root_hash3, _) = cs2.commit(batch3, 3, false).unwrap();

    // verify hashes
    assert_ne!(root_hash1, root_hash2);
    assert_ne!(root_hash2, root_hash3);
    assert_ne!(root_hash1, root_hash3);
}

fn test_height_impl<D: MerkleDB>(mut cs: ChainState<D>) {
    let batch = vec![
        (b"k10".to_vec(), Some(b"v10".to_vec())),
        (b"k70".to_vec(), Some(b"v70".to_vec())),
        (b"k80".to_vec(), Some(b"v80".to_vec())),
    ];

    // verify
    assert_eq!(cs.height().unwrap(), 0u64);

    let (_, _) = cs.commit(batch, 32, false).unwrap();
    assert_eq!(cs.height().unwrap(), 32);

    let batch = vec![(b"k10".to_vec(), Some(b"v60".to_vec()))];

    let (_, _) = cs.commit(batch, 33, false).unwrap();
    assert_eq!(cs.height().unwrap(), 33);

    let batch = vec![(b"k10".to_vec(), Some(b"v100".to_vec()))];

    let (_, _) = cs.commit(batch, 34, false).unwrap();
    assert_eq!(cs.height().unwrap(), 34);
}

#[test]
fn test_height() {
    let path = thread::current().name().unwrap().to_owned();
    test_height_impl(gen_cs(path));
}

#[test]
fn test_height_rocks() {
    let path = thread::current().name().unwrap().to_owned();
    test_height_impl(gen_cs_rocks(path));
}

#[test]
fn test_build_aux_batch() {
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let mut cs = ChainState::new(fdb, "test_db".to_string(), 10);

    let number_of_batches = 21;
    let batch_size = 7;

    //Create Several batches (More than Window size) with different keys and values
    for i in 1..number_of_batches {
        let mut batch: KVBatch = KVBatch::new();
        for j in 0..batch_size {
            let key = format!("key-{}", j);
            let val = format!("val-{}", i);
            batch.push((Vec::from(key), Some(Vec::from(val))));
        }

        //Commit the new batch
        let _ = cs.commit(batch, i as u64, false);

        //After each commit verify the values by using get_aux
        for k in 0..batch_size {
            let key = ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
            let value = format!("val-{}", i);
            assert_eq!(
                cs.get_aux(key.as_slice()).unwrap().unwrap(),
                value.as_bytes()
            )
        }
    }
}

#[test]
fn test_prune_aux_batch() {
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let mut cs = ChainState::new(fdb, "test_db".to_string(), 10);

    let number_of_batches = 21;
    let batch_size = 7;

    //Create Several batches (More than Window size) with different keys and values
    for i in 1..number_of_batches {
        let mut batch: KVBatch = KVBatch::new();
        for j in 0..batch_size {
            let key = format!("key-{}", j);
            let val = format!("val-{}", i);
            batch.push((Vec::from(key), Some(Vec::from(val))));
        }

        //Add a KV to the batch at a random height, 5 in this case
        if i == 5 {
            batch.push((b"random_key".to_vec(), Some(b"random-value".to_vec())));
        }

        //Add a KV to the batch at height 10
        if i == 10 {
            batch.push((b"random10_key".to_vec(), Some(b"random10-value".to_vec())));
        }

        //Commit the new batch
        let _ = cs.commit(batch, i as u64, false);

        //After each commit verify the values by using get_aux
        for k in 0..batch_size {
            let key = ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
            let value = format!("val-{}", i);
            assert_eq!(
                cs.get_aux(key.as_slice()).unwrap().unwrap().as_slice(),
                value.as_bytes()
            )
        }
    }

    //Make sure `random` key is found in the base.
    //Current height is 20, the ver_window size is 10, the versioned window is [10, 20]
    //And the `random` kv pair is inserted at height 5, it must be moved to the base.
    assert_eq!(
        cs.get_aux(ChainState::<TempFinDB>::base_key(b"random_key").as_slice())
            .unwrap(),
        Some(b"random-value".to_vec())
    );
    assert_eq!(
        cs.get_aux(ChainState::<TempFinDB>::versioned_key(b"random10_key", 10).as_slice())
            .unwrap(),
        Some(b"random10-value".to_vec())
    );

    //Query aux values that are older than the window size to confirm batches were pruned
    for i in 1..10 {
        for k in 0..batch_size {
            let key = ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
            assert_eq!(cs.get_aux(key.as_slice()).unwrap(), None,)
        }
    }
}

#[test]
fn test_height_internal_to_base() {
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let mut cs = ChainState::new(fdb, "test_db".to_string(), 100);

    let batch_size = 7;
    let mut batch: KVBatch = KVBatch::new();
    for j in 0..batch_size {
        let key = format!("key-{}", j);
        let val = format!("val-{}", j);
        batch.push((Vec::from(key), Some(Vec::from(val))));
    }

    let _ = cs.commit(batch, 10, false);
    for k in 0..batch_size {
        let key = ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), 10);
        let value = format!("val-{}", k);
        // println!("versioned_key：{:?}, versioned_value:{:?}", std::str::from_utf8(&key).unwrap(), value);
        assert_eq!(
            cs.get_aux(key.as_slice()).unwrap().unwrap().as_slice(),
            value.as_bytes()
        )
    }

    cs.height_internal_to_base(10).unwrap();
    for k in 0..batch_size {
        let key = ChainState::<TempFinDB>::base_key(format!("key-{}", k).as_bytes());
        let value = format!("val-{}", k);
        // println!("height_internal_to_base_key：{:?}", std::str::from_utf8(&key).unwrap());
        assert_eq!(
            cs.get_aux(key.as_slice()).unwrap().unwrap().as_slice(),
            value.as_bytes()
        )
    }
    assert_eq!(cs.get_aux(b"BaseHeight").unwrap().unwrap(), b"10".to_vec());
}

#[test]
fn test_build_state() {
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path).expect("failed to open db");
    let mut cs = ChainState::new(fdb, "test_db".to_string(), VER_WINDOW);

    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(10);
    for i in 0..10 {
        keys.push(format!("key_{}", i));
    }

    //Apply several batches with select few keys at random
    for h in 1..21 {
        let mut batch = KVBatch::new();

        //Build a random batch
        for _ in 0..5 {
            let rnd_key_idx = rng.gen_range(0..10);
            let rnd_val = format!("val-{}", rng.gen_range(0..10));
            batch.push((
                keys[rnd_key_idx].clone().into_bytes(),
                Some(rnd_val.into_bytes()),
            ));
        }

        let _ = cs.commit(batch, h, false);
    }

    //Confirm the build_state function produces the same keys and values as the latest state.
    let mut cs_batch = KVBatch::new();
    let bound = Prefix::new("key".as_bytes());
    cs.iterate(
        &bound.begin(),
        &bound.end(),
        IterOrder::Asc,
        &mut |(k, v)| -> bool {
            //Delete the key from aux db
            cs_batch.push((k, Some(v)));
            false
        },
    );

    let built_batch: Vec<_> = cs
        .build_state(20, Some(ChainState::<TempFinDB>::versioned_key_prefix(20)))
        .iter()
        .map(|(k, v)| {
            (
                ChainState::<TempFinDB>::get_raw_versioned_key(k)
                    .unwrap()
                    .into_bytes(),
                v.clone(),
            )
        })
        .collect();

    assert!(cs_batch.eq(&built_batch))
}

#[test]
fn test_clean_aux_db() {
    //Create new Chain State with new database
    let path = thread::current().name().unwrap().to_owned();
    let fdb = FinDB::open(path.clone()).expect("failed to open db");
    let mut cs = ChainState::new(fdb, "test_db".to_string(), 10);
    let number_of_batches = 21;
    let batch_size = 7;

    //Create Several batches (More than Window size) with different keys and values
    for i in 1..number_of_batches {
        let mut batch: KVBatch = KVBatch::new();
        for j in 0..batch_size {
            let key = format!("key-{}", j);
            let val = format!("val-{}", i);
            batch.push((Vec::from(key), Some(Vec::from(val))));
        }

        //Commit the new batch
        let _ = cs.commit(batch, i as u64, false);
    }

    //Open db with new chain-state - window half the size of the previous
    //Simulate Node restart
    std::mem::drop(cs);
    let new_window_size = 5;
    let fdb_new = TempFinDB::open(path).expect("failed to open db");
    let cs_new = ChainState::new(fdb_new, "test_db".to_string(), new_window_size);

    //Confirm keys older than new window size have been deleted
    for i in 1..(number_of_batches - new_window_size - 1) {
        for k in 0..batch_size {
            let key = ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
            assert_eq!(cs_new.get_aux(key.as_slice()).unwrap(), None)
        }
    }

    //Confirm keys within new window size still exist
    for i in (number_of_batches - new_window_size)..number_of_batches {
        for k in 0..batch_size {
            let key = ChainState::<TempFinDB>::versioned_key(format!("key-{}", k).as_bytes(), i);
            assert!(cs_new.exists_aux(key.as_slice()).unwrap())
        }
    }
}

#[test]
#[should_panic]
fn test_clean_aux() {
    // test FinDB
    let path_base = thread::current().name().unwrap().to_owned();
    let mut fin_path = path_base.clone();
    fin_path.push_str("fin");
    let mut fdb = FinDB::open(fin_path).unwrap();
    fdb.commit(vec![(b"k11".to_vec(), Some(b"v11".to_vec()))], false)
        .unwrap();
    assert_eq!(fdb.get_aux(b"k11").unwrap().unwrap(), b"v11".to_vec());
    fdb.clean_aux().unwrap();
    assert_eq!(fdb.get_aux(b"k11").unwrap(), None);

    // test TempFinDB
    let mut tfin_path = path_base.clone();
    tfin_path.push_str("tfin");
    let mut tfdb = TempFinDB::open(tfin_path).unwrap();
    tfdb.commit(vec![(b"k11".to_vec(), Some(b"v11".to_vec()))], false)
        .unwrap();
    assert_eq!(tfdb.get_aux(b"k11").unwrap().unwrap(), b"v11".to_vec());
    tfdb.clean_aux().unwrap();
    assert_eq!(tfdb.get_aux(b"k11").unwrap(), None);

    // // test RocksDB
    // let mut rocks_path = path_base.clone();
    // rocks_path.push_str("rocks");
    // let mut rdb = RocksDB::open(rocks_path).unwrap();
    // rdb.commit(vec![(b"k11".to_vec(), Some(b"v11".to_vec()))], false)
    //     .unwrap();
    // assert_eq!(rdb.get_aux(b"k11").unwrap().unwrap(), b"v11".to_vec());
    // rdb.clean_aux().unwrap();
    // assert_eq!(rdb.get_aux(b"k11").unwrap(), None);

    // // test TempRocksDB
    // let mut trocks_path = path_base.clone();
    // trocks_path.push_str("trocks");
    // let mut trdb = TempRocksDB::open(trocks_path).expect("failed to open db");
    // trdb.commit(vec![(b"k11".to_vec(), Some(b"v11".to_vec()))], false)
    //     .unwrap();
    // assert_eq!(trdb.get_aux(b"k11").unwrap().unwrap(), b"v11".to_vec());
    // trdb.clean_aux().unwrap();
    // assert_eq!(trdb.get_aux(b"k11").unwrap(), None);

    // test MemoryDB
    let mut mdb = MemoryDB::new();
    mdb.commit(vec![(b"height".to_vec(), Some(b"100".to_vec()))], false)
        .unwrap();
    assert_eq!(mdb.get_aux(b"height").unwrap().unwrap(), b"100".to_vec());
    mdb.clean_aux().unwrap();
    assert_eq!(mdb.get_aux(b"k11").unwrap(), None);

    // test ChainState on FinDB
    let mut cs_fn_path = path_base.clone();
    cs_fn_path.push_str("cs_fin");
    let mut cs_tfdb = gen_cs(cs_fn_path);
    cs_tfdb
        .commit(vec![(b"k10".to_vec(), Some(b"v10".to_vec()))], 25, true)
        .unwrap();
    assert_eq!(
        cs_tfdb.get_aux(&b"Height".to_vec()).unwrap(),
        Some(b"25".to_vec())
    );
    cs_tfdb.clean_aux().unwrap();
    assert_eq!(
        cs_tfdb.get_aux(&b"Height".to_vec()).unwrap(),
        Some(b"25".to_vec())
    );
    std::mem::drop(cs_tfdb);
    let mut cs_fin_path = path_base.clone();
    cs_fin_path.push_str("cs_fin");
    let _ = gen_cs_rocks_fresh(cs_fin_path);

    // // test ChainState on RocksDB
    // let mut cs_rocks_path = path_base.clone();
    // cs_rocks_path.push_str("cs_rocks");
    // let mut cs_rocks = gen_cs_rocks(cs_rocks_path);
    // cs_rocks
    //     .commit(vec![(b"k10".to_vec(), Some(b"v10".to_vec()))], 25, true)
    //     .unwrap();
    // assert_eq!(
    //     cs_rocks.get_aux(&b"Height".to_vec()).unwrap(),
    //     Some(b"25".to_vec())
    // );
    // std::mem::drop(cs_rocks);
    // let mut cs_rocks_path = path_base.clone();
    // cs_rocks_path.push_str("cs_rocks");
    // let cs_rocks = gen_cs_rocks_fresh(cs_rocks_path);
    // assert_eq!(cs_rocks.get_aux(&b"Height".to_vec()).unwrap(), None);
}

#[test]
fn test_get_ver() {
    //Create new Chain State with new database
    let path = thread::current().name().unwrap().to_owned();
    let mut cs = gen_cs(path);

    //Commit a single key at different heights and values
    for height in 1..21 {
        let mut batch = KVBatch::new();
        if height == 3 {
            batch.push((b"test_key".to_vec(), Some(b"test-val1".to_vec())));
        }
        if height == 4 {
            batch.push((b"test_key".to_vec(), Some(b"test-val4".to_vec())));
        }
        if height == 7 {
            //Deleted key at height 7
            batch.push((b"test_key".to_vec(), None));
        }
        if height == 15 {
            batch.push((b"test_key".to_vec(), Some(b"test-val2".to_vec())));
        }

        let _ = cs.commit(batch, height, false);
    }

    //Query the key at each version it was updated
    assert_eq!(
        cs.get_ver(b"test_key", 3).unwrap(),
        Some(b"test-val1".to_vec())
    );
    assert_eq!(
        cs.get_ver(b"test_key", 4).unwrap(),
        Some(b"test-val4".to_vec())
    );
    assert_eq!(cs.get_ver(b"test_key", 7).unwrap(), None);
    assert_eq!(
        cs.get_ver(b"test_key", 15).unwrap(),
        Some(b"test-val2".to_vec())
    );

    //Query the key between update versions
    assert_eq!(
        cs.get_ver(b"test_key", 5).unwrap(),
        Some(b"test-val4".to_vec())
    );
    assert_eq!(
        cs.get_ver(b"test_key", 17).unwrap(),
        Some(b"test-val2".to_vec())
    );
    assert_eq!(cs.get_ver(b"test_key", 10).unwrap(), None);

    //Query the key at a version it didn't exist
    assert_eq!(cs.get_ver(b"test_key", 2).unwrap(), None);

    //Query the key after it's been deleted
    assert_eq!(cs.get_ver(b"test_key", 8).unwrap(), None);
}

fn commit_n_snapshot(
    cs: &mut ChainState<TempFinDB>,
    path: String,
    height: u64,
    batch: KVBatch,
) -> (Vec<u8>, u64) {
    let (hash, h) = cs.commit(batch, height, true).unwrap();
    let path_cp = format!("{}_{}_snap", path, height);
    cs.snapshot(path_cp).unwrap();
    (hash, h)
}

fn compare_kv(l_cs: &ChainState<TempFinDB>, r_cs: &ChainState<TempFinDB>, key: &[u8]) {
    assert_eq!(l_cs.get_aux(key).unwrap(), r_cs.get_aux(key).unwrap())
}

fn export_n_compare(cs: &mut ChainState<TempFinDB>, path: String, height: u64) {
    // export
    let exp_path = format!("{}_{}_exp", path, height);
    let exp_fdb = TempFinDB::open(exp_path).expect("failed to open db export");
    let mut exp_cs = ChainState::new(exp_fdb, "test_db".to_string(), 5);
    cs.export(&mut exp_cs, height).unwrap();

    // open corresponding snapshot
    let snap_path = format!("{}_{}_snap", path, height);
    let snap_fdb = TempFinDB::open(snap_path).expect("failed to open db snapshot");
    let snap_cs = ChainState::new(snap_fdb, "test_db".to_string(), 5);

    // compare height and KVs.
    //We can't expect same root hash when state is built on truncated commit history
    assert_eq!(exp_cs.height().unwrap(), snap_cs.height().unwrap());
    assert_eq!(exp_cs.root_hash(), snap_cs.root_hash());
    compare_kv(&exp_cs, &snap_cs, b"k10");
    compare_kv(&exp_cs, &snap_cs, b"k20");
    compare_kv(&exp_cs, &snap_cs, b"k30");
    compare_kv(&exp_cs, &snap_cs, b"k40");
    compare_kv(&exp_cs, &snap_cs, b"k50");
    compare_kv(&exp_cs, &snap_cs, b"k60");
}

#[test]
fn test_snapshot() {
    //Create new Chain State with new database
    let path = thread::current().name().unwrap().to_owned();
    let fdb = TempFinDB::open(path.clone()).expect("failed to open db");
    let mut cs = ChainState::new(fdb, "test_db".to_string(), 5);

    // commit block 1 and take snapshot 1
    commit_n_snapshot(
        &mut cs,
        path.clone(),
        1,
        vec![(b"k10".to_vec(), Some(b"v10".to_vec()))],
    );

    // commit block 2 and take snapshot 2
    commit_n_snapshot(
        &mut cs,
        path.clone(),
        2,
        vec![
            (b"k10".to_vec(), Some(b"v11".to_vec())),
            (b"k20".to_vec(), Some(b"v20".to_vec())),
        ],
    );

    // commit block 3 and take snapshot 3
    commit_n_snapshot(
        &mut cs,
        path.clone(),
        3,
        vec![
            (b"k10".to_vec(), None),
            (b"k30".to_vec(), Some(b"v30".to_vec())),
        ],
    );

    // commit block 4 and take snapshot 4
    commit_n_snapshot(
        &mut cs,
        path.clone(),
        4,
        vec![
            (b"k10".to_vec(), Some(b"v13".to_vec())),
            (b"k20".to_vec(), None),
            (b"k30".to_vec(), Some(b"v36".to_vec())),
            (b"k40".to_vec(), Some(b"v41".to_vec())),
        ],
    );

    // commit block 5 and take snapshot 5
    commit_n_snapshot(
        &mut cs,
        path.clone(),
        5,
        vec![
            (b"k20".to_vec(), Some(b"v22".to_vec())),
            (b"k30".to_vec(), None),
            (b"k50".to_vec(), Some(b"v51".to_vec())),
        ],
    );

    // commit block 6 and take snapshot 6
    commit_n_snapshot(
        &mut cs,
        path.clone(),
        6,
        vec![
            (b"k20".to_vec(), None),
            (b"k30".to_vec(), Some(b"v37".to_vec())),
            (b"k60".to_vec(), Some(b"v64".to_vec())),
        ],
    );

    // export chain state on every height and compare
    export_n_compare(&mut cs, path.clone(), 1);
    export_n_compare(&mut cs, path.clone(), 2);
    export_n_compare(&mut cs, path.clone(), 3);
    export_n_compare(&mut cs, path.clone(), 4);
    export_n_compare(&mut cs, path.clone(), 5);
    export_n_compare(&mut cs, path.clone(), 6);

    // export on invalid heights
    let exp_path = format!("{}_exp", path);
    let exp_fdb = TempFinDB::open(exp_path).expect("failed to open db export");
    let mut snap_cs = ChainState::new(exp_fdb, "test_db".to_string(), 5);
    assert!(cs.export(&mut snap_cs, 0).is_err());
    assert!(cs.export(&mut snap_cs, 7).is_err());

    // clean up snapshot 1
    let snap_path_1 = format!("{}_{}_snap", path, 1);
    let _ = TempFinDB::open(snap_path_1).expect("failed to open db snapshot");
}

#[test]
fn test_state_at() {
    let fdb = TempFinDB::new().expect("failed to create fin db");
    let chain = Arc::new(RwLock::new(ChainState::new(fdb, "test_db".to_string(), 2)));
    let state = State::new(chain.clone(), true);

    assert!(chain
        .write()
        .commit(
            vec![
                (b"k10".to_vec(), Some(b"v110".to_vec())),
                (b"k20".to_vec(), Some(b"v120".to_vec())),
            ],
            1,
            true,
        )
        .is_ok());

    assert!(chain
        .write()
        .commit(
            vec![
                (b"k10".to_vec(), Some(b"v210".to_vec())),
                (b"k20".to_vec(), Some(b"v220".to_vec())),
            ],
            2,
            true,
        )
        .is_ok());

    let state_1 = state
        .state_at(1)
        .expect("failed to create state at height 1");

    let state_2 = state
        .state_at(2)
        .expect("failed to create state at height 2");

    assert!(chain
        .write()
        .commit(
            vec![
                (b"k10".to_vec(), Some(b"v310".to_vec())),
                (b"k20".to_vec(), Some(b"v320".to_vec())),
            ],
            3,
            true,
        )
        .is_ok());

    assert!(state_1
        .get(b"k10")
        .map_or(false, |v| v == Some(b"v110".to_vec())));

    drop(state_1);

    assert!(state
        .get(b"k10")
        .map_or(false, |v| v == Some(b"v310".to_vec())));

    assert!(state_2
        .get(b"k10")
        .map_or(false, |v| v == Some(b"v210".to_vec())));
    drop(state_2);

    assert!(chain
        .write()
        .commit(
            vec![
                (b"k10".to_vec(), Some(b"v410".to_vec())),
                (b"k20".to_vec(), Some(b"v420".to_vec())),
            ],
            4,
            true,
        )
        .is_ok());

    assert!(state
        .get_ver(b"k10", 1)
        .map_or(false, |v| v == Some(b"v110".to_vec())));
    assert!(state
        .get_ver(b"k10", 2)
        .map_or(false, |v| v == Some(b"v210".to_vec())));

    // Keys at height 2 are moved to base after this commit
    assert!(chain
        .write()
        .commit(
            vec![
                (b"k10".to_vec(), Some(b"v510".to_vec())),
                (b"k20".to_vec(), Some(b"v520".to_vec())),
            ],
            5,
            true,
        )
        .is_ok());

    // Keys at height 1 is in base now and override by height 2
    assert!(state.get_ver(b"k10", 1).is_err());
    assert!(state
        .get_ver(b"k10", 2)
        .map_or(false, |v| v == Some(b"v210".to_vec())));
}
