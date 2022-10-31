use fin_db::FinDB;
use std::{env::temp_dir, time::SystemTime};
use storage::db::MerkleDB;
use storage::state::{ChainState, ChainStateOpts};
use temp_db::TempFinDB;

#[test]
fn test_current_window() {
    let ver_window = 2;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window);

    assert!(chain.current_window().map(|t| t == (0, 0)).unwrap());

    assert!(chain.commit(vec![], 1, true).is_ok());
    assert!(chain.commit(vec![], 2, true).is_ok());

    assert!(chain.current_window().map(|t| t == (0, 2)).unwrap());
    assert!(chain.commit(vec![], 3, true).is_ok());
    assert!(chain.current_window().map(|t| t == (1, 3)).unwrap());
    assert!(chain.commit(vec![], 4, true).is_ok());
    assert!(chain.current_window().map(|t| t == (2, 4)).unwrap());

    assert!(chain.commit(vec![], 5, true).is_ok());
    assert!(chain.current_window().map(|t| t == (3, 5)).unwrap());
}

#[test]
fn test_pin_height() {
    let ver_window = 3;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window);
    assert!(chain.commit(vec![], 1, true).is_ok());
    assert!(chain.commit(vec![], 2, true).is_ok());
    assert!(chain.commit(vec![], 3, true).is_ok());
    assert!(chain.commit(vec![], 4, true).is_ok());
    assert!(chain.pin_at(1).is_ok());
    assert!(chain.pin_at(2).is_ok());
    assert_eq!(chain.current_pinned_height(), vec![1, 2]);

    assert!(chain.pin_at(1).is_ok());
    assert_eq!(chain.current_pinned_height(), vec![1, 2]);
    assert!(chain.pin_at(3).is_ok());
    assert_eq!(chain.current_pinned_height(), vec![1, 2, 3]);
}

#[test]
fn test_pin_extend_window() {
    let ver_window = 2;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window);

    assert!(chain.commit(vec![], 1, true).is_ok());
    assert!(chain.commit(vec![], 2, true).is_ok());
    assert!(chain.commit(vec![], 3, true).is_ok());
    assert!(chain.current_window().map(|t| t == (1, 3)).is_ok());

    assert!(chain.pin_at(1).is_ok());
    assert!(chain.commit(vec![], 4, true).is_ok());
    assert!(chain.current_window().map(|t| t == (1, 4)).is_ok());
}

#[test]
fn test_pin_height_error() {
    let ver_window = 1;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window);

    // future height
    assert!(chain.pin_at(1).is_err());
    assert!(chain.commit(vec![], 1, true).is_ok());
    assert!(chain.commit(vec![], 2, true).is_ok());
    assert!(chain.commit(vec![], 3, true).is_ok());
    // too old height
    assert!(chain.pin_at(1).is_err());
}

#[test]
fn test_unpin_height() {
    let ver_window = 1;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window);

    assert!(chain.commit(vec![], 1, true).is_ok());
    assert!(chain.commit(vec![], 2, true).is_ok());
    assert!(chain.pin_at(1).is_ok());
    assert!(chain.pin_at(1).is_ok());
    chain.unpin_at(1);
    assert_eq!(chain.current_pinned_height(), vec![1]);
    chain.unpin_at(1);
    assert_eq!(chain.current_pinned_height(), Vec::<u64>::new());
    assert!(chain.commit(vec![], 3, true).is_ok());
    assert!(chain.pin_at(2).is_ok());
    assert!(chain.pin_at(3).is_ok());
    chain.unpin_at(3);
    assert_eq!(chain.current_pinned_height(), vec![2]);
}

#[test]
fn test_unpin_shrink_window() {
    let ver_window = 2;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window);

    assert!(chain.commit(vec![], 1, true).is_ok());
    assert!(chain.commit(vec![], 2, true).is_ok());
    assert!(chain.commit(vec![], 3, true).is_ok());
    assert!(chain.pin_at(1).is_ok());
    assert!(chain.commit(vec![], 4, true).is_ok());
    chain.unpin_at(1);
    assert!(chain.current_window().map(|t| t == (1, 4)).is_ok());
    // next commit following unpin_at
    assert!(chain.commit(vec![], 5, true).is_ok());
    assert!(chain.current_window().map(|t| t == (3, 5)).is_ok());
}

#[test]
fn test_zero_window() {
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), 0);

    for h in 1..4 {
        assert!(chain.commit(vec![], h, true).is_ok());
    }

    assert!(chain.current_window().is_err());

    assert!(chain.pin_at(4).is_err());
}

#[test]
fn test_create_snapshot_1() {
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let opts = ChainStateOpts {
        name: Some("test".to_string()),
        ver_window: 10,
        snapshot_interval: 0,
        cleanup_aux: false,
    };
    let mut chain = ChainState::create_with_opts(fdb, opts);

    for h in 1..20 {
        assert!(chain.commit(vec![], h, true).is_ok());
        assert!(chain.get_snapshots_info().is_empty());
    }
}

#[test]
#[should_panic]
fn test_create_snapshot_2() {
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let opts = ChainStateOpts {
        name: Some("test".to_string()),
        ver_window: 10,
        snapshot_interval: 1,
        cleanup_aux: false,
    };
    let _ = ChainState::create_with_opts(fdb, opts);
}

#[test]
#[should_panic]
fn test_create_snapshot_2_1() {
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let opts = ChainStateOpts {
        name: Some("test".to_string()),
        ver_window: 0,
        snapshot_interval: 2,
        cleanup_aux: false,
    };
    let _ = ChainState::create_with_opts(fdb, opts);
}

#[test]
#[should_panic]
fn test_create_snapshot_2_2() {
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let opts = ChainStateOpts {
        name: Some("test".to_string()),
        ver_window: 3,
        snapshot_interval: 2,
        cleanup_aux: false,
    };
    let _ = ChainState::create_with_opts(fdb, opts);
}

#[test]
fn test_create_snapshot_3() {
    let ver_window = 12;
    let interval = 3;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let opts = ChainStateOpts {
        name: Some("test".to_string()),
        ver_window,
        snapshot_interval: interval,
        cleanup_aux: false,
    };
    let snapshot_created_at = interval.saturating_add(1);
    let snapshot_dropped_at = opts.ver_window.saturating_add(interval);
    let mut chain = ChainState::create_with_opts(fdb, opts);

    assert!(chain.get_snapshots_info().is_empty());

    for h in 0..snapshot_created_at {
        assert!(chain.commit(vec![], h, true).is_ok());
        assert!(chain.get_snapshots_info().is_empty());
    }

    for h in snapshot_created_at..snapshot_dropped_at {
        assert!(chain.commit(vec![], h, true).is_ok());

        let snapshots = chain.get_snapshots_info();
        let latest = snapshots.last().unwrap();
        assert_eq!(latest.end, h.saturating_sub(1) / interval * interval);
        assert_eq!(latest.count, 0);
        let first = snapshots.first().unwrap();
        assert_eq!(first.end, snapshot_created_at.saturating_sub(1));
    }

    for h in snapshot_dropped_at..20 {
        assert!(chain.commit(vec![], h, true).is_ok());

        let snapshots = chain.get_snapshots_info();
        let latest = snapshots.last().unwrap();
        assert_eq!(latest.end, h.saturating_sub(1) / interval * interval);

        let first = snapshots.first().unwrap();
        let min_height = chain.get_ver_range().unwrap().start;
        let mut snapshot_at = chain.last_snapshot_before(min_height).unwrap();
        if snapshot_at < min_height {
            // At this case, the snapshot at `snapshot_at` has been removed in last commit
            snapshot_at += interval;
        }
        assert_eq!(first.end, snapshot_at);
    }
}

#[test]
fn test_create_snapshot_3_1() {
    let ver_window = 21;
    let interval = 7;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let opts = ChainStateOpts {
        name: Some("test".to_string()),
        ver_window,
        snapshot_interval: interval,
        cleanup_aux: false,
    };

    let snapshot_dropped_at = opts.ver_window.saturating_add(interval);
    let mut chain = ChainState::create_with_opts(fdb, opts);

    for h in 0..snapshot_dropped_at {
        assert!(chain.commit(vec![], h, true).is_ok());
    }

    let height = snapshot_dropped_at.saturating_sub(ver_window);
    assert!(chain.pin_at(height).is_ok());

    // commit to create more snapshots
    for h in snapshot_dropped_at..100 {
        assert!(chain.commit(vec![], h, true).is_ok());
    }

    // pin first, then calculate oldest snapshot
    let last_snapshot = chain.oldest_snapshot().unwrap();
    let snapshots = chain.get_snapshots_info();
    let first = snapshots.first().unwrap();
    assert_eq!(first.end, last_snapshot);

    chain.unpin_at(height);
    // commit to remove snapshots
    assert!(chain.commit(vec![], 101, true).is_ok());
    let snapshots = chain.get_snapshots_info();
    let first = snapshots.first().unwrap();
    let snapshot_at = chain.oldest_snapshot().unwrap();
    assert_eq!(first.end, snapshot_at);
}

fn gen_cs(ver_window: u64, interval: u64) -> ChainState<TempFinDB> {
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let opts = ChainStateOpts {
        name: Some("test".to_string()),
        ver_window,
        snapshot_interval: interval,
        cleanup_aux: false,
    };
    ChainState::create_with_opts(fdb, opts)
}

fn apply_operations<DB: MerkleDB>(
    chain: &mut ChainState<DB>,
    operations: Vec<(u64, Option<Vec<u8>>)>,
    height_cap: u64,
) {
    let key = b"test_key".to_vec();
    let mut h = 0;
    for e in operations {
        while h < e.0 {
            chain.commit(vec![], h, false).unwrap();
            h += 1;
        }
        let batch = vec![(key.clone(), e.1)];
        chain.commit(batch, e.0, false).unwrap();
        h += 1;
    }

    while h < height_cap {
        chain.commit(vec![], h, false).unwrap();
        h += 1;
    }
}

fn verify_expectations<DB: MerkleDB>(
    chain: &ChainState<DB>,
    expectations: Vec<(u64, Option<Vec<u8>>)>,
) {
    for e in expectations {
        let val = match chain.get_ver(b"test_key", e.0) {
            Err(e) if e.to_string().contains("no versioning info") => None,
            Ok(v) => v,
            _ => panic!("failed!"),
        };
        if val != e.1 {
            println!(
                "Error at {} expect: {:?} actual: {:?}",
                e.0,
                e.1.as_ref().and_then(|v| String::from_utf8(v.clone()).ok()),
                val.as_ref().and_then(|v| String::from_utf8(v.clone()).ok())
            );
        }
        assert_eq!(val, e.1);
    }
}

#[test]
fn test_get_ver_with_snapshots() {
    let mut chain = gen_cs(110, 11);

    let operations = vec![
        (3, Some(b"test-val3".to_vec())),
        (4, Some(b"test-val4".to_vec())),
        (7, None),
        (15, Some(b"test-val15".to_vec())),
    ];
    apply_operations(&mut chain, operations, 50);

    let expectations = vec![
        (3, Some(b"test-val3".to_vec())),
        (4, Some(b"test-val4".to_vec())),
        (6, Some(b"test-val4".to_vec())),
        (7, None),
        (8, None),
        (10, None),
        (15, Some(b"test-val15".to_vec())),
        (20, Some(b"test-val15".to_vec())),
    ];
    verify_expectations(&chain, expectations);
}

#[test]
fn test_get_ver_with_snapshots_2() {
    for interval in 5..20 {
        let mut chain = gen_cs(interval * 10, interval);

        let operations = vec![
            (3, Some(b"test-val3".to_vec())),
            (4, Some(b"test-val4".to_vec())),
            (7, None),
            (15, Some(b"test-val15".to_vec())),
        ];
        apply_operations(&mut chain, operations, 50);

        let expectations = vec![
            (3, Some(b"test-val3".to_vec())),
            (4, Some(b"test-val4".to_vec())),
            (6, Some(b"test-val4".to_vec())),
            (7, None),
            (8, None),
            (10, None),
            (15, Some(b"test-val15".to_vec())),
            (20, Some(b"test-val15".to_vec())),
        ];
        verify_expectations(&chain, expectations);
    }
}

#[test]
fn test_get_ver_with_snapshots_3() {
    let mut chain = gen_cs(55, 11);

    let operations = vec![
        (3, Some(b"test-val3".to_vec())),
        (4, Some(b"test-val4".to_vec())),
        (60, None),
        (77, Some(b"test-val77".to_vec())),
    ];
    apply_operations(&mut chain, operations, 100);

    // min_height is 100 - 55 = 45
    let expectations = vec![
        (3, None),
        (4, None),                         // squashed in base
        (44, Some(b"test-val4".to_vec())), // in th base
        (45, Some(b"test-val4".to_vec())), // in th ver_window
        (60, None),
        (61, None),
        (77, Some(b"test-val77".to_vec())),
        (80, Some(b"test-val77".to_vec())),
    ];
    verify_expectations(&chain, expectations);
}

#[test]
fn test_commit_at_zero() {
    let mut chain = gen_cs(100, 0);

    let key = vec![0u8; 12];
    let val = vec![0u8; 12];

    chain
        .commit(vec![(key.clone(), Some(val.clone()))], 0, true)
        .unwrap();
    chain.commit(vec![], 1, true).unwrap();
    chain.commit(vec![], 1, true).unwrap();

    assert_eq!(chain.get(key.as_slice()).unwrap(), Some(val.clone()));
    assert_eq!(chain.get_ver(key.as_slice(), 0).unwrap(), Some(val.clone()));
    assert_eq!(chain.get_ver(key.as_slice(), 1).unwrap(), Some(val));
}

fn gen_findb_cs(
    exist: Option<String>,
    ver_window: u64,
    interval: u64,
) -> (String, ChainState<FinDB>) {
    let path = exist.unwrap_or_else(|| {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut path = temp_dir();
        path.push(format!("findb–{}", time));
        path.as_os_str().to_str().unwrap().to_string()
    });

    let fdb = FinDB::open(&path).unwrap_or_else(|_| panic!("Failed to open a findb at {}", path));
    let opts = ChainStateOpts {
        name: Some("findb".to_string()),
        ver_window,
        snapshot_interval: interval,
        cleanup_aux: false,
    };

    (path, ChainState::create_with_opts(fdb, opts))
}

#[test]
fn test_reload_with_snapshots() {
    let (path, cs) = gen_findb_cs(None, 0, 0);
    drop(cs);
    let (path, cs) = gen_findb_cs(Some(path), 100, 10);
    drop(cs);
    let (path, cs) = gen_findb_cs(Some(path), 111, 3);
    drop(cs);
    let (path, cs) = gen_findb_cs(Some(path), 20, 4);
    drop(cs);
    let (_path, cs) = gen_findb_cs(Some(path), 0, 0);
    drop(cs);
}

fn commit_n(chain: &mut ChainState<FinDB>, n: u64) {
    let mut operations = vec![];
    for h in 0..n {
        let val = format!("val-{}", h);
        operations.push((h, Some(val.into_bytes())));
    }

    apply_operations(chain, operations, n);
}

fn compare_n(chain: &ChainState<FinDB>, s: u64, e: u64) {
    let mut expectations = vec![];
    for h in s..e {
        let val = format!("val-{}", h);
        expectations.push((h, Some(val.into_bytes())));
    }
    verify_expectations(chain, expectations);
}

fn expect_same(chain: &ChainState<FinDB>, s: u64, e: u64, val: Option<Vec<u8>>) {
    let mut expectations = vec![];
    for h in s..e {
        expectations.push((h, val.clone()));
    }
    verify_expectations(chain, expectations);
}

#[test]
fn test_reload_with_snapshots_1() {
    let (path, mut cs) = gen_findb_cs(None, 100, 10);
    commit_n(&mut cs, 200);
    expect_same(&cs, 0, 98, None);
    compare_n(&cs, 98, 99);
    expect_same(&cs, 200, 210, Some(format!("val-{}", 199).into_bytes()));
    drop(cs);

    println!("ver_window 100, interval 5");
    let (path, cs) = gen_findb_cs(Some(path), 100, 5);
    // current height 199, min_height 99
    // height 98 is in the base but not squashed
    expect_same(&cs, 0, 98, None);
    compare_n(&cs, 98, 200);
    expect_same(&cs, 200, 210, Some(format!("val-{}", 199).into_bytes()));
    drop(cs);

    println!("ver_window 111, interval 3");
    let (_, cs) = gen_findb_cs(Some(path), 111, 3);
    // current height 199, ver_window 111 min_height 88
    // height 87 is in the base but not squashed
    expect_same(&cs, 0, 88, None);
    compare_n(&cs, 88, 200);
    expect_same(&cs, 200, 210, Some(format!("val-{}", 199).into_bytes()));
    drop(cs);
}
