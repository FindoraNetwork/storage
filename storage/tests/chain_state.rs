use storage::state::ChainState;
use temp_db::TempFinDB;

#[test]
fn test_current_window() {
    let ver_window = 2;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window, false);

    assert!(chain.current_window().map(|t| t == (1, 0)).is_ok());

    assert!(chain.commit(vec![], 1, true).is_ok());
    assert!(chain.commit(vec![], 2, true).is_ok());

    assert!(chain.current_window().map(|t| t == (1, 2)).is_ok());
    assert!(chain.commit(vec![], 3, true).is_ok());
    assert!(chain.current_window().map(|t| t == (1, 3)).is_ok());
    assert!(chain.commit(vec![], 4, true).is_ok());
    assert!(chain.current_window().map(|t| t == (2, 4)).is_ok());

    assert!(chain.commit(vec![], 5, true).is_ok());
    assert!(chain.current_window().map(|t| t == (3, 5)).is_ok());
}

#[test]
fn test_pin_height() {
    let ver_window = 3;
    let fdb = TempFinDB::new().expect("failed to create temp findb");
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window, false);
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
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window, false);

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
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window, false);

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
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window, false);

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
    let mut chain = ChainState::new(fdb, "test".to_string(), ver_window, false);

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
    let mut chain = ChainState::new(fdb, "test".to_string(), 0, false);

    for h in 1..4 {
        assert!(chain.commit(vec![], h, true).is_ok());
    }

    assert!(chain.current_window().is_err());

    assert!(chain.pin_at(4).is_err());
}
