use std::collections::{BTreeSet};
use std::collections::btree_set::Iter;
use crate::key::{Key, KEY_SIZE};

pub(crate) struct Memtable {
    data: BTreeSet<Key>,
    // max_size_bytes: u64,
    max_keys: u64
}

impl Memtable {
    pub(crate) fn new(max_size_bytes: u64) -> Memtable {
        Memtable{ data: BTreeSet::new(), max_keys: max_size_bytes / KEY_SIZE as u64 }
    }

    pub(crate) fn max_keys(&self) -> u64 {
        self.max_keys
    }

    pub(crate) fn put(&mut self, key: Key) {
        assert!((self.data.len() as u64) < self.max_keys);
        self.data.insert(key);
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn iter(&self) -> Iter<'_, Key> {
        self.data.iter()
    }
}