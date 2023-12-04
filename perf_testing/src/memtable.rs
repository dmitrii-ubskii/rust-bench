use std::collections::{btree_set::Iter, BTreeSet};

use crate::key::{Key, KEY_SIZE};

pub(crate) struct Memtable {
    data: BTreeSet<Key>,
    max_keys: usize,
}

impl Memtable {
    pub(crate) fn new(max_size_bytes: usize) -> Memtable {
        Memtable { data: BTreeSet::new(), max_keys: max_size_bytes / KEY_SIZE }
    }

    pub(crate) fn max_keys(&self) -> usize {
        self.max_keys
    }

    pub(crate) fn put(&mut self, key: Key) {
        assert!(self.data.len() < self.max_keys);
        self.data.insert(key);
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn iter(&self) -> Iter<'_, Key> {
        self.data.iter()
    }
}
