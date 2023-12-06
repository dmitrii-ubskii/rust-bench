use std::collections::BTreeSet;

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
}

impl IntoIterator for Memtable {
    type Item = Key;

    type IntoIter = <BTreeSet<Key> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<'a> IntoIterator for &'a Memtable {
    type Item = &'a Key;

    type IntoIter = <&'a BTreeSet<Key> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}
