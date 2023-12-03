pub(crate) const KEY_SIZE: usize = 32;

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct Key {
    pub(crate) key: [u8; KEY_SIZE],
}