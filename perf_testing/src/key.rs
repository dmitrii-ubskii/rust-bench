use std::slice;

use rand::Fill;

pub(crate) const KEY_SIZE: usize = 32;

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct Key {
    pub(crate) key: [u8; KEY_SIZE],
}

impl Fill for Key {
    fn try_fill<R: rand::Rng + ?Sized>(&mut self, rng: &mut R) -> Result<(), rand::Error> {
        self.key.try_fill(rng)
    }
}

pub(crate) struct Keys(pub(crate) Vec<Key>);

impl Fill for Keys {
    fn try_fill<R: rand::Rng + ?Sized>(&mut self, rng: &mut R) -> Result<(), rand::Error> {
        let pun = unsafe {
            let data = &mut self.0[0] as *mut Key as *mut _;
            let len = self.0.len();
            slice::from_raw_parts_mut::<u8>(data, len * std::mem::size_of::<Key>())
        };
        pun.try_fill(rng)
    }
}
