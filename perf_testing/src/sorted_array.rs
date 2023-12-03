use std::fmt::{Display, Formatter};

const CAPACITY: usize = 1000;

struct SortedArray {
    data: [u64; CAPACITY],
    size: usize,
}

impl Display for SortedArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}, size: {}", self.data, self.size)
    }
}

impl SortedArray {
    fn new() -> SortedArray {
        SortedArray { data: [0; CAPACITY], size: 0 }
    }

    fn seek(&self, item: &u64) -> Option<usize> {
        self.data.iter().position(|x| x > item)
    }

    fn insert(&mut self, item: u64) {
        let position = self.seek(&item);
        // println!("{}", self);
        // println!("Found element greater than item {} at position: {:?}", item, position);
        match position {
            Some(pos) => {
                for i in (pos..self.size).rev() {
                    self.data[i + 1] = self.data[i];
                }
                self.data[pos] = item;
                self.size = self.size + 1;
            }
            None => {
                self.data[self.size] = item;
                self.size = self.size + 1;
            }
        }
    }
}

fn test_sorted_array(array: &mut SortedArray) {
    for i in (0..CAPACITY).rev() {
        array.insert(i as u64)
    }
}
