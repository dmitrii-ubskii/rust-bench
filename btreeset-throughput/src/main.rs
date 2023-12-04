use std::{collections::BTreeSet, sync::Mutex, time::Instant};

use crossbeam_skiplist::SkipSet;
use crossbeam_utils::thread::scope;
use rand::{thread_rng, Rng};

type Key = [u8; 32];
const KEY_SIZE: usize = std::mem::size_of::<Key>();

fn main() {
    let total_size_mb = 1024usize;
    println!("Key size: {} bytes", KEY_SIZE);
    println!("Total generated size: {} MiB\n", total_size_mb);

    let num_keys: usize = total_size_mb * 1024 * 1024 / KEY_SIZE;

    println!("# Randomly generating keys (Vec + capacity)");
    let now = Instant::now();
    let data: Vec<Key> = (0..num_keys).map(|_| random()).collect();
    report_throughput(total_size_mb, now);

    println!("# Vec: pushes, then sort");
    measure(&data, |data| {
        let mut vec = Vec::new();
        for &x in data {
            vec.push(x);
        }
        vec.sort();
    });

    println!("# Vec: pushes with_capacity, then sort");
    measure(&data, |data| {
        let mut vec = Vec::with_capacity(num_keys);
        for &x in data {
            vec.push(x);
        }
        vec.sort();
    });

    println!("# Vec: collect, then sort");
    measure(&data, |data| {
        let mut vec: Vec<Key> = std::hint::black_box(data.iter().copied().collect());
        vec.sort();
    });

    println!("# BTreeSet: insert");
    measure(&data, |data| {
        let mut set = BTreeSet::new();
        for &x in data {
            set.insert(x);
        }
    });

    println!("# BTreeSet: collect");
    measure(&data, |data| {
        let _set: BTreeSet<Key> = data.iter().copied().collect();
    });

    println!("# BTreeSet: extend");
    measure(&data, |data| {
        let mut set = BTreeSet::new();
        set.extend(data.iter().copied());
    });

    println!("# BTreeSet: concurrent inserts in 4 threads");
    measure(&data, |data| {
        let set = Mutex::new(BTreeSet::new());
        scope(|s| {
            let set = &set;
            for chunk in data.chunks(data.len() / 4) {
                s.spawn(move |_| {
                    for &x in chunk {
                        set.lock().unwrap().insert(x);
                    }
                });
            }
        })
        .unwrap();
    });

    println!("# BTreeSet: batched concurrent inserts in 4 threads");
    measure(&data, |data| {
        let set = Mutex::new(BTreeSet::new());
        scope(|s| {
            let set = &set;
            for chunk in data.chunks(data.len() / 4) {
                s.spawn(move |_| {
                    for batch in chunk.chunks(32) {
                        let mut set = set.lock().unwrap();
                        for &x in batch {
                            set.insert(x);
                        }
                    }
                });
            }
        })
        .unwrap();
    });

    println!("# SkipSet: insert");
    measure(&data, |data| {
        let set = SkipSet::new();
        for &x in data {
            set.insert(x);
        }
    });

    println!("# SkipSet: collect");
    measure(&data, |data| {
        let _set: SkipSet<Key> = data.iter().copied().collect();
    });

    println!("# SkipSet: sorted inserts");
    measure(&data, |data| {
        let btree_set: BTreeSet<Key> = data.iter().copied().collect();
        let set = SkipSet::new();
        for x in btree_set {
            set.insert(x);
        }
    });

    println!("# SkipSet: concurrent inserts in 4 threads");
    measure(&data, |data| {
        let set = SkipSet::new();
        scope(|s| {
            let set = &set;
            for chunk in data.chunks(data.len() / 4) {
                s.spawn(move |_| {
                    for &x in chunk {
                        set.insert(x);
                    }
                });
            }
        })
        .unwrap();
    });

    println!("# SkipSet: concurrent sorted inserts in 4 threads");
    measure(&data, |data| {
        let set = SkipSet::new();
        scope(|s| {
            let set = &set;
            for chunk in data.chunks(data.len() / 4) {
                s.spawn(move |_| {
                    let btree_set: BTreeSet<Key> = chunk.iter().copied().collect();
                    for x in btree_set {
                        set.insert(x);
                    }
                });
            }
        })
        .unwrap();
    });
}

fn measure(data: &Vec<Key>, f: impl FnOnce(&Vec<Key>)) {
    let now = Instant::now();
    f(data);
    report_throughput(data.len() * KEY_SIZE / 1024 / 1024, now);
}

fn report_throughput(size_mb: usize, now: Instant) {
    let elapsed = now.elapsed();
    println!("Done in {:.3} s", elapsed.as_secs_f64());
    println!("Throughput: {:.2} MiB/s", size_mb as f64 / elapsed.as_secs_f64());
    println!();
}

fn random() -> Key {
    let mut key = [0; 32];
    thread_rng().fill(&mut key);
    key
}
