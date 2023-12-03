mod sorted_array;
mod key;
mod memtable;
mod measurement;
mod kv_storage;

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::ops::DerefMut;
use std::path::Path;
use rand::Rng;
use std::time::{Duration, Instant};
use speedb::Options;
use crate::key::{Key, KEY_SIZE};
use crate::kv_storage::Storage;
use crate::measurement::Measurement;
use crate::memtable::{Memtable};


fn load_set_ordered(set: &mut BTreeSet<Key>, count: u64) -> Measurement {
    let mut generated: Vec<Key> = vec![];
    for i in (0..count) {
        let mut key: [u8; KEY_SIZE] = [0; KEY_SIZE];
        let bytes = i.to_be_bytes();
        key[..bytes.len()].copy_from_slice(bytes.as_slice());
        generated.push(Key { key });
    }
    let start = Instant::now();
    for key in generated {
        set.insert(key);
    }
    let end = Instant::now();
    debug_assert_eq!(set.len(), count as usize);
    Measurement::new(
        count as u64, KEY_SIZE as u64,
        (count as u64) * (KEY_SIZE as u64), end.duration_since(start)
    )
}

fn load_set_batch_ordered(set: &mut BTreeSet<Key>, count: u64) -> Measurement {
    let mut rng = rand::thread_rng();
    const BATCH: usize = 10;
    let mut generated: Vec<BTreeSet<Key>> = vec![];
    for _ in (0..count).step_by(BATCH) {
        let batch: BTreeSet<Key> = (0..BATCH).map(|_| Key { key: rng.gen() }).collect();
        generated.push(batch);
    }
    println!("Created batches: {}, {}", generated.len(), BATCH);
    let start = Instant::now();
    generated.into_iter().enumerate().for_each(|(i, batch)| {
        // set.append(batch);
        set.extend(batch.into_iter());
        // if i % 1000 == 0 {
        //     println!("Batch nr: {}", i);
        //     println!("Set size: {}", set.len());
        // }
    });
    let end = Instant::now();
    debug_assert_eq!(set.len(), count as usize);
    Measurement::new(
        count as u64, KEY_SIZE as u64,
    (count as u64) * (KEY_SIZE as u64), end.duration_since(start)
    )
}

fn load_set_random(set: &mut BTreeSet<Key>, count: u64) -> Measurement {
    let mut rng = rand::thread_rng();
    let generated: Vec<Key> = (0..count).map(|_| Key { key: rng.gen() }).collect();
    let start = Instant::now();
    for key in generated {
        set.insert(key);
    }
    let end = Instant::now();
    debug_assert_eq!(set.len(), count as usize);
    Measurement::new(
        count, KEY_SIZE as u64,
        (count) * (KEY_SIZE as u64), end.duration_since(start)
    )
}

fn fill_memtable(memtable: &mut Memtable) -> Measurement {
    let mut rng = rand::thread_rng();
    let generated: Vec<Key> = (0..memtable.max_keys()).map(|_| Key { key: rng.gen() }).collect();
    let start = Instant::now();
    for key in generated {
        memtable.put(key);
    }
    let end = Instant::now();
    debug_assert_eq!(memtable.len(), memtable.max_keys() as usize);
    Measurement::new(
        memtable.len() as u64, KEY_SIZE as u64,
        (memtable.len() as u64) * (KEY_SIZE as u64), end.duration_since(start)
    )
}


fn main() {
    const MAX_SIZE_BYTES: u64 = 256_000_000;
    let dir_name = "testing-store";
    let mut options = Options::default();
    options.create_if_missing(true);
    let mut storage = Storage::new(dir_name, &mut options);
    let start = Instant::now();
    for i in (0..100) {
        println!("---Iteration {} ---", i);
        let mut memtable = Memtable::new(MAX_SIZE_BYTES);
        let fill_measurement = fill_memtable(&mut memtable);
        println!("Memtable fill: {}", fill_measurement);
        let (sst_measurement, ingest_measurement)= storage.write_to_sst_and_ingest(memtable).unwrap();
        println!("SST write: {}", sst_measurement);
        println!("SST ingest: {}", ingest_measurement);


        // let mut set_random = BTreeSet::new();
        // let measurement_random = load_set_random(&mut set_random, COUNT);
        // println!("Elapsed random: {}", measurement_random);
        // let mut set_ordered = BTreeSet::new();
        // let measurement_ordered = load_set_ordered(&mut set_ordered, COUNT);
        // println!("Elapsed ordered: {}", measurement_ordered);
        // let mut batch_ordered = BTreeSet::new();
        // let measurement_batch_ordered = load_set_batch_ordered(&mut batch_ordered, COUNT);
        // println!("Elapsed batch ordered: {}", measurement_batch_ordered);
    }
    let end = Instant::now();
    println!("Total time: {}", end.duration_since(start).as_secs());

    let start = Instant::now();
    let count = storage.total_keys();
    let end = Instant::now();
    println!("Total keys in db: {}, in time: {}", count, end.duration_since(start).as_secs_f64());
}
