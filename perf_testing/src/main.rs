// mod sorted_array;
mod key;
mod memtable;
mod measurement;
mod kv_storage;

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use rand::Rng;
use std::time::{Duration, Instant};
use speedb::{Error, Options, WriteBatch, WriteOptions};
use crate::key::{Key, KEY_SIZE};
use crate::kv_storage::{Storage, StorageReader};
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
        (count as u64) * (KEY_SIZE as u64), end.duration_since(start),
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
        (count as u64) * (KEY_SIZE as u64), end.duration_since(start),
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
        (count) * (KEY_SIZE as u64), end.duration_since(start),
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
        (memtable.len() as u64) * (KEY_SIZE as u64), end.duration_since(start),
    )
}

const READER_LOG_PERIOD: u64 = 10_000;

fn read_random_full_keys(reader: StorageReader, stop: Arc<AtomicBool>) {
    let mut rng = rand::thread_rng();
    let mut attempts: u64 = 0;
    let mut matches: u64 = 0;
    let start = Instant::now();

    let mut current = start.clone();
    while !stop.load(Ordering::Relaxed) {
        let key = Key { key: rng.gen() };
        attempts = attempts + 1;
        match reader.get(key) {
            Some(_) => matches = matches + 1,
            None => {}
        }

        if attempts % READER_LOG_PERIOD == 0 {
            let now = Instant::now();
            let duration = now.duration_since(current).as_secs_f64();
            let rate = READER_LOG_PERIOD as f64 / duration;
            println!("Get rate\t: {:.2} reads/sec", rate);
            current = now;
        }
    }
    let end = Instant::now();
    let duration = end.duration_since(start).as_secs_f64();
    let rate = (attempts as f64) / duration;
    println!("Total of {} get queries, which matched {} times, in {:2} seconds. Average get rate: {:.2} reads/sec", attempts, matches, duration, rate);
}

fn read_prefix_iter(reader: StorageReader, stop: Arc<AtomicBool>) {
    let mut rng = rand::thread_rng();
    let mut attempts: u64 = 0;
    let mut matches: u64 = 0;
    let mut iterated: u64 = 0;
    let start = Instant::now();

    let mut current = start.clone();
    while !stop.load(Ordering::Relaxed) {
        let prefix: [u8; 16] = rng.gen();
        attempts = attempts + 1;
        let read = reader.iterate_10(prefix);
        iterated = iterated + (read as u64);
        match read {
            0 => {}
            _ => matches = matches + 1,
        }

        if attempts % READER_LOG_PERIOD == 0 {
            let now = Instant::now();
            let duration = now.duration_since(current).as_secs_f64();
            let rate = READER_LOG_PERIOD as f64 / duration;
            println!("Prefix rate\t: {:.2} reads/sec", rate);
            current = now;
        }
    }
    let end = Instant::now();
    let duration = end.duration_since(start).as_secs_f64();
    let rate = (attempts as f64) / duration;
    println!("Did {} prefix queries, which matched >=1 element {} times for a total of {}, in {:2} seconds. Rate of prefix seeks: {:.2} reads/sec", attempts, matches, iterated, duration, rate);
}

fn main() {
    const SST_SIZE_TARGET: u64 = 64_000_000;
    const SST_COUNT: u64 = 1000;
    let dir_name = "testing-store";
    let mut options = Options::default();
    options.create_if_missing(true);
    options.enable_statistics();
    options.set_max_background_jobs(8);
    options.set_max_subcompactions(8);
    let mut storage = Storage::new(dir_name, &mut options);

    let stop = Arc::new(AtomicBool::new(false));
    let reader_keys = storage.new_reader();
    let stop_keys = stop.clone();
    thread::spawn(move || {
        read_random_full_keys(reader_keys, stop_keys);
    });
    let reader_prefixes = storage.new_reader();
    let stop_prefixes = stop.clone();
    thread::spawn(move || {
        read_prefix_iter(reader_prefixes, stop_prefixes);
    });

    let start = Instant::now();
    write_memtables_to_storage(&mut storage, SST_SIZE_TARGET, SST_COUNT);
    // write_direct_to_storage(&mut storage, SST_SIZE_TARGET * SST_COUNT / KEY_SIZE as u64, SST_SIZE_TARGET / KEY_SIZE as u64);

    stop.store(true, Ordering::Relaxed);
    let end = Instant::now();
    println!("Total time: {}", end.duration_since(start).as_secs_f64());

    let start = Instant::now();
    let count = storage.total_keys();
    let end = Instant::now();
    println!("Total keys in db: {}, in time: {}", count, end.duration_since(start).as_secs_f64());
}

fn write_memtables_to_storage(storage: &mut Storage, sst_size_target: u64, sst_count: u64) {
    for i in (0..sst_count) {
        println!("---Iteration {} ---", i);
        let mut memtable = Memtable::new(sst_size_target);
        let fill_measurement = fill_memtable(&mut memtable);
        println!("Memtable fill: {}", fill_measurement);
        let (sst_measurement, ingest_measurement) = storage.write_to_sst_and_ingest(memtable).unwrap();
        println!("SST write: {}", sst_measurement);
        println!("SST ingest: {}", ingest_measurement);
    }
}


fn write_direct_to_storage(storage: &mut Storage, key_count: u64, batch_size: u64) {
    let mut iteration: u64 = 0;
    let mut write_options = WriteOptions::new();
    write_options.disable_wal(true);
    for _ in (0..key_count).step_by(batch_size as usize) {
        println!("---Iteration {} ---", iteration);
        let mut rng = rand::thread_rng();
        let generated: Vec<Key> = (0..batch_size).map(|_| Key { key: rng.gen() }).collect();
        let start = Instant::now();
        for keys in generated.chunks(100) {
            storage.put(keys);
        }
        let end = Instant::now();
        let storage_write_measurement = Measurement::new(
            batch_size, KEY_SIZE as u64,
            (batch_size) * (KEY_SIZE as u64), end.duration_since(start),
        );
        iteration = iteration + 1;
        println!("Storage batch write: {}", storage_write_measurement);
    }
}
