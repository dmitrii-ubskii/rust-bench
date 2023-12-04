mod key;
mod kv_storage;
mod measurement;
mod memtable;

use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Instant,
};

use rand::{thread_rng, Rng};
use speedb::{Options, WriteOptions};

use crate::{
    key::{Key, Keys, KEY_SIZE},
    kv_storage::{Storage, StorageReader},
    measurement::Measurement,
    memtable::Memtable,
};

#[allow(dead_code)]
fn load_set_ordered(set: &mut BTreeSet<Key>, count: usize) -> Measurement {
    let mut generated: Vec<Key> = vec![];
    for i in 0..count {
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
    debug_assert_eq!(set.len(), count);
    Measurement::new(count, KEY_SIZE, (count * KEY_SIZE) as u64, end.duration_since(start))
}

#[allow(dead_code)]
fn load_set_batch_ordered(set: &mut BTreeSet<Key>, count: usize) -> Measurement {
    let mut rng = thread_rng();
    const BATCH: usize = 10;
    let mut generated: Vec<BTreeSet<Key>> = vec![];
    for _ in (0..count).step_by(BATCH) {
        let batch: BTreeSet<Key> = (0..BATCH).map(|_| Key { key: rng.gen() }).collect();
        generated.push(batch);
    }
    println!("Created batches: {}, {}", generated.len(), BATCH);
    let start = Instant::now();
    generated.into_iter().for_each(|batch| {
        set.extend(batch);
    });
    let end = Instant::now();
    debug_assert_eq!(set.len(), { count });
    Measurement::new(count, KEY_SIZE, (count * KEY_SIZE) as u64, end.duration_since(start))
}

#[allow(dead_code)]
fn load_set_random(set: &mut BTreeSet<Key>, count: usize) -> Measurement {
    let mut rng = thread_rng();
    let generated: Vec<Key> = (0..count).map(|_| Key { key: rng.gen() }).collect();
    let start = Instant::now();
    for key in generated {
        set.insert(key);
    }
    let end = Instant::now();
    debug_assert_eq!(set.len(), { count });
    Measurement::new(count, KEY_SIZE, (count * KEY_SIZE) as u64, end.duration_since(start))
}

fn fill_memtable(memtable: &mut Memtable) -> Measurement {
    let mut rng = thread_rng();
    let generated: Vec<Key> = (0..memtable.max_keys()).map(|_| Key { key: rng.gen() }).collect();
    let start = Instant::now();
    for key in generated {
        memtable.put(key);
    }
    let end = Instant::now();
    debug_assert_eq!(memtable.len(), { memtable.max_keys() });
    Measurement::new(memtable.len(), KEY_SIZE, (memtable.len() * KEY_SIZE) as u64, end.duration_since(start))
}

const READER_LOG_PERIOD: usize = 10_000;

fn read_random_full_keys(reader: StorageReader, stop: Arc<AtomicBool>) {
    let mut rng = thread_rng();
    let mut attempts: usize = 0;
    let mut matches: usize = 0;
    let start = Instant::now();

    let mut current = start;
    while !stop.load(Ordering::Relaxed) {
        let key = Key { key: rng.gen() };
        attempts += 1;
        matches += reader.get(key).is_some() as usize;

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
    println!(
        "Total of {} get queries, which matched {} times, in {:2} seconds. Average get rate: {:.2} reads/sec",
        attempts, matches, duration, rate
    );
}

fn read_prefix_iter(reader: StorageReader, stop: Arc<AtomicBool>) {
    let mut rng = thread_rng();
    let mut matches: usize = 0;
    let mut iterated: usize = 0;
    let start = Instant::now();

    let mut current = start;
    for attempts in 0.. {
        let prefix: [u8; 16] = rng.gen();
        let read = reader.iterate_10(prefix);
        iterated += read as usize;
        matches += (read == 0) as usize;

        if attempts % READER_LOG_PERIOD == 0 {
            let now = Instant::now();
            let duration = now.duration_since(current).as_secs_f64();
            let rate = READER_LOG_PERIOD as f64 / duration;
            println!("Prefix rate\t: {:.2} reads/sec", rate);
            current = now;
        }

        if stop.load(Ordering::Relaxed) {
            let duration = start.elapsed().as_secs_f64();
            let rate = (attempts as f64) / duration;
            print!(
                "Did {attempts} prefix queries, which matched >=1 element {matches} times for a total of {iterated}, "
            );
            print!("in {duration:2} seconds. ");
            println!("Rate of prefix seeks: {rate:.2} reads/sec");
            break;
        }
    }
}

fn main() {
    const SST_SIZE_TARGET: usize = 64_000_000;
    const SST_COUNT: usize = 10;
    let dir_name = "testing-store";
    std::fs::remove_dir_all(dir_name).expect("could not remove data dir");
    let mut options = Options::default();
    options.create_if_missing(true);
    options.enable_statistics();
    options.set_max_background_jobs(4);
    options.set_max_subcompactions(4);
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
    // write_direct_to_storage(&mut storage, SST_SIZE_TARGET * SST_COUNT / KEY_SIZE, SST_SIZE_TARGET / KEY_SIZE);

    stop.store(true, Ordering::Relaxed);
    let end = Instant::now();
    println!("Total time: {}", end.duration_since(start).as_secs_f64());

    let start = Instant::now();
    let count = storage.total_keys();
    let end = Instant::now();
    println!("Total keys in db: {}, in time: {}", count, end.duration_since(start).as_secs_f64());
}

#[allow(dead_code)]
fn write_memtables_to_storage(storage: &mut Storage, sst_size_target: usize, sst_count: usize) {
    for i in 0..sst_count {
        println!("---Iteration {} ---", i);
        let mut memtable = Memtable::new(sst_size_target);
        let fill_measurement = fill_memtable(&mut memtable);
        println!("Memtable fill: {}", fill_measurement);
        let (sst_measurement, ingest_measurement) = storage.write_to_sst_and_ingest(memtable).unwrap();
        println!("SST write: {}", sst_measurement);
        println!("SST ingest: {}", ingest_measurement);
    }
}

#[allow(dead_code)]
fn write_direct_to_storage(storage: &mut Storage, key_count: usize, batch_size: usize) {
    let mut write_options = WriteOptions::new();
    write_options.disable_wal(true);
    for (iteration, _) in (0..key_count).step_by(batch_size).enumerate() {
        println!("---Iteration {} ---", iteration);
        let mut rng = thread_rng();
        let generated = {
            let mut keys = Keys(vec![Key { key: [0; KEY_SIZE] }; batch_size]);
            rng.fill(&mut keys);
            keys.0
        };
        let start = Instant::now();
        for keys in generated.chunks(100) {
            storage.put(keys);
        }
        let end = Instant::now();
        let storage_write_measurement =
            Measurement::new(batch_size, KEY_SIZE, (batch_size * KEY_SIZE) as u64, end.duration_since(start));
        println!("Storage batch write: {}", storage_write_measurement);
    }
}
