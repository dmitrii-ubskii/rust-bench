#![allow(dead_code)]

mod key;
mod kv_storage;
mod measurement;
mod memtable;

use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    time::Instant,
};

use itertools::Itertools;
use rand::{thread_rng, Rng};
use speedb::{
    BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamily, ColumnFamilyDescriptor, CuckooTableOptions,
    DBCompactionStyle, Options, WriteOptions,
};

use crate::{
    key::{Key, Keys, KEY_SIZE},
    kv_storage::{Storage, StorageReader},
    measurement::Measurement,
    memtable::Memtable,
};

const READER_LOG_PERIOD: usize = 10_000;

fn read_random_full_keys(reader: StorageReader, stop: Arc<AtomicBool>, read_queue: Arc<RwLock<Vec<Key>>>) {
    let mut rng = thread_rng();
    let mut matches = 0;
    let start = Instant::now();

    let mut current = start;
    for attempts in 0.. {
        if stop.load(Ordering::Relaxed) {
            let duration = start.elapsed();
            let rate = (attempts as f64) / duration.as_secs_f64();
            println!(
                "Total of {} get queries, which matched {} times, in {:.2?}. Average get rate: {:.2} reads/sec",
                attempts, matches, duration, rate
            );
            break;
        }

        let key = loop {
            let mut vec = read_queue.write().unwrap();
            let len = vec.len();
            if len > 0 {
                break vec.remove(rng.gen_range(0..len));
            }
        };
        matches += reader.get(key).is_some() as usize;

        if attempts % READER_LOG_PERIOD == 0 {
            let now = Instant::now();
            let duration = now.duration_since(current).as_secs_f64();
            let rate = READER_LOG_PERIOD as f64 / duration;
            println!("Get rate\t: {:.2} reads/sec", rate);
            current = now;
        }
    }
}

fn read_prefix_iter(reader: StorageReader, stop: Arc<AtomicBool>) {
    let mut rng = thread_rng();
    let mut matches = 0;
    let mut iterated = 0;
    let start = Instant::now();

    let mut current = start;
    for attempts in 0.. {
        if stop.load(Ordering::Relaxed) {
            let duration = start.elapsed();
            let rate = (attempts as f64) / duration.as_secs_f64();
            print!("Did {attempts} prefix queries, which matched >=1 element {matches} times ");
            println!("for a total of {iterated}, in {duration:.2?}. Rate of prefix seeks: {rate:.2} reads/sec");
            break;
        }

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
    }
}

const SST_SIZE_TARGET: usize = 64_000_000;
const SST_COUNT: usize = 320;

fn main() {
    let storage_dir = Path::new("testing-store");

    let options = {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.enable_statistics();
        options.set_max_background_jobs(4);
        options.set_max_subcompactions(4);
        options.create_missing_column_families(true);
        // options.set_disable_auto_compactions(true);
        options
    };

    // let read_queue = Arc::new(RwLock::new(Vec::<Key>::new()));

    // let stop = Arc::new(AtomicBool::new(false));
    // let key_reader_thread = thread::spawn({
    // let reader = storage.new_reader();
    // let stop = stop.clone();
    // let read_queue = read_queue.clone();
    // move || read_random_full_keys(reader, stop, read_queue)
    // });
    // let prefix_reader_thread = thread::spawn({
    // let reader = storage.new_reader();
    // let stop = stop.clone();
    // move || read_prefix_iter(reader, stop)
    // });

    test_direct(storage_dir, &options, 1, ["cf0", "cf0", "cf0", "cf0"]);
    test_direct(storage_dir, &options, 4, ["cf0", "cf0", "cf0", "cf0"]);
    test_direct(storage_dir, &options, 4, ["cf0", "cf1", "cf2", "cf3"]);
    // test_memtables(storage_dir, &options);

    // print!("{}", options.get_statistics().unwrap());
    // stop.store(true, Ordering::Relaxed);
    // key_reader_thread.join().unwrap();
    // prefix_reader_thread.join().unwrap();
}

fn test_direct(storage_dir: &Path, options: &Options, num_threads: usize, cfs: [&str; 4]) {
    if storage_dir.exists() {
        std::fs::remove_dir_all(storage_dir).expect("could not remove data dir");
    }

    let dbs = cfs
        .iter()
        .unique()
        .map(|&cf| (cf, Storage::new(&storage_dir.join(format!("db{cf}")), options)))
        .collect::<HashMap<_, _>>();

    let start = Instant::now();
    thread::scope(|s| {
        let dbs = &dbs;
        dbg!(num_threads);
        for i in 0..num_threads {
            s.spawn(move || {
                let cf = cfs[i];
                let storage = &dbs[cf];
                write_direct_to_storage(
                    storage,
                    storage.db.cf_handle(dbg!(cf)).unwrap(),
                    SST_SIZE_TARGET * SST_COUNT / KEY_SIZE / num_threads,
                    SST_SIZE_TARGET / KEY_SIZE / num_threads,
                )
            });
        }
    });
    println!("Total time: {:.2?}", start.elapsed());

    let start = Instant::now();
    let count: usize = dbs.values().map(Storage::total_keys).sum();
    println!("Total keys in db: {}, in time: {:.2?}", count, start.elapsed());
}

fn test_memtables(storage_dir: &Path, options: &Options) {
    if storage_dir.exists() {
        std::fs::remove_dir_all(storage_dir).expect("could not remove data dir");
    }

    let mut storage = Storage::new(storage_dir, options);

    let start = Instant::now();
    write_memtables_to_storage(&mut storage, SST_SIZE_TARGET, SST_COUNT);
    println!("Total time: {:.2?}", start.elapsed());

    let start = Instant::now();
    let count = storage.total_keys();
    println!("Total keys in db: {}, in time: {:.2?}", count, start.elapsed());
}

#[allow(dead_code)]
fn write_memtables_to_storage(storage: &mut Storage, sst_size_target: usize, sst_count: usize) {
    for i in 0..sst_count {
        println!("---Iteration {} ---", i);
        let mut memtable = Memtable::new(sst_size_target);
        let (fill_measurement, _read_queue_add) = fill_memtable(&mut memtable);
        println!("Memtable fill: {}", fill_measurement);
        let (sst_measurement, ingest_measurement) = storage.write_to_sst_and_ingest(memtable).unwrap();
        println!("SST write: {}", sst_measurement);
        println!("SST ingest: {}", ingest_measurement);
    }
}

fn fill_memtable(memtable: &mut Memtable) -> (Measurement, Vec<Key>) {
    let mut rng = thread_rng();
    let generated = {
        let mut keys = Keys(vec![Key { key: [0; KEY_SIZE] }; memtable.max_keys()]);
        rng.fill(&mut keys);
        keys.0
    };
    let start = Instant::now();
    for keys in generated.chunks(100) {
        let start = Instant::now();
        keys.iter().for_each(|&key| memtable.put(key));
        println!("memtable.put = {:.2?}", start.elapsed());
    }
    debug_assert_eq!(memtable.len(), memtable.max_keys());
    (Measurement::new(memtable.len(), KEY_SIZE, (memtable.len() * KEY_SIZE) as u64, start.elapsed()), Vec::new())
}

#[allow(dead_code)]
fn write_direct_to_storage(storage: &Storage, cf: Arc<BoundColumnFamily>, key_count: usize, batch_size: usize) {
    for (iteration, _) in (0..key_count).step_by(batch_size).enumerate() {
        // println!("---Iteration {iteration} ---");
        let mut rng = thread_rng();
        let generated: Vec<Key> = {
            let mut keys = Keys(vec![Key { key: [0; KEY_SIZE] }; batch_size]);
            rng.fill(&mut keys);
            keys.0
        };
        let start = Instant::now();
        for keys in generated.chunks(128) {
            storage.put(keys, &cf);
        }
        let storage_write_measurement =
            Measurement::new(batch_size, KEY_SIZE, (batch_size * KEY_SIZE) as u64, start.elapsed());
        // println!("Storage batch write: {storage_write_measurement}");
    }
}
