use std::{path::Path, sync::Arc, time::Instant};

use speedb::{
    Direction::Forward, Error, IteratorMode, IteratorMode::From, Options, SstFileWriter, WriteBatch, WriteOptions, DB, ColumnFamily, BoundColumnFamily,
};

use crate::{
    key::{Key, KEY_SIZE},
    measurement::Measurement,
    memtable::Memtable,
};

pub(crate) struct Storage<'a> {
    pub db: Arc<DB>,
    sst_writer: SstFileWriter<'a>,
    sst_counter: usize,
    write_options: WriteOptions,
}

impl<'a> Storage<'a> {
    const EMPTY_VALUE: [u8; 0] = [];

    pub(crate) fn new(path: &Path, options: &'a Options) -> Storage<'a> {
        let db = Arc::new(DB::open_cf(options, path, ["cf1", "cf2", "cf3", "cf0"]).unwrap());
        let sst_writer = SstFileWriter::create(options);
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        Storage { db, sst_writer, sst_counter: 0, write_options }
    }

    pub(crate) fn new_reader(&mut self) -> StorageReader {
        StorageReader { db: self.db.clone() }
    }

    pub(crate) fn write_to_sst_and_ingest(&mut self, memtable: Memtable) -> Result<(Measurement, Measurement), Error> {
        let key_count = memtable.len();
        let suffix = self.sst_counter;
        self.sst_counter += 1;
        let path = self.db.path().join(format!("ingested_sst_{suffix}"));

        let start_time = Instant::now();
        self.sst_writer.open(&path)?;
        for key in memtable {
            self.sst_writer.put(key.key, Storage::EMPTY_VALUE).unwrap();
        }
        let file_size = self.sst_writer.file_size();
        self.sst_writer.finish()?;
        let sst_write_measurement = Measurement::new(key_count, KEY_SIZE, file_size, start_time.elapsed());

        let start_time = Instant::now();
        self.db.ingest_external_file(vec![path])?;
        let ingest_measurement = Measurement::new(key_count, KEY_SIZE, file_size, start_time.elapsed());

        Ok((sst_write_measurement, ingest_measurement))
    }

    pub(crate) fn total_keys(&self) -> usize {
        self.db.iterator(IteratorMode::Start).count()
    }

    pub(crate) fn put(&self, keys: &[Key], cf: &Arc<BoundColumnFamily>) {
        let mut write_batch = WriteBatch::default();
        keys.iter().for_each(|key| write_batch.put_cf(cf, key.key, Storage::EMPTY_VALUE));
        self.db.write_opt(write_batch, &self.write_options).unwrap();
    }
}

pub(crate) struct StorageReader {
    db: Arc<DB>,
}

impl StorageReader {
    pub(crate) fn get(&self, key: Key) -> Option<Vec<u8>> {
        self.db.get(key.key).unwrap()
    }

    pub(crate) fn iterate_10(&self, prefix: [u8; 16]) -> usize {
        self.db.iterator(From(&prefix, Forward)).take(10).count()
    }
}
