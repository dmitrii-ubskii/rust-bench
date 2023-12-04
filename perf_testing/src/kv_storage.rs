use std::{sync::Arc, time::Instant};

use speedb::{
    DBWithThreadMode, Direction::Forward, Error, IteratorMode, IteratorMode::From, Options, SingleThreaded,
    SstFileWriter, WriteBatch, WriteOptions, DB,
};

use crate::{
    key::{Key, KEY_SIZE},
    measurement::Measurement,
    memtable::Memtable,
};

pub(crate) struct Storage<'a> {
    db: Arc<DB>,
    sst_writer: SstFileWriter<'a>,
    sst_counter: usize,
    write_options: WriteOptions,
}

impl<'a> Storage<'a> {
    const EMPTY_KEY: [u8; 0] = [];

    pub(crate) fn new(path: &str, options: &'a mut Options) -> Storage<'a> {
        let db: DBWithThreadMode<SingleThreaded> = DB::open(options, path).unwrap();
        let writer: SstFileWriter<'a> = SstFileWriter::create(options);
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        Storage { db: Arc::new(db), sst_writer: writer, sst_counter: 0, write_options }
    }

    pub(crate) fn new_reader(&mut self) -> StorageReader {
        StorageReader { db: self.db.clone() }
    }

    pub(crate) fn write_to_sst_and_ingest(&mut self, memtable: Memtable) -> Result<(Measurement, Measurement), Error> {
        let start_time = Instant::now();
        let suffix = self.sst_counter;
        self.sst_counter += 1;
        let path = self.db.path().join(format!("ingested_sst_{}", suffix));
        self.sst_writer.open(&path)?;
        memtable.iter().for_each(|key| self.sst_writer.put(key.key, Storage::EMPTY_KEY).unwrap());
        let file_size = self.sst_writer.file_size();
        self.sst_writer.finish()?;
        let sst_written_time = Instant::now();
        self.db.ingest_external_file(vec![path])?;
        let end_time = Instant::now();
        let sst_write_measurement =
            Measurement::new(memtable.len(), KEY_SIZE, file_size, sst_written_time.duration_since(start_time));
        let ingest_measurement =
            Measurement::new(memtable.len(), KEY_SIZE, file_size, end_time.duration_since(sst_written_time));
        Ok((sst_write_measurement, ingest_measurement))
    }

    pub(crate) fn total_keys(&self) -> usize {
        self.db.iterator(IteratorMode::Start).count()
    }

    pub(crate) fn put(&mut self, keys: &[Key]) {
        let mut write_batch = WriteBatch::default();
        keys.iter().for_each(|key| write_batch.put(key.key, Storage::EMPTY_KEY));
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
