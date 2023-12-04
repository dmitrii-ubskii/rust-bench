use std::{
    fmt::{Display, Formatter},
    time::Duration,
};

pub(crate) struct Measurement {
    pub(crate) key_count: usize,
    pub(crate) key_size: usize,
    pub(crate) written_bytes: u64,
    pub(crate) duration: Duration,
}

impl Measurement {
    pub(crate) fn new(key_count: usize, key_size: usize, written_bytes: u64, duration: Duration) -> Measurement {
        Measurement { key_count, key_size, written_bytes, duration }
    }

    fn throughput(&self) -> f64 {
        (self.written_bytes as f64) / self.duration.as_secs_f64()
    }
}

impl Display for Measurement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MB/sec: {:.3}, key count: {}, key size: {}, bytes written: {}, time: {:?}",
            self.throughput() / 1_000_000.0,
            self.key_count,
            self.key_size,
            self.written_bytes,
            self.duration,
        )
    }
}
