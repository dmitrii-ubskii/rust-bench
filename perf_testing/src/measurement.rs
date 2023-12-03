use std::fmt::{Display, Formatter};
use std::time::Duration;

pub(crate) struct Measurement {
    pub(crate) key_count: u64,
    pub(crate) key_size: u64,
    pub(crate) written_bytes : u64,
    pub(crate) duration: Duration,
}

impl Measurement {
    const NANOS_PER_SECOND: u128 = 1_000 * 1_000 * 1_000;

    pub(crate) fn new(key_count: u64, key_size: u64, written_bytes: u64, duration: Duration) -> Measurement {
        Measurement { key_count, key_size, written_bytes, duration }
    }

    fn throughput(&self) -> f64 {
        (self.written_bytes as f64) / self.duration.as_secs_f64()
    }
}

impl Display for Measurement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MB/sec: {:.3}, key count: {}, key size: {}, bytes written: {}, nanos: {}",
               self.throughput() / 1_000_000.0,
               self.key_count,
               self.key_size,
               self.written_bytes,
               self.duration.as_nanos(),
        )
    }
}
