use std::{
    fs::{self, File},
    io::Write,
    time::{Duration, Instant},
};

use rand::{thread_rng, Rng};

fn main() {
    let data = {
        let mut data = vec![0u8; 1024 * 1024 * 1024];
        thread_rng().fill(&mut *data);
        data
    };
    for sync_interval in [2, 10, 50, 100].into_iter().map(Duration::from_millis) {
        println!("Syncing all metadata every {sync_interval:?}");
        write_benchmark(&data, sync_interval);
    }
}

fn write_benchmark(data: &[u8], _: Duration) {
    let mut file = File::create("./.tmp").expect("could not open ./.tmp for writing");
    let start = Instant::now();
    for chunk in data.chunks(4096) {
        file.write_all(chunk).expect("could not write a chunk into ./.tmp");
    }
    file.sync_all().expect("could not sync file to disk");
    drop(file);
    report_throughput(data.len(), start);
    fs::remove_file("./.tmp").expect("could not delete ./.tmp");
}

fn report_throughput(size: usize, now: Instant) {
    let elapsed = now.elapsed();
    println!("Done in {:.3} s", elapsed.as_secs_f64());
    println!("Throughput: {:.2} MiB/s", size as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64());
    println!();
}
