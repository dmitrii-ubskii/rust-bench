mod agent;
mod concept;
mod storage;

use std::{
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use clap::{arg, command, value_parser, ArgAction};
use itertools::Itertools;

use self::{concept::Attribute, storage::Storage};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Mode {
    SingleColumnFamily,
    MultipleColumnFamilies,
    MultipleDatabases,
}

impl FromStr for Mode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "SINGLE" => Ok(Self::SingleColumnFamily),
            "CF" => Ok(Self::MultipleColumnFamilies),
            "DB" => Ok(Self::MultipleDatabases),
            s => Err(format!("Unexpected mode argument: '{s}'. Expected SINGLE, CF, or DB.")),
        }
    }
}

fn main() {
    let args = command!()
        .arg(arg!(-b --"batch-reads" "Try to batch reads before writes").required(false).action(ArgAction::SetTrue))
        .arg(
            arg!(-t --threads "Number of writer threads")
                .required(false)
                .action(ArgAction::Set)
                .value_parser(value_parser!(usize))
                .default_value("4"),
        )
        .arg(
            arg!(-m --mode <MODE> "SINGLE (default) / CF / DB")
                .value_parser(value_parser!(Mode))
                .default_value("SINGLE"),
        )
        .arg(
            arg!(-d --dir <DIR> "storage directory (default: ./testing-store)")
                .value_parser(value_parser!(PathBuf))
                .default_value("testing-store"),
        )
        .arg(
            arg!(-s --seconds <SECONDS> "how long to run the benchmark for")
                .value_parser(value_parser!(u64))
                .default_value("1"),
        )
        .get_matches();

    let mode = get_arg(&args, "mode");
    let storage_dir = get_arg::<PathBuf>(&args, "dir");
    let storage = Storage::new(&storage_dir, mode);

    let num_threads = get_arg::<usize>(&args, "threads");
    let batch_reads = args.get_one("batch-reads").copied().unwrap_or(false);

    #[rustfmt::skip]
    let supernodes = [
        0xADE1A1DE,  0xADE1A1DE,  0xADE1A1DE,  0xADE1A1DE,  0xADE1A1DE,
        0xBAA1,      0xBAA1,      0xBAA1,      0xBAA1,
        0xB0BB1E,    0xB0BB1E,    0xB0BB1E,
        0xDEBB1E,    0xDEBB1E,    0xDEBB1E,
        0x01AF,      0x01AF,
        0xC0FFEE,    0xC0FFEE,
        0x0DDBA11,
        0xB01DFACE,
    ]
    .into_iter()
    .map(|value| Attribute { type_: agent::NAME, value })
    .collect_vec();

    let mut writer = storage.writer();
    supernodes.iter().unique().for_each(|name| {
        agent::register_person(&mut writer, *name);
    });
    storage.commit(writer);

    let stop = AtomicBool::new(false);

    thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn({
                let stop = &stop;
                let supernodes = &supernodes;
                let storage = &storage;
                move || agent::agent(storage, stop, batch_reads, supernodes)
            });
        }

        thread::sleep(Duration::from_secs(get_arg(&args, "seconds")));
        stop.store(true, Ordering::Release);
    });

    storage.print_stats();
}

fn get_arg<T: Clone + Send + Sync + 'static>(args: &clap::ArgMatches, key: &str) -> T {
    args.get_one::<T>(key).cloned().expect("could not get value of --{key}")
}
