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
use rand::{seq::SliceRandom, thread_rng, Rng};

use self::{
    concept::{Attribute, AttributeType, Prefix, Thing, ThingID, Type, TypeID, ValueType},
    storage::{Storage, WriteHandle},
};

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
        .arg(arg!(-b --batch-reads "Try to batch reads before writes").required(false).action(ArgAction::SetTrue))
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
        .get_matches();

    let Some(&mode) = args.get_one("mode") else { panic!("could not get value of --mode") };

    let Some(storage_dir) = args.get_one::<PathBuf>("dir") else { panic!("could not get value of --dir") };
    let storage = Storage::new(storage_dir, mode);

    let stop = AtomicBool::new(false);

    let Some(&num_threads) = args.get_one::<usize>("threads") else { panic!("could not get value of --threads") };
    let batch_reads = args.get_one("batch_reads").copied().unwrap_or(false);

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
    .map(|value| Attribute { type_: NAME, value })
    .collect_vec();

    let mut writer = WriteHandle::default();
    supernodes.iter().unique().for_each(|name| {
        register_person(&mut writer, *name);
    });
    storage.commit(writer);

    thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn({
                let stop = &stop;
                let supernodes = &supernodes;
                let storage = &storage;
                move || agent(storage, stop, batch_reads, supernodes)
            });
        }

        thread::sleep(Duration::from_secs(5));
        stop.store(true, Ordering::Release);
    });

    storage.print_stats();
}

const PERSON: Type = Type { prefix: Prefix::Entity, id: TypeID { id: 0 } };
const FRIENDSHIP: Type = Type { prefix: Prefix::Relation, id: TypeID { id: 0 } };
const FRIEND: Type = Type { prefix: Prefix::Role, id: TypeID { id: 0 } };
const NAME: AttributeType =
    AttributeType { prefix: Prefix::Attribute, id: TypeID { id: 0 }, value_type: ValueType::Long };

fn agent(storage: &Storage, stop: &AtomicBool, batch_reads: bool, supernodes: &Vec<Attribute>) {
    while !stop.load(Ordering::Relaxed) {
        let mut writer = WriteHandle::default();

        if batch_reads {
            todo!()
        } else {
            let name = Attribute { type_: NAME, value: thread_rng().gen() };
            let person = register_person(&mut writer, name);
            make_supernode_friendships(storage, &mut writer, person, supernodes);
            // make_random_friendships(db, &mut write_batch, person, supernodes);
        }

        // db.write_without_wal(write_batch).unwrap();
        storage.commit(writer);
    }
}

fn make_supernode_friendships(storage: &Storage, writer: &mut WriteHandle, person: Thing, supernodes: &Vec<Attribute>) {
    let name = supernodes.choose(&mut thread_rng()).unwrap();
    if let Some(popular) = storage.get_one_owner(name) {
        let rel = Thing { type_: FRIENDSHIP, thing_id: ThingID { id: thread_rng().gen() } };
        writer.put_relation(rel, [(FRIEND, popular), (FRIEND, person)]);
    }
}

fn register_person(writer: &mut WriteHandle, name: Attribute) -> Thing {
    writer.put_attribute(name);
    // assume collisions unlikely
    let person = Thing { type_: PERSON, thing_id: ThingID { id: thread_rng().gen() } };
    writer.put_entity(person);
    writer.put_ownership(person, name);
    person
}
