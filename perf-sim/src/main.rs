mod concept;

use std::{
    path::Path,
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use clap::{arg, command, value_parser, ArgAction};
use concept::{
    Attribute, AttributeType, EdgeType, HasEdge, Prefix, RelatesEdge, Thing, ThingID, Type, TypeID, ValueType,
};
use itertools::Itertools;
use rand::{seq::SliceRandom, thread_rng, Rng};
use speedb::{Direction, IteratorMode, Options, DB};

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
        .get_matches();

    let Some(&mode) = args.get_one("mode") else { panic!("could not get value of --mode") };
    let db = make_storage(mode);

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

    supernodes.iter().unique().for_each(|name| {
        register_person(&db, *name);
    });

    thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn({
                let stop = &stop;
                let supernodes = &supernodes;
                let db = &db;
                move || agent(db, stop, batch_reads, supernodes)
            });
        }

        thread::sleep(Duration::from_secs(5));
        stop.store(true, Ordering::Release);
    });

    dbg!(db.iterator(IteratorMode::Start).count());
}

fn make_storage(mode: Mode) -> DB {
    let storage_dir = Path::new("testing-store");
    if storage_dir.exists() {
        std::fs::remove_dir_all(storage_dir).expect("could not remove data dir");
    }

    let options = {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.enable_statistics();
        options.set_max_background_jobs(4);
        options.set_max_subcompactions(4);
        options
    };

    match mode {
        Mode::SingleColumnFamily => (),
        Mode::MultipleColumnFamilies => todo!(),
        Mode::MultipleDatabases => todo!(),
    }

    DB::open(&options, storage_dir).expect("Could not create database storage")
}

const PERSON: Type = Type { prefix: Prefix::Entity, id: TypeID { id: 0 } };
const FRIENDSHIP: Type = Type { prefix: Prefix::Relation, id: TypeID { id: 0 } };
const FRIEND: Type = Type { prefix: Prefix::Role, id: TypeID { id: 0 } };
const NAME: AttributeType =
    AttributeType { prefix: Prefix::Attribute, id: TypeID { id: 0 }, value_type: ValueType::Long };

fn agent(db: &DB, stop: &AtomicBool, batch_reads: bool, supernodes: &Vec<Attribute>) {
    while !stop.load(Ordering::Relaxed) {
        if batch_reads {
            todo!()
        } else {
            let name = Attribute { type_: NAME, value: thread_rng().gen() };
            let person = register_person(db, name);
            make_supernode_friendships(db, person, supernodes);
            // make_random_friendships(db, person, supernodes);
        }
    }
}

fn make_supernode_friendships(db: &DB, person: Thing, supernodes: &Vec<Attribute>) {
    let name = supernodes.choose(&mut thread_rng()).unwrap();
    if let Some(popular) = get_one_owner(name, db) {
        let rel = Thing { type_: FRIENDSHIP, thing_id: ThingID { id: thread_rng().gen() } };
        db.put(rel.as_bytes(), []).unwrap();

        let relates_edge = RelatesEdge { rel, role_type: FRIEND, player: popular };
        db.put(relates_edge.to_forward_bytes(), []).unwrap();
        db.put(relates_edge.to_backward_bytes(), []).unwrap();

        let relates_edge = RelatesEdge { rel, role_type: FRIEND, player: person };
        db.put(relates_edge.to_forward_bytes(), []).unwrap();
        db.put(relates_edge.to_backward_bytes(), []).unwrap();
    }
}

fn get_one_owner(name: &Attribute, db: &DB) -> Option<Thing> {
    let prefix = [name.as_bytes() as &[u8], &[EdgeType::Has as u8]].concat();
    db.iterator(IteratorMode::From(&prefix, Direction::Forward))
        .next()
        .and_then(Result::ok)
        .and_then(|(k, _)| <[u8; HasEdge::backward_encoding_size()]>::try_from(&*k).ok())
        .map(HasEdge::from_bytes_backward)
        .map(|HasEdge { owner, .. }| owner)
}

fn register_person(db: &DB, name: Attribute) -> Thing {
    db.put(name.as_bytes(), []).unwrap();
    // assume collisions unlikely
    let person = Thing { type_: PERSON, thing_id: ThingID { id: thread_rng().gen() } };
    db.put(person.as_bytes(), []).unwrap();

    let has_edge = HasEdge { owner: person, attr: name };
    db.put(has_edge.to_forward_bytes(), []).unwrap();
    db.put(has_edge.to_backward_bytes(), []).unwrap();

    person
}
