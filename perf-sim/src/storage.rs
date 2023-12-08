use std::path::Path;

use itertools::Itertools;
use rand::{thread_rng, Rng};
use speedb::{ColumnFamily, IteratorMode, Options, WriteBatch, DB, DEFAULT_COLUMN_FAMILY_NAME};

use crate::{
    concept::{Attribute, EdgeType, HasEdge, RelatesEdge, RelationSiblingEdge, Thing, ThingID, Type},
    Mode,
};

const THING: &str = "thing";
const ATTRIBUTE: &str = "attribute";
const HAS_FORWARD: &str = "has_forward";
const HAS_BACKWARD: &str = "has_backward";
const RELATES_FORWARD: &str = "relates_forward";
const RELATES_BACKWARD: &str = "relates_backward";
const RELATION_SIBLING: &str = "relation_sibling";
const CFS: [&str; 7] =
    [THING, ATTRIBUTE, HAS_FORWARD, HAS_BACKWARD, RELATES_FORWARD, RELATES_BACKWARD, RELATION_SIBLING];

pub enum Storage {
    Single(SingleDB),
    MultipleColumnFamilies {
        db: DB,
        thing_cf: &'static ColumnFamily,
        attribute_cf: &'static ColumnFamily,
        has_forward_cf: &'static ColumnFamily,
        has_backward_cf: &'static ColumnFamily,
        relates_forward_cf: &'static ColumnFamily,
        relates_backward_cf: &'static ColumnFamily,
        relation_sibling_cf: &'static ColumnFamily,
    },
    MultipleDatabases {
        thing_db: SingleDB,
        attribute_db: SingleDB,
        has_forward_db: SingleDB,
        has_backward_db: SingleDB,
        relates_forward_db: SingleDB,
        relates_backward_db: SingleDB,
        relation_sibling_db: SingleDB,
    },
}

pub struct SingleDB {
    db: DB,
    cf: &'static ColumnFamily,
}

impl SingleDB {
    fn open(options: &Options, storage_dir: &Path) -> Self {
        let db =
            DB::open_cf(options, storage_dir, [DEFAULT_COLUMN_FAMILY_NAME]).expect("Could not create database storage");
        unsafe { Self { cf: &*(db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).unwrap() as *const _), db } }
    }
}

/// SAFETY ???
unsafe impl Sync for Storage {}

fn exact_prefix_iterator_cf<'a>(
    db: &'a DB,
    cf: &ColumnFamily,
    prefix: Vec<u8>,
) -> impl Iterator<Item = std::boxed::Box<[u8]>> + 'a {
    db.prefix_iterator_cf(cf, &prefix)
        .filter_map(Result::ok)
        .take_while(move |(k, _)| k.len() >= prefix.len() && k[0..prefix.len()] == prefix)
        .map(|(k, _)| k)
}

fn exact_prefix_iterator_cf_from<'a>(
    db: &'a DB,
    cf: &ColumnFamily,
    prefix: Vec<u8>,
    start: &[u8],
) -> impl Iterator<Item = std::boxed::Box<[u8]>> + 'a {
    db.prefix_iterator_cf(cf, start)
        .filter_map(Result::ok)
        .take_while(move |(k, _)| k.len() >= prefix.len() && k[0..prefix.len()] == prefix)
        .map(|(k, _)| k)
}

impl Storage {
    pub fn new(storage_dir: &Path, mode: Mode) -> Self {
        if storage_dir.exists() {
            std::fs::remove_dir_all(storage_dir).expect("could not remove data dir");
        }

        let options = {
            let mut options = Options::default();
            options.create_if_missing(true);
            options.create_missing_column_families(true);
            options.enable_statistics();
            options.set_max_background_jobs(4);
            options.set_max_subcompactions(4);
            options
        };

        match mode {
            Mode::SingleColumnFamily => Self::Single(SingleDB::open(&options, storage_dir)),
            Mode::MultipleColumnFamilies => {
                let db = DB::open_cf(&options, storage_dir, CFS).expect("Could not create database storage");
                unsafe {
                    Self::MultipleColumnFamilies {
                        thing_cf: &*(db.cf_handle(THING).unwrap() as *const _),
                        attribute_cf: &*(db.cf_handle(ATTRIBUTE).unwrap() as *const _),
                        has_forward_cf: &*(db.cf_handle(HAS_FORWARD).unwrap() as *const _),
                        has_backward_cf: &*(db.cf_handle(HAS_BACKWARD).unwrap() as *const _),
                        relates_forward_cf: &*(db.cf_handle(RELATES_FORWARD).unwrap() as *const _),
                        relates_backward_cf: &*(db.cf_handle(RELATES_BACKWARD).unwrap() as *const _),
                        relation_sibling_cf: &*(db.cf_handle(RELATION_SIBLING).unwrap() as *const _),
                        db,
                    }
                }
            }
            Mode::MultipleDatabases => Self::MultipleDatabases {
                thing_db: SingleDB::open(&options, &storage_dir.join("thing")),
                attribute_db: SingleDB::open(&options, &storage_dir.join("attribute")),
                has_forward_db: SingleDB::open(&options, &storage_dir.join("has_forward")),
                has_backward_db: SingleDB::open(&options, &storage_dir.join("has_backward")),
                relates_forward_db: SingleDB::open(&options, &storage_dir.join("relates_forward")),
                relates_backward_db: SingleDB::open(&options, &storage_dir.join("relates_backward")),
                relation_sibling_db: SingleDB::open(&options, &storage_dir.join("relation_sibling")),
            },
        }
    }

    #[allow(dead_code)]
    pub fn get_one_has(&self, owner: Thing) -> Option<Attribute> {
        let prefix = [owner.as_bytes() as &[u8], &[EdgeType::Has as u8]].concat();
        let (db, cf) = match self {
            Self::Single(SingleDB { db, cf }) => (db, cf),
            Self::MultipleColumnFamilies { db, has_forward_cf, .. } => (db, has_forward_cf),
            Self::MultipleDatabases { has_forward_db: SingleDB { db, cf }, .. } => (db, cf),
        };
        exact_prefix_iterator_cf(db, cf, prefix)
            .next()
            .and_then(|k| <[u8; HasEdge::forward_encoding_size()]>::try_from(&*k).ok())
            .map(HasEdge::from_bytes_forward)
            .map(|HasEdge { attr, .. }| attr)
    }

    pub fn get_one_owner(&self, attribute: &Attribute) -> Option<Thing> {
        let prefix = [attribute.as_bytes() as &[u8], &[EdgeType::Has as u8]].concat();
        let (db, cf) = match self {
            Self::Single(SingleDB { db, cf }) => (db, cf),
            Self::MultipleColumnFamilies { db, has_backward_cf, .. } => (db, has_backward_cf),
            Self::MultipleDatabases { has_backward_db: SingleDB { db, cf }, .. } => (db, cf),
        };
        exact_prefix_iterator_cf(db, cf, prefix)
            .next()
            .and_then(|k| <[u8; HasEdge::backward_encoding_size()]>::try_from(&*k).ok())
            .map(HasEdge::from_bytes_backward)
            .map(|HasEdge { owner, .. }| owner)
    }

    #[allow(dead_code)]
    pub fn iter_siblings(
        &self,
        start: Thing,
        role_type: Type,
        relation_type: Type,
    ) -> impl Iterator<Item = Thing> + '_ {
        let prefix =
            [start.as_bytes() as &[u8], &[EdgeType::Sibling as u8], role_type.as_bytes(), relation_type.as_bytes()]
                .concat();
        let (db, cf) = match self {
            Self::Single(SingleDB { db, cf }) => (db, cf),
            Self::MultipleColumnFamilies { db, relation_sibling_cf, .. } => (db, relation_sibling_cf),
            Self::MultipleDatabases { relation_sibling_db: SingleDB { db, cf }, .. } => (db, cf),
        };
        exact_prefix_iterator_cf(db, cf, prefix)
            .filter_map(|k| <[u8; RelationSiblingEdge::encoding_size()]>::try_from(&*k).ok())
            .map(RelationSiblingEdge::from_bytes)
            .map(|RelationSiblingEdge { rhs_player, .. }| rhs_player)
    }

    pub fn get_random_sibling(&self, start: Thing, role_type: Type, relation_type: Type) -> Option<Thing> {
        let random_relation = Thing { type_: relation_type, thing_id: ThingID { id: thread_rng().gen() } };
        let prefix = [start.as_bytes() as &[u8], role_type.as_bytes(), random_relation.as_bytes()].concat();
        let start = [&prefix as &[u8], &thread_rng().gen::<usize>().to_be_bytes()].concat();
        let (db, cf) = match self {
            Self::Single(SingleDB { db, cf }) => (db, cf),
            Self::MultipleColumnFamilies { db, relation_sibling_cf, .. } => (db, relation_sibling_cf),
            Self::MultipleDatabases { relation_sibling_db: SingleDB { db, cf }, .. } => (db, cf),
        };
        exact_prefix_iterator_cf_from(db, cf, prefix, &start)
            .next()
            .and_then(|k| <[u8; RelationSiblingEdge::encoding_size()]>::try_from(&*k).ok())
            .map(RelationSiblingEdge::from_bytes)
            .map(|RelationSiblingEdge { rhs_player, .. }| rhs_player)
    }

    pub fn commit(&self, writer: WriteHandle) {
        match self {
            Self::Single(SingleDB { db, .. }) | Self::MultipleColumnFamilies { db, .. } => {
                let WriteHandle::Single { batch, .. } = writer else { unreachable!() };
                db.write_without_wal(batch).unwrap()
            }
            Self::MultipleDatabases {
                thing_db,
                attribute_db,
                has_forward_db,
                has_backward_db,
                relates_forward_db,
                relates_backward_db,
                relation_sibling_db,
            } => {
                let WriteHandle::Multi {
                    thing_batch,
                    attribute_batch,
                    has_forward_batch,
                    has_backward_batch,
                    relates_forward_batch,
                    relates_backward_batch,
                    relation_sibling_batch,
                    ..
                } = writer
                else {
                    unreachable!()
                };
                thing_db.db.write_without_wal(thing_batch).unwrap();
                attribute_db.db.write_without_wal(attribute_batch).unwrap();
                has_forward_db.db.write_without_wal(has_forward_batch).unwrap();
                has_backward_db.db.write_without_wal(has_backward_batch).unwrap();
                relates_forward_db.db.write_without_wal(relates_forward_batch).unwrap();
                relates_backward_db.db.write_without_wal(relates_backward_batch).unwrap();
                relation_sibling_db.db.write_without_wal(relation_sibling_batch).unwrap();
            }
        }
    }

    pub fn print_stats(&self) {
        let total = match self {
            Self::Single(SingleDB { db, cf }) => db.iterator_cf(cf, IteratorMode::Start).count(),
            Self::MultipleColumnFamilies { db, .. } => CFS
                .iter()
                .map(|cf| db.iterator_cf(db.cf_handle(cf).unwrap(), IteratorMode::Start).count())
                .sum::<usize>(),
            Self::MultipleDatabases {
                thing_db,
                attribute_db,
                has_forward_db,
                has_backward_db,
                relates_forward_db,
                relates_backward_db,
                relation_sibling_db,
            } => [
                thing_db,
                attribute_db,
                has_forward_db,
                has_backward_db,
                relates_forward_db,
                relates_backward_db,
                relation_sibling_db,
            ]
            .into_iter()
            .map(|SingleDB { db, cf }| db.iterator_cf(cf, IteratorMode::Start).count())
            .sum(),
        };
        println!("Total keys in DB: {total}")
    }

    pub fn writer(&self) -> WriteHandle<'_> {
        match self {
            Self::Single(_) | Self::MultipleColumnFamilies { .. } => {
                WriteHandle::Single { batch: WriteBatch::default(), storage: self }
            }
            Self::MultipleDatabases { .. } => WriteHandle::Multi {
                thing_batch: WriteBatch::default(),
                attribute_batch: WriteBatch::default(),
                has_forward_batch: WriteBatch::default(),
                has_backward_batch: WriteBatch::default(),
                relates_forward_batch: WriteBatch::default(),
                relates_backward_batch: WriteBatch::default(),
                relation_sibling_batch: WriteBatch::default(),
                storage: self,
            },
        }
    }
}

pub enum WriteHandle<'a> {
    Single {
        batch: WriteBatch,
        storage: &'a Storage,
    },
    Multi {
        thing_batch: WriteBatch,
        attribute_batch: WriteBatch,
        has_forward_batch: WriteBatch,
        has_backward_batch: WriteBatch,
        relates_forward_batch: WriteBatch,
        relates_backward_batch: WriteBatch,
        relation_sibling_batch: WriteBatch,
        storage: &'a Storage,
    },
}

impl WriteHandle<'_> {
    pub fn put_entity(&mut self, entity: Thing) {
        self.thing_batch_put(entity.as_bytes());
    }

    pub fn put_attribute(&mut self, attribute: Attribute) {
        self.attribute_batch_put(attribute.as_bytes());
    }

    pub fn put_ownership(&mut self, owner: Thing, attribute: Attribute) {
        let has_edge = HasEdge { owner, attr: attribute };
        self.has_forward_batch_put(&has_edge.to_forward_bytes());
        self.has_backward_batch_put(&has_edge.to_backward_bytes());
    }

    pub fn put_relation(&mut self, rel: Thing, players: impl IntoIterator<Item = (Type, Thing)>) {
        self.thing_batch_put(rel.as_bytes());

        let players = players.into_iter().collect_vec();

        for &(role_type, player) in &players {
            let relates_edge = RelatesEdge { rel, role_type, player };
            self.relates_forward_batch_put(&relates_edge.to_forward_bytes());
            self.relates_backward_batch_put(&relates_edge.to_backward_bytes());
        }

        for ((lhs_role_type, lhs_player), (rhs_role_type, rhs_player)) in players.into_iter().tuple_combinations() {
            let shortcut_edge = RelationSiblingEdge { lhs_player, lhs_role_type, rel, rhs_role_type, rhs_player };
            self.relation_sibling_batch_put(&shortcut_edge.to_forward_bytes());
            self.relation_sibling_batch_put(&shortcut_edge.to_backward_bytes());
        }
    }

    fn thing_batch_put(&mut self, key: &[u8]) {
        match self {
            WriteHandle::Single {
                batch,
                storage: Storage::Single(SingleDB { cf, .. }) | Storage::MultipleColumnFamilies { thing_cf: cf, .. },
            } => batch.put_cf(cf, key, []),
            WriteHandle::Multi {
                thing_batch,
                storage: Storage::MultipleDatabases { thing_db: SingleDB { cf, .. }, .. },
                ..
            } => thing_batch.put_cf(cf, key, []),
            _ => unreachable!(),
        }
    }

    fn attribute_batch_put(&mut self, key: &[u8]) {
        match self {
            WriteHandle::Single {
                batch,
                storage: Storage::Single(SingleDB { cf, .. }) | Storage::MultipleColumnFamilies { attribute_cf: cf, .. },
            } => batch.put_cf(cf, key, []),
            WriteHandle::Multi {
                attribute_batch,
                storage: Storage::MultipleDatabases { attribute_db: SingleDB { cf, .. }, .. },
                ..
            } => attribute_batch.put_cf(cf, key, []),
            _ => unreachable!(),
        }
    }

    fn has_forward_batch_put(&mut self, key: &[u8]) {
        match self {
            WriteHandle::Single {
                batch,
                storage:
                    Storage::Single(SingleDB { cf, .. }) | Storage::MultipleColumnFamilies { has_forward_cf: cf, .. },
            } => batch.put_cf(cf, key, []),
            WriteHandle::Multi {
                has_forward_batch,
                storage: Storage::MultipleDatabases { has_forward_db: SingleDB { cf, .. }, .. },
                ..
            } => has_forward_batch.put_cf(cf, key, []),
            _ => unreachable!(),
        }
    }

    fn has_backward_batch_put(&mut self, key: &[u8]) {
        match self {
            WriteHandle::Single {
                batch,
                storage:
                    Storage::Single(SingleDB { cf, .. }) | Storage::MultipleColumnFamilies { has_backward_cf: cf, .. },
            } => batch.put_cf(cf, key, []),
            WriteHandle::Multi {
                has_backward_batch,
                storage: Storage::MultipleDatabases { has_backward_db: SingleDB { cf, .. }, .. },
                ..
            } => has_backward_batch.put_cf(cf, key, []),
            _ => unreachable!(),
        }
    }

    fn relates_forward_batch_put(&mut self, key: &[u8]) {
        match self {
            WriteHandle::Single {
                batch,
                storage:
                    Storage::Single(SingleDB { cf, .. }) | Storage::MultipleColumnFamilies { relates_forward_cf: cf, .. },
            } => batch.put_cf(cf, key, []),
            WriteHandle::Multi {
                relates_forward_batch,
                storage: Storage::MultipleDatabases { relates_forward_db: SingleDB { cf, .. }, .. },
                ..
            } => relates_forward_batch.put_cf(cf, key, []),
            _ => unreachable!(),
        }
    }

    fn relates_backward_batch_put(&mut self, key: &[u8]) {
        match self {
            WriteHandle::Single {
                batch,
                storage:
                    Storage::Single(SingleDB { cf, .. }) | Storage::MultipleColumnFamilies { relates_backward_cf: cf, .. },
            } => batch.put_cf(cf, key, []),
            WriteHandle::Multi {
                relates_backward_batch,
                storage: Storage::MultipleDatabases { relates_backward_db: SingleDB { cf, .. }, .. },
                ..
            } => relates_backward_batch.put_cf(cf, key, []),
            _ => unreachable!(),
        }
    }

    fn relation_sibling_batch_put(&mut self, key: &[u8]) {
        match self {
            WriteHandle::Single {
                batch,
                storage:
                    Storage::Single(SingleDB { cf, .. }) | Storage::MultipleColumnFamilies { relation_sibling_cf: cf, .. },
            } => batch.put_cf(cf, key, []),
            WriteHandle::Multi {
                relation_sibling_batch,
                storage: Storage::MultipleDatabases { relation_sibling_db: SingleDB { cf, .. }, .. },
                ..
            } => relation_sibling_batch.put_cf(cf, key, []),
            _ => unreachable!(),
        }
    }
}
