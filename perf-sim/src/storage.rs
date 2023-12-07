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
    Single {
        db: DB,
        cf: &'static ColumnFamily,
    },
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
}

/// SAFETY ???
unsafe impl Sync for Storage {}

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
            Mode::SingleColumnFamily => {
                let db = DB::open_cf(&options, storage_dir, [DEFAULT_COLUMN_FAMILY_NAME])
                    .expect("Could not create database storage");
                unsafe { Self::Single { cf: &*(db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).unwrap() as *const _), db } }
            }
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
            Mode::MultipleDatabases => todo!(),
        }
    }

    pub fn get_one_has(&self, owner: Thing) -> Option<Attribute> {
        let prefix = [owner.as_bytes() as &[u8], &[EdgeType::Has as u8]].concat();
        match self {
            Self::Single { db, .. } => db.prefix_iterator(&prefix),
            Self::MultipleColumnFamilies { db, has_forward_cf, .. } => db.prefix_iterator_cf(has_forward_cf, &prefix),
        }
        .next()
        .and_then(Result::ok)
        .and_then(|(k, _)| <[u8; HasEdge::forward_encoding_size()]>::try_from(&*k).ok())
        .map(HasEdge::from_bytes_forward)
        .map(|HasEdge { attr, .. }| attr)
    }

    pub fn get_one_owner(&self, attribute: &Attribute) -> Option<Thing> {
        let prefix = [attribute.as_bytes() as &[u8], &[EdgeType::Has as u8]].concat();
        match self {
            Self::Single { db, .. } => db.prefix_iterator(&prefix),
            Self::MultipleColumnFamilies { db, has_backward_cf, .. } => db.prefix_iterator_cf(has_backward_cf, &prefix),
        }
        .next()
        .and_then(Result::ok)
        .and_then(|(k, _)| <[u8; HasEdge::backward_encoding_size()]>::try_from(&*k).ok())
        .map(HasEdge::from_bytes_backward)
        .map(|HasEdge { owner, .. }| owner)
    }

    pub fn iter_siblings(
        &self,
        start: Thing,
        role_type: Type,
        relation_type: Type,
    ) -> impl Iterator<Item = Thing> + '_ {
        let prefix = [start.as_bytes() as &[u8], role_type.as_bytes(), relation_type.as_bytes()].concat();
        match self {
            Self::Single { db, .. } => db.prefix_iterator(&prefix),
            Self::MultipleColumnFamilies { db, relation_sibling_cf, .. } => {
                db.prefix_iterator_cf(relation_sibling_cf, &prefix)
            }
        }
        .filter_map(Result::ok)
        .filter_map(|(k, _)| <[u8; RelationSiblingEdge::encoding_size()]>::try_from(&*k).ok())
        .map(RelationSiblingEdge::from_bytes)
        .map(|RelationSiblingEdge { rhs_player, .. }| rhs_player)
    }

    pub fn get_random_sibling(&self, start: Thing, role_type: Type, relation_type: Type) -> Option<Thing> {
        let random_relation = Thing { type_: relation_type, thing_id: ThingID { id: thread_rng().gen() } };
        let prefix = [start.as_bytes() as &[u8], role_type.as_bytes(), random_relation.as_bytes()].concat();
        match self {
            Self::Single { db, .. } => db.prefix_iterator(&prefix),
            Self::MultipleColumnFamilies { db, relation_sibling_cf, .. } => {
                db.prefix_iterator_cf(relation_sibling_cf, &prefix)
            }
        }
        .next()
        .and_then(Result::ok)
        .and_then(|(k, _)| <[u8; RelationSiblingEdge::encoding_size()]>::try_from(&*k).ok())
        .map(RelationSiblingEdge::from_bytes)
        .map(|RelationSiblingEdge { rhs_player, .. }| rhs_player)
    }

    pub fn commit(&self, writer: WriteHandle) {
        match self {
            Self::Single { db, .. } | Self::MultipleColumnFamilies { db, .. } => {
                db.write_without_wal(writer.batch).unwrap()
            }
        }
    }

    pub fn print_stats(&self) {
        print!("Total keys in DB: ");
        match self {
            Self::Single { db, .. } => println!("{}", db.iterator(IteratorMode::Start).count()),
            Storage::MultipleColumnFamilies { db, .. } => {
                println!(
                    "{}",
                    CFS.iter()
                        .map(|cf| db.iterator_cf(db.cf_handle(cf).unwrap(), IteratorMode::Start).count())
                        .sum::<usize>()
                )
            }
        }
    }

    pub fn writer(&self) -> WriteHandle<'_> {
        WriteHandle { batch: WriteBatch::default(), storage: self }
    }

    fn thing_cf(&self) -> &ColumnFamily {
        match self {
            Self::Single { cf, .. } => cf,
            Self::MultipleColumnFamilies { thing_cf, .. } => thing_cf,
        }
    }

    fn attribute_cf(&self) -> &ColumnFamily {
        match self {
            Self::Single { cf, .. } => cf,
            Self::MultipleColumnFamilies { attribute_cf, .. } => attribute_cf,
        }
    }

    fn has_forward_cf(&self) -> &ColumnFamily {
        match self {
            Self::Single { cf, .. } => cf,
            Self::MultipleColumnFamilies { has_forward_cf, .. } => has_forward_cf,
        }
    }

    fn has_backward_cf(&self) -> &ColumnFamily {
        match self {
            Self::Single { cf, .. } => cf,
            Self::MultipleColumnFamilies { has_backward_cf, .. } => has_backward_cf,
        }
    }

    fn relates_forward_cf(&self) -> &ColumnFamily {
        match self {
            Self::Single { cf, .. } => cf,
            Self::MultipleColumnFamilies { relates_forward_cf, .. } => relates_forward_cf,
        }
    }

    fn relates_backward_cf(&self) -> &ColumnFamily {
        match self {
            Self::Single { cf, .. } => cf,
            Self::MultipleColumnFamilies { relates_backward_cf, .. } => relates_backward_cf,
        }
    }

    fn relation_sibling_cf(&self) -> &ColumnFamily {
        match self {
            Self::Single { cf, .. } => cf,
            Self::MultipleColumnFamilies { relation_sibling_cf, .. } => relation_sibling_cf,
        }
    }
}

pub struct WriteHandle<'a> {
    batch: WriteBatch,
    storage: &'a Storage,
}

impl WriteHandle<'_> {
    pub fn put_entity(&mut self, entity: Thing) {
        self.batch.put_cf(self.storage.thing_cf(), entity.as_bytes(), []);
    }

    pub fn put_attribute(&mut self, attribute: Attribute) {
        self.batch.put_cf(self.storage.attribute_cf(), attribute.as_bytes(), []);
    }

    pub fn put_ownership(&mut self, owner: Thing, attribute: Attribute) {
        let has_edge = HasEdge { owner, attr: attribute };
        self.batch.put_cf(self.storage.has_forward_cf(), has_edge.to_forward_bytes(), []);
        self.batch.put_cf(self.storage.has_backward_cf(), has_edge.to_backward_bytes(), []);
    }

    pub fn put_relation(&mut self, rel: Thing, players: impl IntoIterator<Item = (Type, Thing)>) {
        self.batch.put_cf(self.storage.thing_cf(), rel.as_bytes(), []);

        let players = players.into_iter().collect_vec();

        for &(role_type, player) in &players {
            let relates_edge = RelatesEdge { rel, role_type, player };
            self.batch.put_cf(self.storage.relates_forward_cf(), relates_edge.to_forward_bytes(), []);
            self.batch.put_cf(self.storage.relates_backward_cf(), relates_edge.to_backward_bytes(), []);
        }

        for ((lhs_role_type, lhs_player), (rhs_role_type, rhs_player)) in players.into_iter().tuple_combinations() {
            let shortcut_edge = RelationSiblingEdge { lhs_player, lhs_role_type, rel, rhs_role_type, rhs_player };
            self.batch.put_cf(self.storage.relation_sibling_cf(), shortcut_edge.to_forward_bytes(), []);
            self.batch.put_cf(self.storage.relation_sibling_cf(), shortcut_edge.to_backward_bytes(), []);
        }
    }
}
