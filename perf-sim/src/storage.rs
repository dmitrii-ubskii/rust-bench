use std::path::Path;

use itertools::Itertools;
use rand::{thread_rng, Rng};
use speedb::{IteratorMode, Options, WriteBatch, DB};

use crate::{
    concept::{Attribute, EdgeType, HasEdge, RelatesEdge, RelationSiblingEdge, Thing, ThingID, Type},
    Mode,
};

pub struct Storage {
    db: DB,
}

impl Storage {
    pub fn new(storage_dir: &Path, mode: Mode) -> Self {
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

        Self { db: DB::open(&options, storage_dir).expect("Could not create database storage") }
    }

    pub fn get_one_owner(&self, attribute: &Attribute) -> Option<Thing> {
        let prefix = [attribute.as_bytes() as &[u8], &[EdgeType::Has as u8]].concat();
        self.db
            .prefix_iterator(&prefix)
            .next()
            .and_then(Result::ok)
            .and_then(|(k, _)| <[u8; HasEdge::backward_encoding_size()]>::try_from(&*k).ok())
            .map(HasEdge::from_bytes_backward)
            .map(|HasEdge { owner, .. }| owner)
    }

    pub fn get_random_sibling(&self, start: Thing, role_type: Type, relation_type: Type) -> Option<Thing> {
        let random_relation = Thing { type_: relation_type, thing_id: ThingID { id: 0 } };
        let prefix = [start.as_bytes() as &[u8], role_type.as_bytes(), random_relation.as_bytes()].concat();
        self.db
            .prefix_iterator(&prefix)
            .next()
            .and_then(Result::ok)
            .and_then(|(k, _)| <[u8; RelationSiblingEdge::encoding_size()]>::try_from(&*k).ok())
            .map(RelationSiblingEdge::from_bytes)
            .map(|RelationSiblingEdge { rhs_player, .. }| rhs_player)
    }

    pub fn commit(&self, write_handle: WriteHandle) {
        self.db.write_without_wal(write_handle.batch).unwrap();
    }

    pub fn print_stats(&self) {
        dbg!(self.db.iterator(IteratorMode::Start).count());
    }
}

#[derive(Default)]
pub struct WriteHandle {
    batch: WriteBatch,
}

impl WriteHandle {
    pub fn put_entity(&mut self, entity: Thing) {
        self.batch.put(entity.as_bytes(), []);
    }

    pub fn put_attribute(&mut self, attribute: Attribute) {
        self.batch.put(attribute.as_bytes(), []);
    }

    pub fn put_ownership(&mut self, owner: Thing, attribute: Attribute) {
        let has_edge = HasEdge { owner, attr: attribute };
        self.batch.put(has_edge.to_forward_bytes(), []);
        self.batch.put(has_edge.to_backward_bytes(), []);
    }

    pub fn put_relation(&mut self, rel: Thing, players: impl IntoIterator<Item = (Type, Thing)>) {
        self.batch.put(rel.as_bytes(), []);

        let players = players.into_iter().collect_vec();

        for &(role_type, player) in &players {
            let relates_edge = RelatesEdge { rel, role_type, player };
            self.batch.put(relates_edge.to_forward_bytes(), []);
            self.batch.put(relates_edge.to_backward_bytes(), []);
        }

        for ((lhs_role_type, lhs_player), (rhs_role_type, rhs_player)) in players.into_iter().tuple_combinations() {
            let shortcut_edge = RelationSiblingEdge { lhs_player, lhs_role_type, rel, rhs_role_type, rhs_player };
            self.batch.put(shortcut_edge.to_forward_bytes(), []);
            self.batch.put(shortcut_edge.to_backward_bytes(), []);
        }
    }
}
