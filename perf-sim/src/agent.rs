use std::sync::atomic::{AtomicBool, Ordering};

use rand::{thread_rng, Rng, seq::SliceRandom};

use crate::{
    concept::{Attribute, AttributeType, Prefix, Thing, ThingID, Type, TypeID, ValueType},
    storage::{Storage, WriteHandle},
};

pub const PERSON: Type = Type { prefix: Prefix::Entity, id: TypeID { id: 0 } };

pub const FRIENDSHIP: Type = Type { prefix: Prefix::Relation, id: TypeID { id: 0 } };

pub const FRIEND: Type = Type { prefix: Prefix::Role, id: TypeID { id: 0 } };

pub const NAME: AttributeType =
    AttributeType { prefix: Prefix::Attribute, id: TypeID { id: 0 }, value_type: ValueType::Long };

pub fn agent(storage: &Storage, stop: &AtomicBool, batch_reads: bool, supernodes: &Vec<Attribute>) {
    while !stop.load(Ordering::Relaxed) {
        let mut writer = storage.writer();

        if batch_reads {
            todo!()
        } else {
            let name = Attribute { type_: NAME, value: thread_rng().gen() };
            let person = register_person(&mut writer, name);
            make_supernode_friendships(storage, &mut writer, person, supernodes);
            make_random_friendships(storage, &mut writer, person, supernodes);
        }

        storage.commit(writer);
    }
}

pub fn make_supernode_friendships(
    storage: &Storage,
    writer: &mut WriteHandle,
    person: Thing,
    supernodes: &Vec<Attribute>,
) {
    let name = supernodes.choose(&mut thread_rng()).unwrap();
    if let Some(popular) = storage.get_one_owner(name) {
        let rel = Thing { type_: FRIENDSHIP, thing_id: ThingID { id: thread_rng().gen() } };
        writer.put_relation(rel, [(FRIEND, popular), (FRIEND, person)]);
    }
}

pub fn make_random_friendships(
    storage: &Storage,
    writer: &mut WriteHandle,
    person: Thing,
    supernodes: &Vec<Attribute>,
) {
    for _ in 0..5 {
        let name = supernodes.choose(&mut thread_rng()).unwrap();
        if let Some(popular) = storage.get_one_owner(name) {
            if let Some(rando) = storage.get_random_sibling(popular, FRIEND, FRIENDSHIP) {
                let rel = Thing { type_: FRIENDSHIP, thing_id: ThingID { id: thread_rng().gen() } };
                writer.put_relation(rel, [(FRIEND, rando), (FRIEND, person)]);
            }
        }
    }
}

pub fn register_person(writer: &mut WriteHandle, name: Attribute) -> Thing {
    writer.put_attribute(name);
    // assume collisions unlikely
    let person = Thing { type_: PERSON, thing_id: ThingID { id: thread_rng().gen() } };
    writer.put_entity(person);
    writer.put_ownership(person, name);
    person
}

