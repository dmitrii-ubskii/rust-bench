#![allow(dead_code)]

use std::mem::{size_of, transmute};

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Prefix {
    Role,
    Entity,
    Relation,
    Attribute,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypeID {
    pub id: u16,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Type {
    pub prefix: Prefix,
    pub id: TypeID,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ThingID {
    pub id: u64,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Thing {
    pub type_: Type,
    pub thing_id: ThingID,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ValueType {
    Long,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct AttributeType {
    pub prefix: Prefix,
    pub id: TypeID,
    pub value_type: ValueType,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Attribute {
    pub type_: AttributeType,
    pub value: u64,
}

impl Attribute {
    pub fn as_bytes(&self) -> &[u8; size_of::<Self>()] {
        unsafe { transmute(self) }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EdgeType {
    Has, Relates,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct HasForwardEdge {
    pub owner: Thing,
    pub edge_type: EdgeType,
    pub attr: Attribute,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct HasBackwardEdge {
    pub attr: Attribute,
    pub edge_type: EdgeType,
    pub owner: Thing,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelatesForwardEdge {
    pub rel: Thing,
    pub edge_type: EdgeType,
    pub role_type: Type,
    pub player: Thing,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelatesBackwardEdge {
    pub player: Thing,
    pub edge_type: EdgeType,
    pub role_type: Type,
    pub rel: Thing,
}

macro_rules! bytes {
    ($($t:ty)*) => {$(
        impl $t {
            pub fn as_bytes(&self) -> &[u8; size_of::<Self>()] {
                unsafe { transmute(self) }
            }
        }
    )*};
}

bytes! {
    Thing
    Type

    HasBackwardEdge
    HasForwardEdge

    RelatesForwardEdge
    RelatesBackwardEdge
}
