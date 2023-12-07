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
    Has,
    Relates,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct HasEdge {
    pub owner: Thing,
    pub attr: Attribute,
}

impl HasEdge {
    pub fn to_forward_bytes(self) -> [u8; size_of::<HasForwardEdge>()] {
        let Self { owner, attr } = self;
        HasForwardEdge { owner, attr, edge_type: EdgeType::Has }.to_bytes()
    }

    pub fn to_backward_bytes(self) -> [u8; size_of::<HasBackwardEdge>()] {
        let Self { owner, attr } = self;
        HasForwardEdge { owner, attr, edge_type: EdgeType::Has }.to_bytes()
    }

    pub const fn backward_encoding_size() -> usize {
        size_of::<HasBackwardEdge>()
    }

    pub fn from_bytes_backward(bytes: [u8; size_of::<HasBackwardEdge>()]) -> Self {
        let HasBackwardEdge { attr, edge_type: _, owner } = unsafe { transmute(bytes) };
        Self { owner, attr }
    }
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct HasForwardEdge {
    pub owner: Thing,
    pub edge_type: EdgeType,
    pub attr: Attribute,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct HasBackwardEdge {
    pub attr: Attribute,
    pub edge_type: EdgeType,
    pub owner: Thing,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelatesEdge {
    pub rel: Thing,
    pub role_type: Type,
    pub player: Thing,
}

impl RelatesEdge {
    pub fn to_forward_bytes(self) -> [u8; size_of::<RelatesForwardEdge>()] {
        let Self { rel, role_type, player } = self;
        RelatesForwardEdge { rel, role_type, player, edge_type: EdgeType::Relates }.to_bytes()
    }

    pub fn to_backward_bytes(self) -> [u8; size_of::<RelatesBackwardEdge>()] {
        let Self { rel, role_type, player } = self;
        RelatesForwardEdge { rel, role_type, player, edge_type: EdgeType::Relates }.to_bytes()
    }
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct RelatesForwardEdge {
    pub rel: Thing,
    pub edge_type: EdgeType,
    pub role_type: Type,
    pub player: Thing,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct RelatesBackwardEdge {
    pub player: Thing,
    pub edge_type: EdgeType,
    pub role_type: Type,
    pub rel: Thing,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelationSiblingEdge {
    pub lhs_player: Thing,
    pub lhs_role_type: Type,
    pub rel: Thing,
    pub rhs_role_type: Type,
    pub rhs_player: Thing,
}

impl RelationSiblingEdge {
    pub fn to_forward_bytes(self) -> [u8; size_of::<Self>()] {
        self.to_bytes()
    }

    pub fn to_backward_bytes(self) -> [u8; size_of::<Self>()] {
        let Self { lhs_player, lhs_role_type, rel, rhs_role_type, rhs_player } = self;
        Self {
            lhs_player: rhs_player,
            lhs_role_type: rhs_role_type,
            rel,
            rhs_role_type: lhs_role_type,
            rhs_player: lhs_player,
        }
        .to_bytes()
    }
}

macro_rules! bytes {
    ($($t:ty)*) => {$(
        impl $t {
            pub fn as_bytes(&self) -> &[u8; size_of::<Self>()] {
                unsafe { transmute(self) }
            }
            pub fn to_bytes(self) -> [u8; size_of::<Self>()] {
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

    RelationSiblingEdge
}
