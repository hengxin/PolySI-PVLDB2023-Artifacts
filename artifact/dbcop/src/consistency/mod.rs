pub mod algo;
pub mod sat;
pub mod util;

#[derive(Debug, Clone, Copy)]
pub enum Consistency {
    ReadCommitted,
    RepeatableRead,
    ReadAtomic,
    Causal,
    Prefix,
    SnapshotIsolation,
    Serializable,
    Inc,
}
