pub mod multiqueue {
    include!(concat!(env!("OUT_DIR"), "/multiqueue.rs"));
    include!(concat!(env!("OUT_DIR"), "/multiqueue.serde.rs"));
}
