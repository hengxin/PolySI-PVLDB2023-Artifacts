extern crate dbcop;

use dbcop::db::history::History;
use std::env;
use std::fs::File;
use std::io::BufReader;

fn main() {
    let file = File::open(env::args().nth(1).unwrap()).unwrap();
    let buf_reader = BufReader::new(file);
    let hist: History = bincode::deserialize_from(buf_reader).unwrap();
    println!(
        "{:?}",
        hist.get_duration().num_nanoseconds().unwrap() as f64 / 1_000_000_000f64
    );
}
