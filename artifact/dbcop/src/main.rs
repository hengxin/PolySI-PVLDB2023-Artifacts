extern crate bincode;
extern crate chrono;
extern crate clap;
extern crate dbcop;
extern crate rayon;
extern crate serde_yaml;

// use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use clap::{App, AppSettings, Arg, SubCommand};
use std::fs::File;
use std::io::{BufReader, BufWriter};

use std::path::Path;

use std::fs;

use dbcop::db::history::generate_mult_histories;
use dbcop::db::history::History;
use dbcop::verifier::Verifier;

fn main() {
    let app = App::new("dbcop")
        .version("1.0")
        .author("Ranadeep")
        .about("Generates histories or verifies executed histories")
        .subcommands(vec![
            SubCommand::with_name("generate")
                .arg(
                    Arg::with_name("g_directory")
                        .long("gen_dir")
                        .short("d")
                        .takes_value(true)
                        .required(true)
                        .help("Directory to generate histories"),
                )
                .arg(
                    Arg::with_name("n_history")
                        .long("nhist")
                        .short("h")
                        .default_value("10")
                        .help("Number of histories to generate"),
                )
                .arg(
                    Arg::with_name("n_node")
                        .long("nnode")
                        .short("n")
                        .default_value("3")
                        .help("Number of nodes per history"),
                )
                .arg(
                    Arg::with_name("n_variable")
                        .long("nvar")
                        .short("v")
                        .default_value("5")
                        .help("Number of variables per history"),
                )
                .arg(
                    Arg::with_name("n_transaction")
                        .long("ntxn")
                        .short("t")
                        .default_value("5")
                        .help("Number of transactions per history"),
                )
                .arg(
                    Arg::with_name("n_event")
                        .long("nevt")
                        .short("e")
                        .default_value("2")
                        .help("Number of events per transactions"),
                )
                .about("Generate histories"),
            SubCommand::with_name("verify")
                .arg(
                    Arg::with_name("v_directory")
                        .long("ver_dir")
                        .short("d")
                        .takes_value(true)
                        .required(true)
                        .help("Directory containing executed histories"),
                )
                .arg(
                    Arg::with_name("o_directory")
                        .long("out_dir")
                        .short("o")
                        .takes_value(true)
                        .required(true)
                        .help("Directory to output the results"),
                )
                .arg(
                    Arg::with_name("sat")
                        .long("sat")
                        .help("Use MiniSAT as backend"),
                )
                .arg(
                    Arg::with_name("bicomponent")
                        .long("bic")
                        .help("Use BiComponent"),
                )
                .arg(
                    Arg::with_name("consistency")
                        .long("cons")
                        .short("c")
                        .takes_value(true)
                        .help("Check for mentioned consistency"),
                )
                .about("Verifies histories"),
        ])
        .setting(AppSettings::SubcommandRequired);

    let app_matches = app.get_matches();

    match app_matches.subcommand() {
        ("generate", Some(matches)) => {
            let dir = Path::new(matches.value_of("g_directory").unwrap());

            if !dir.is_dir() {
                fs::create_dir_all(dir).expect("failed to create directory");
            }

            let mut histories = generate_mult_histories(
                matches.value_of("n_history").unwrap().parse().unwrap(),
                matches.value_of("n_node").unwrap().parse().unwrap(),
                matches.value_of("n_variable").unwrap().parse().unwrap(),
                matches.value_of("n_transaction").unwrap().parse().unwrap(),
                matches.value_of("n_event").unwrap().parse().unwrap(),
            );

            for hist in histories.drain(..) {
                let file = File::create(dir.join(format!("hist-{:05}.bincode", hist.get_id())))
                    .expect("couldn't create bincode file");
                let buf_writer = BufWriter::new(file);
                bincode::serialize_into(buf_writer, &hist)
                    .expect("dumping history to bincode file went wrong");
            }
        }
        ("verify", Some(matches)) => {
            // let v_dir = Path::new(matches.value_of("v_directory").unwrap());
            //
            // let histories: Vec<History> = fs::read_dir(v_dir)
            //     .expect("couldn't read history directory")
            //     .filter_map(|entry_res| match entry_res {
            //         Ok(ref entry) if entry.path().is_dir() => {
            //             let file = File::open(entry.path().join("history.bincode")).unwrap();
            //             let buf_reader = BufReader::new(file);
            //             Some(bincode::deserialize_from(buf_reader).unwrap())
            //         }
            //         _ => None,
            //     })
            //     .collect();
            //
            // let o_dir = Path::new(matches.value_of("o_directory").unwrap());
            //
            // if !o_dir.is_dir() {
            //     fs::create_dir_all(o_dir).expect("failed to create directory");
            // }
            //
            // histories.iter().for_each(|ref hist| {
            //     let curr_dir = o_dir.join(format!("hist-{:05}", hist.get_id()));
            //
            //     let mut verifier = Verifier::new(curr_dir.to_path_buf());
            //
            //     if matches.is_present("causal") {
            //         verifier.model("cc");
            //     } else if matches.is_present("snapshot_isolation") {
            //         verifier.model("si");
            //     } else if matches.is_present("serializable") {
            //         verifier.model("ser");
            //     }
            //
            //     verifier.sat(matches.is_present("sat"));
            //     verifier.bicomponent(matches.is_present("bicomponent"));
            //
            //     if !verifier.transactional_history_verify(hist.get_data()) {
            //         println!("hist-{:05} failed", hist.get_id());
            //     } else {
            //         println!("hist-{:05} done", hist.get_id());
            //     }
            // });

            let v_path =
                Path::new(matches.value_of("v_directory").unwrap()).join("history.bincode");
            let file = File::open(v_path).unwrap();
            let buf_reader = BufReader::new(file);
            let hist: History = bincode::deserialize_from(buf_reader).unwrap();

            let o_dir = Path::new(matches.value_of("o_directory").unwrap());

            if !o_dir.is_dir() {
                fs::create_dir_all(o_dir).expect("failed to create directory");
            }

            // let curr_dir = o_dir.join(format!("hist-{:05}", hist.get_id()));

            let mut verifier = Verifier::new(o_dir.to_path_buf());

            match matches.value_of("consistency") {
                Some("cc") => verifier.model("cc"),
                Some("si") => verifier.model("si"),
                Some("ser") => verifier.model("ser"),
                None => verifier.model(""),
                _ => unreachable!(),
            };

            verifier.sat(matches.is_present("sat"));
            verifier.bicomponent(matches.is_present("bicomponent"));

            match verifier.verify(hist.get_data()) {
                Some(level) => println!(
                    "hist-{:05} failed - minimum level failed {:?}",
                    hist.get_id(),
                    level
                ),
                None => println!("hist-{:05} done", hist.get_id()),
            }
        }
        _ => unreachable!(),
    }
}
