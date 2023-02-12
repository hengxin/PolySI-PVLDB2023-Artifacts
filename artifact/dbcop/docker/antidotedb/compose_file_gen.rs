#!/usr/bin/env run-cargo-script
//! ```cargo
//! [dependencies]
//! serde_yaml = "*"
//! clap = "*"
//! ```

extern crate clap;
extern crate serde_yaml;

use std::fs::OpenOptions;

use clap::{App, Arg};

use serde_yaml::Value;

fn main() {
    let matches = App::new("compose_file")
        .version("1.0")
        .author("Ranadeep")
        .about("Generates docker-compose.yml for a given template.")
        .arg(
            Arg::with_name("template")
                .long("temp")
                .short("t")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("n_nodes")
                .long("nodes")
                .short("n")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let num: usize = matches.value_of("n_nodes").unwrap().parse().unwrap();
    let template_path = matches.value_of("template").unwrap();

    let mut spec: Value = {
        let mut template = OpenOptions::new()
            .read(true)
            .open(template_path)
            .expect("couldn't open");

        serde_yaml::from_reader(template).unwrap()
    };

    {
        let containers = &mut spec["services"];

        let template = serde_yaml::to_string(&containers).unwrap();

        match containers {
            Value::Mapping(ref mut maps) => {
                maps.clear();

                for i_num in { 1..num + 1 } {
                    let container_st = template.replace("{i}", &i_num.to_string());
                    match serde_yaml::from_str(&container_st) {
                        Ok(Value::Mapping(map)) => {
                            maps.extend(map.iter().map(|(k, v)| (k.clone(), v.clone())))
                        }
                        _ => unreachable!(),
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("docker-compose.yml")
            .expect("couldn't create");

        serde_yaml::to_writer(file, &spec).expect("failed to write docker-compose file");
    }

    println!("docker-compose file is ready.");
}
