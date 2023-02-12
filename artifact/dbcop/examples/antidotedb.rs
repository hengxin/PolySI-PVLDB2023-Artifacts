extern crate antidotedb;
extern crate byteorder;
extern crate clap;
extern crate dbcop;

use std::fs;
use std::path::Path;

use dbcop::db::cluster::{Cluster, ClusterNode, Node};
use dbcop::db::history::{HistParams, Transaction};

use clap::{App, Arg};

use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

use antidotedb::crdt::{Operation, LWWREG};
use antidotedb::AntidoteDB;

#[derive(Debug, Clone)]
pub struct AntidoteNode {
    node: Node,
    addr: String,
    id: usize,
    timestamp: Option<Vec<u8>>,
}

impl From<Node> for AntidoteNode {
    fn from(node: Node) -> Self {
        AntidoteNode {
            node: node.clone(),
            addr: format!("{}:8087", node.ip),
            id: node.id,
            timestamp: None,
        }
    }
}

impl ClusterNode for AntidoteNode {
    fn exec_session(&self, hist: &mut Vec<Transaction>) {
        let mut conn = AntidoteDB::connect_with_string(&self.addr);

        let mut timestamp = self.timestamp.clone();

        // println!("{:?}", timestamp);

        hist.iter_mut().for_each(|transaction| {
            let db_transaction = conn.start_transaction(timestamp.as_ref());

            transaction.events.iter_mut().for_each(|event| {
                let obj = LWWREG::new(&format!("{}", event.variable), "dbcop");
                if event.write {
                    let op = obj.set(event.value as u64);

                    match conn.mult_update_in_transaction(&[op], &db_transaction) {
                        Ok(_) => event.success = true,
                        Err(_e) => {
                            assert_eq!(event.success, false);
                            // println!("WRITE ERR -- {:?}", _e);
                        }
                    }
                } else {
                    match conn.mult_read_in_transaction(&[obj.clone()], &db_transaction) {
                        Ok(values) => {
                            let bytes = values[0].get_reg().get_value();
                            event.value =
                                Cursor::new(bytes).read_u64::<BigEndian>().unwrap() as usize;
                            event.success = true;
                        }
                        Err(_) => assert!(!event.success),
                    }
                }
            });

            match conn.commit_transaction(&db_transaction) {
                Ok(commit_time) => {
                    transaction.success = true;
                    timestamp = Some(commit_time);
                }
                Err(_e) => {
                    assert_eq!(transaction.success, false);
                    println!("{:?} -- COMMIT ERROR", transaction);
                }
            }
        })
    }
}

#[derive(Debug)]
pub struct AntidoteCluster(Vec<AntidoteNode>);

impl AntidoteCluster {
    fn new(ips: &Vec<&str>) -> Self {
        let mut v = AntidoteCluster::node_vec(ips);
        let k: Vec<_> = v.drain(..).map(|x| From::from(x)).collect();
        AntidoteCluster(k)
    }

    fn create_table(&self) -> bool {
        true
    }

    fn create_variables(&mut self, n_variable: usize) {
        let mut conn = AntidoteDB::connect_with_string(&self.get_antidote_addr(0).unwrap());

        let db_transaction = conn.start_transaction(None);

        let ops: Vec<_> = (0..n_variable)
            .map(|variable| LWWREG::new(&format!("{}", variable), "dbcop").set(0))
            .collect();

        conn.mult_update_in_transaction(&ops, &db_transaction)
            .expect("error to init zero values");

        match conn.commit_transaction(&db_transaction) {
            Ok(commit_time) => {
                self.0.iter_mut().for_each(|x| {
                    x.timestamp = Some(commit_time.clone());
                });
            }
            Err(_e) => {
                println!("COMMIT ERROR while init");
            }
        }

        self.0.iter_mut().for_each(|x| {
            let mut conn = AntidoteDB::connect_with_string(&x.addr);

            let timestamp = x.timestamp.clone();

            // println!("{:?}", timestamp);

            let db_transaction = conn.start_transaction(timestamp.as_ref());

            let objs: Vec<_> = (0..n_variable)
                .map(|variable| LWWREG::new(&format!("{}", variable), "dbcop"))
                .collect();

            match conn.mult_read_in_transaction(&objs, &db_transaction) {
                Ok(values) => assert!((0..n_variable).all(|var| {
                    let bytes = values[var].get_reg().get_value();
                    Cursor::new(bytes).read_u64::<BigEndian>().unwrap() == 0
                })),
                Err(_) => unreachable!(),
            }

            match conn.commit_transaction(&db_transaction) {
                Ok(commit_time) => {}
                Err(_e) => unreachable!(),
            }
        });

        // println!("zero init is done");
    }

    fn drop_database(&self) {}

    fn get_antidote_addr(&self, i: usize) -> Option<String> {
        self.0.get(i).map(|ref node| node.addr.clone())
    }
}

impl Cluster<AntidoteNode> for AntidoteCluster {
    fn n_node(&self) -> usize {
        self.0.len()
    }
    fn setup(&self) -> bool {
        self.create_table()
    }
    fn get_node(&self, id: usize) -> Node {
        self.0[id].node.clone()
    }
    fn get_cluster_node(&self, id: usize) -> AntidoteNode {
        self.0[id].clone()
    }
    fn setup_test(&mut self, p: &HistParams) {
        self.create_variables(p.get_n_variable());
    }
    fn cleanup(&self) {
        self.drop_database();
    }
    fn info(&self) -> String {
        "AntidoteDB".to_string()
    }
}

fn main() {
    let matches = App::new("Antidote")
        .version("1.0")
        .author("Ranadeep")
        .about("executes histories on AntidoteDB")
        .arg(
            Arg::with_name("hist_dir")
                .long("dir")
                .short("d")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("hist_out")
                .long("out")
                .short("o")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("ips")
                .help("Cluster ips")
                .multiple(true)
                .required(true),
        )
        .get_matches();

    let hist_dir = Path::new(matches.value_of("hist_dir").unwrap());
    let hist_out = Path::new(matches.value_of("hist_out").unwrap());

    fs::create_dir_all(hist_out).expect("couldn't create directory");

    let ips: Vec<_> = matches.values_of("ips").unwrap().collect();

    let mut cluster = AntidoteCluster::new(&ips);

    cluster.execute_all(hist_dir, hist_out, 50);
}
