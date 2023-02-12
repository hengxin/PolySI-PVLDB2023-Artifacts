extern crate clap;
extern crate dbcop;
extern crate mysql;

use std::path::Path;

use dbcop::db::cluster::{Cluster, ClusterNode, Node};
use dbcop::db::history::{HistParams, Transaction};

use std::fs;

use clap::{App, Arg};

#[derive(Debug)]
pub struct GaleraNode {
    addr: String,
    id: usize,
}

impl From<Node> for GaleraNode {
    fn from(node: Node) -> Self {
        GaleraNode {
            addr: format!("mysql://{}@{}", "root", node.ip),
            id: node.id,
        }
    }
}

impl ClusterNode for GaleraNode {
    fn exec_session(&self, hist: &mut Vec<Transaction>) {
        match mysql::Pool::new(self.addr.clone()) {
            Ok(conn) => hist.iter_mut().for_each(|transaction| {
                for mut sqltxn in conn.start_transaction(
                    true,
                    Some(mysql::IsolationLevel::Serializable),
                    Some(false),
                ) {
                    transaction.events.iter_mut().for_each(|event| {
                        if event.write {
                            match sqltxn.prep_exec(
                                "UPDATE dbcop.variables SET val=? WHERE var=?",
                                (event.value, event.variable),
                            ) {
                                Ok(_) => event.success = true,
                                Err(_e) => {
                                    assert_eq!(event.success, false);
                                    // println!("WRITE ERR -- {:?}", _e);
                                }
                            }
                        } else {
                            match sqltxn.prep_exec(
                                "SELECT * FROM dbcop.variables WHERE var=?",
                                (event.variable,),
                            ) {
                                Ok(mut result) => {
                                    if let Some(q_result) = result.next() {
                                        let mut row = q_result.unwrap();
                                        if let Some(value) = row.take("val") {
                                            event.value = value;
                                            event.success = true;
                                        }
                                    } else {
                                        // may be diverged
                                        assert_eq!(event.success, false);
                                    }
                                }
                                Err(_e) => {
                                    // println!("READ ERR -- {:?}", _e);
                                    assert_eq!(event.success, false);
                                }
                            }
                        }
                    });
                    match sqltxn.commit() {
                        Ok(_) => {
                            transaction.success = true;
                        }
                        Err(_e) => {
                            assert_eq!(transaction.success, false);
                            println!("{:?} -- COMMIT ERROR {}", transaction, _e);
                        }
                    }
                }
            }),
            Err(_e) => {
                hist.iter().for_each(|transaction| {
                    assert_eq!(transaction.success, false);
                });
                // println!("CONNECTION ERROR {}", _e);}
            }
        }
    }
}

#[derive(Debug)]
pub struct GaleraCluster(Vec<Node>);

impl GaleraCluster {
    fn new(ips: &Vec<&str>) -> Self {
        GaleraCluster(GaleraCluster::node_vec(ips))
    }

    fn create_table(&self) -> bool {
        match self.get_mysql_addr(0) {
            Some(ip) => mysql::Pool::new(ip)
                .and_then(|pool| {
                    pool.prep_exec("CREATE DATABASE IF NOT EXISTS dbcop", ()).unwrap();
                    pool.prep_exec("DROP TABLE IF EXISTS dbcop.variables", ()).unwrap();
                    pool.prep_exec(
                        "CREATE TABLE IF NOT EXISTS dbcop.variables (var BIGINT(64) UNSIGNED NOT NULL PRIMARY KEY, val BIGINT(64) UNSIGNED NOT NULL)", ()
                    ).unwrap();
                    // conn.query("USE dbcop").unwrap();
                    Ok(true)
                }).expect("problem creating database"),
            _ => false,
        }
    }

    fn create_variables(&self, n_variable: usize) {
        if let Some(ip) = self.get_mysql_addr(0) {
            if let Ok(conn) = mysql::Pool::new(ip) {
                for mut stmt in conn
                    .prepare("INSERT INTO dbcop.variables (var, val) values (?, 0)")
                    .into_iter()
                {
                    (0..n_variable).for_each(|variable| {
                        stmt.execute((variable,)).unwrap();
                    });
                }
            }
        }
    }

    fn drop_database(&self) {
        if let Some(ip) = self.get_mysql_addr(0) {
            if let Ok(conn) = mysql::Pool::new(ip) {
                conn.prep_exec("DROP DATABASE dbcop", ()).unwrap();
            }
        }
    }

    fn get_mysql_addr(&self, i: usize) -> Option<String> {
        match self.0.get(i) {
            Some(ref node) => Some(format!("mysql://{}@{}", "root", node.ip)),
            None => None,
        }
    }
}

impl Cluster<GaleraNode> for GaleraCluster {
    fn n_node(&self) -> usize {
        self.0.len()
    }
    fn setup(&self) -> bool {
        self.create_table()
    }
    fn get_node(&self, id: usize) -> Node {
        self.0[id].clone()
    }
    fn get_cluster_node(&self, id: usize) -> GaleraNode {
        From::from(self.get_node(id))
    }
    fn setup_test(&mut self, p: &HistParams) {
        self.create_variables(p.get_n_variable());
    }
    fn cleanup(&self) {
        self.drop_database();
    }
    fn info(&self) -> String {
        "Galera".to_string()
    }
}

fn main() {
    let matches = App::new("Galera")
        .version("1.0")
        .author("Ranadeep")
        .about("executes histories on Galera")
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

    let mut cluster = GaleraCluster::new(&ips);

    cluster.execute_all(hist_dir, hist_out, 500);
}
