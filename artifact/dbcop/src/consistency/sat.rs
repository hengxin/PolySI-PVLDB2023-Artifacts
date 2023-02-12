use hashbrown::{HashMap, HashSet};
use std::fs;
use std::fs::{File, OpenOptions};

use std::default::Default;

use std::process::{Command, Stdio};

use std::path::PathBuf;

use std::io::BufRead;
use std::io::{BufReader, BufWriter};

use std::io::Write;

#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Debug)]
pub enum Edge {
    CO,
    VI,
}

#[derive(Debug)]
struct CNF {
    clauses: Vec<Vec<(bool, usize)>>,
    n_variable: usize,
}

impl Default for CNF {
    fn default() -> Self {
        CNF {
            clauses: vec![Vec::new()],
            n_variable: 0,
        }
    }
}

impl CNF {
    fn add_variable(&mut self, var: usize, sign: bool) {
        self.n_variable = std::cmp::max(self.n_variable, var);
        self.clauses.last_mut().unwrap().push((sign, var));
    }

    fn finish_clause(&mut self) {
        self.clauses.push(Vec::new());
    }

    fn write_to_file(&mut self, path: &PathBuf) {
        let mut file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .expect("couldn't create"),
        );

        writeln!(file, "p cnf {} {}", self.n_variable, self.clauses.len() - 1)
            .expect("failed to write parameters");
        for clause in self.clauses.drain(..).rev().skip(1) {
            for (sign, literal) in clause {
                if sign {
                    write!(file, "{} ", literal).expect("failed to write cnf to file");
                } else {
                    write!(file, "-{} ", literal).expect("failed to write cnf to file");
                }
            }
            writeln!(file, "0").expect("failed to write cnf to file");
        }
    }
}

#[derive(Debug)]
pub struct Sat {
    cnf: CNF,
    edge_variable: HashMap<(Edge, (usize, usize), (usize, usize)), usize>,
    write_variable: HashMap<usize, HashMap<(usize, usize), HashSet<(usize, usize)>>>,
    transactions: Vec<(usize, usize)>,
}

impl Sat {
    pub fn new(
        txns_info: &HashMap<(usize, usize), (HashMap<usize, (usize, usize)>, HashSet<usize>)>,
    ) -> Self {
        let mut write_variable: HashMap<usize, HashMap<(usize, usize), HashSet<(usize, usize)>>> =
            HashMap::new();

        for (&transaction1, (ref read_info, write_info)) in txns_info.iter() {
            for &x in write_info.iter() {
                let entry = write_variable.entry(x).or_insert_with(Default::default);
                entry.entry(transaction1).or_insert_with(Default::default);
            }
            for (&x, &transaction2) in read_info.iter() {
                let entry1 = write_variable.entry(x).or_insert_with(Default::default);
                let entry2 = entry1.entry(transaction2).or_insert_with(Default::default);
                entry2.insert(transaction1);
            }
        }

        for (_, wr_map) in write_variable.iter_mut() {
            wr_map.entry((0, 0)).or_insert_with(Default::default);
        }

        let mut transactions: Vec<_> = txns_info.keys().cloned().collect();
        transactions.sort_unstable();

        Sat {
            cnf: Default::default(),
            edge_variable: HashMap::new(),
            write_variable,
            transactions,
        }
    }

    pub fn session(&mut self) {
        let mut clauses = Vec::new();

        for id in self.transactions.windows(2) {
            clauses.push(vec![(
                Edge::VI,
                if id[0].0 == id[1].0 { id[0] } else { (0, 0) },
                id[1],
                true,
            )])
        }

        self.add_clauses(&clauses);
    }

    pub fn pre_vis_co(&mut self) {
        let mut clauses = Vec::new();

        for &t1 in self.transactions.iter() {
            for &t2 in self.transactions.iter() {
                if t1 != t2 {
                    // VIS <= CO
                    clauses.push(vec![(Edge::VI, t1, t2, false), (Edge::CO, t1, t2, true)]);

                    // CO total
                    // no cycle
                    clauses.push(vec![(Edge::CO, t1, t2, false), (Edge::CO, t2, t1, false)]);
                    // total
                    clauses.push(vec![(Edge::CO, t1, t2, true), (Edge::CO, t2, t1, true)]);

                    for &t3 in self.transactions.iter() {
                        if t2 != t3 && t1 != t3 {
                            // CO transitive / CO;CO => CO
                            clauses.push(vec![
                                (Edge::CO, t1, t2, false),
                                (Edge::CO, t2, t3, false),
                                (Edge::CO, t1, t3, true),
                            ]);
                        }
                    }
                }
            }
        }
        self.add_clauses(&clauses);
    }

    pub fn ser(&mut self) {
        let mut clauses = Vec::new();

        for &t1 in self.transactions.iter() {
            for &t2 in self.transactions.iter() {
                if t1 != t2 {
                    // CO <= VIS
                    clauses.push(vec![(Edge::CO, t1, t2, false), (Edge::VI, t1, t2, true)]);
                }
            }
        }
        self.add_clauses(&clauses);
    }

    pub fn vis_transitive(&mut self) {
        let mut clauses = Vec::new();

        for &t1 in self.transactions.iter() {
            for &t2 in self.transactions.iter() {
                if t1 != t2 {
                    for &t3 in self.transactions.iter() {
                        if t2 != t3 && t1 != t3 {
                            // VI transitive / VI;VI => VI
                            clauses.push(vec![
                                (Edge::VI, t1, t2, false),
                                (Edge::VI, t2, t3, false),
                                (Edge::VI, t1, t3, true),
                            ]);
                        }
                    }
                }
            }
        }
        self.add_clauses(&clauses);
    }

    pub fn wr(&mut self) {
        let mut clauses = Vec::new();

        for (_, ref wr_map) in self.write_variable.iter() {
            for (&u1, ref vs) in wr_map.iter() {
                for &v in vs.iter() {
                    // clauses.push(vec![(Edge::WR(x), u1, v, true)]);
                    clauses.push(vec![(Edge::VI, u1, v, true)]);
                }
            }
        }

        self.add_clauses(&clauses);
    }

    pub fn read_atomic(&mut self) {
        let mut clauses = Vec::new();

        for (_, ref wr_map) in self.write_variable.iter() {
            for (&u1, ref vs) in wr_map.iter() {
                for &v in vs.iter() {
                    for (&u2, _) in wr_map.iter() {
                        if u2 != u1 && u2 != v {
                            clauses.push(vec![(Edge::VI, u2, v, false), (Edge::CO, u2, u1, true)]);
                        }
                    }
                }
            }
        }

        self.add_clauses(&clauses);
    }

    pub fn prefix(&mut self) {
        let mut clauses = Vec::new();

        for &t1 in self.transactions.iter() {
            for &t2 in self.transactions.iter() {
                if t1 != t2 {
                    for &t3 in self.transactions.iter() {
                        if t2 != t3 && t1 != t3 {
                            // CO;VI => VI
                            clauses.push(vec![
                                (Edge::CO, t1, t2, false),
                                (Edge::VI, t2, t3, false),
                                (Edge::VI, t1, t3, true),
                            ]);
                        }
                    }
                }
            }
        }
        self.add_clauses(&clauses);
    }

    pub fn conflict(&mut self) {
        let mut clauses = Vec::new();
        for (_, ref wr_map) in self.write_variable.iter() {
            for (&u1, _) in wr_map.iter() {
                for (&u2, _) in wr_map.iter() {
                    if u1 != u2 {
                        clauses.push(vec![(Edge::CO, u1, u2, false), (Edge::VI, u1, u2, true)]);
                    }
                }
            }
        }
        self.add_clauses(&clauses);
    }

    pub fn solve(&mut self, path: &PathBuf) -> Option<Vec<(usize, usize)>> {
        let inp_cnf = path.join("history.cnf");
        let out_cnf = path.join("result.cnf");
        self.cnf.write_to_file(&inp_cnf);

        if let Ok(mut child) = Command::new("minisat")
            .arg(&inp_cnf)
            .arg(&out_cnf)
            .stdout(Stdio::null())
            .spawn()
        {
            child.wait().expect("failed to execute process");
        } else {
            panic!("failed to execute process")
        }

        fs::remove_file(inp_cnf).expect("couldn't delete input cnf");

        // println!("status: {}", output.status);
        // println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        // println!("stderr: {}", String::from_utf8_lossy(&output.stderr));

        let result = File::open(&out_cnf).expect("file couldn't open");

        let reader = BufReader::new(&result);

        let mut lines = reader.lines().map(|l| l.unwrap());

        let mut assignments = HashMap::new();

        match lines.next() {
            Some(ref e) if e.as_str() == "SAT" => {
                for line in lines {
                    for var_st in line.split_whitespace() {
                        let var: isize = var_st.parse().unwrap();
                        if var != 0 {
                            assignments.insert(var.abs() as usize, var > 0);
                        }
                    }
                }
            }
            Some(ref e) if e.as_str() == "UNSAT" => {
                // println!("{:?}", e);
                // for line in lines {
                //     println!("{}", line);
                // }
            }
            _ => {
                unreachable!();
            }
        }

        if !assignments.is_empty() {
            let edges: Vec<_> = self
                .edge_variable
                .iter()
                .filter_map(|(&k, &v)| {
                    if k.0 == Edge::CO {
                        assert!(k.1 != k.2);
                        Some(if assignments[&v] {
                            (k.1, k.2)
                        } else {
                            (k.2, k.1)
                        })
                    } else {
                        None
                    }
                })
                .collect();

            // edges.sort_unstable();

            // building co
            let mut parents: HashMap<(usize, usize), HashSet<(usize, usize)>> = Default::default();
            for e in &edges {
                parents
                    .entry(e.1)
                    .or_insert_with(Default::default)
                    .insert(e.0);

                parents.entry(e.0).or_insert_with(Default::default);
            }

            let mut lin = Vec::new();

            while !parents.is_empty() {
                let next_t: Vec<_> = parents
                    .iter()
                    .filter_map(|(t1, t2s)| if t2s.is_empty() { Some(*t1) } else { None })
                    .collect();
                assert_eq!(next_t.len(), 1);

                parents.retain(|_, t2s| !t2s.is_empty());

                for (_, t2s) in parents.iter_mut() {
                    t2s.remove(&next_t[0]);
                }

                lin.push(next_t[0]);
            }

            Some(lin)
        } else {
            None
        }
    }

    pub fn add_clause(&mut self, edges: &[(Edge, (usize, usize), (usize, usize), bool)]) {
        for edge in edges.iter() {
            let (variable, flip) = self.get_variable(edge.0, edge.1, edge.2);
            self.cnf.add_variable(variable, edge.3 ^ flip);
        }
        self.cnf.finish_clause();
    }

    pub fn add_clauses(&mut self, clauses: &[Vec<(Edge, (usize, usize), (usize, usize), bool)>]) {
        for clause in clauses.iter() {
            self.add_clause(clause);
        }
    }

    pub fn get_variable(
        &mut self,
        edge: Edge,
        u: (usize, usize),
        v: (usize, usize),
    ) -> (usize, bool) {
        assert!(u != v);
        let usable = self.edge_variable.len() + 1;
        match edge {
            Edge::CO if u > v => (
                *self.edge_variable.entry((edge, v, u)).or_insert(usable),
                true,
            ),
            _ => (
                *self.edge_variable.entry((edge, u, v)).or_insert(usable),
                false,
            ),
        }
    }
}
