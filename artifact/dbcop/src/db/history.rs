use std::fmt;

use hashbrown::HashMap;

use rand::distributions::{Distribution, Uniform};
use rand::Rng;

use rayon::iter::{IntoParallelIterator, ParallelIterator};

use chrono::{DateTime, Duration, Local};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Event {
    pub write: bool,
    pub variable: usize,
    pub value: usize,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Transaction {
    pub events: Vec<Event>,
    pub success: bool,
}

pub type Session = Vec<Transaction>;

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let repr = format!(
            "<{}({}):{:2}>",
            if self.write { 'W' } else { 'R' },
            self.variable,
            self.value
        );
        if !self.success {
            write!(f, "!")?;
        }
        write!(f, "{}", repr)
    }
}

impl Event {
    pub fn read(var: usize) -> Self {
        Event {
            write: false,
            variable: var,
            value: 0,
            success: false,
        }
    }
    pub fn write(var: usize, val: usize) -> Self {
        Event {
            write: true,
            variable: var,
            value: val,
            success: false,
        }
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let repr = format!("{:?}", self.events);
        if !self.success {
            write!(f, "!")?;
        }
        write!(f, "{}", repr)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HistParams {
    id: usize,
    n_node: usize,
    n_variable: usize,
    n_transaction: usize,
    n_event: usize,
}

impl HistParams {
    pub fn get_id(&self) -> usize {
        self.id
    }
    pub fn set_id(&mut self, id: usize) {
        self.id = id;
    }
    pub fn get_n_node(&self) -> usize {
        self.n_node
    }
    pub fn get_n_variable(&self) -> usize {
        self.n_variable
    }
    pub fn get_n_transaction(&self) -> usize {
        self.n_transaction
    }
    pub fn get_event(&self) -> usize {
        self.n_event
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct History {
    params: HistParams,
    info: String,
    start: DateTime<Local>,
    end: DateTime<Local>,
    data: Vec<Session>,
}

impl History {
    pub fn new(
        params: HistParams,
        info: String,
        start: DateTime<Local>,
        end: DateTime<Local>,
        data: Vec<Session>,
    ) -> Self {
        History {
            params,
            info,
            start,
            end,
            data,
        }
    }

    pub fn get_id(&self) -> usize {
        self.params.get_id()
    }

    pub fn get_data(&self) -> &Vec<Session> {
        &self.data
    }

    pub fn get_cloned_data(&self) -> Vec<Session> {
        self.data.clone()
    }

    pub fn get_params(&self) -> &HistParams {
        &self.params
    }

    pub fn get_cloned_params(&self) -> HistParams {
        self.params.clone()
    }

    pub fn get_duration(&self) -> Duration {
        self.end - self.start
    }
}

pub fn generate_single_history(
    n_node: usize,
    n_variable: usize,
    n_transaction: usize,
    n_event: usize,
) -> Vec<Session> {
    let mut counters = HashMap::new();
    let mut random_generator = rand::thread_rng();
    let read_variable_range = Uniform::from(0..n_variable);
    let jump = (n_variable as f64 / n_node as f64).ceil() as usize;
    (0..n_node)
        .map(|i_node| {
            let i = i_node * jump;
            let j = std::cmp::min((i_node + 1) * jump, n_variable);
            // let write_variable_range = Uniform::from(i..j);
            (0..n_transaction)
                .map(|_| Transaction {
                    events: (0..n_event)
                        .map(|_| {
                            if random_generator.gen() {
                                let variable = read_variable_range.sample(&mut random_generator);
                                Event::read(variable)
                            } else {
                                let variable = read_variable_range.sample(&mut random_generator);
                                // let variable = write_variable_range.sample(&mut random_generator);
                                let value = {
                                    let entry = counters.entry(variable).or_insert(0);
                                    *entry += 1;
                                    *entry
                                };
                                Event::write(variable, value)
                            }
                        })
                        .collect(),
                    success: false,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

pub fn generate_mult_histories(
    n_hist: usize,
    n_node: usize,
    n_variable: usize,
    n_transaction: usize,
    n_event: usize,
) -> Vec<History> {
    (0..n_hist)
        .into_par_iter()
        .map(|i_hist| {
            let start_time = Local::now();
            let hist = generate_single_history(n_node, n_variable, n_transaction, n_event);
            let end_time = Local::now();
            History {
                params: HistParams {
                    id: i_hist,
                    n_node,
                    n_variable,
                    n_transaction,
                    n_event,
                },
                info: "generated".to_string(),
                start: start_time,
                end: end_time,
                data: hist,
            }
        })
        .collect()
}
