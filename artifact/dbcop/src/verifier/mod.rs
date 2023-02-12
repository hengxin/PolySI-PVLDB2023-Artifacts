use hashbrown::{HashMap, HashSet};
// use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use consistency::sat::Sat;
use consistency::Consistency;
use db::history::Session;

use consistency::algo::{
    AtomicHistoryPO, PrefixConsistentHistory, SerializableHistory, SnapshotIsolationHistory,
};
use consistency::util::ConstrainedLinearization;

mod util;

use self::util::{BiConn, UGraph};

use slog::{Drain, Logger};

pub struct Verifier {
    log: slog::Logger,
    consistency_model: Consistency,
    use_sat: bool,
    use_bicomponent: bool,
    dir: PathBuf,
}

impl Verifier {
    pub fn new(dir: PathBuf) -> Self {
        // fs::create_dir(&dir).unwrap();
        let log_file = File::create(dir.join("result_log.json")).unwrap();

        Verifier {
            log: Self::get_logger(log_file),
            consistency_model: Consistency::Serializable,
            use_sat: false,
            use_bicomponent: false,
            dir,
        }
    }

    pub fn model(&mut self, model: &str) {
        self.consistency_model = match model {
            "rc" => Consistency::ReadCommitted,
            "rr" => Consistency::RepeatableRead,
            "ra" => Consistency::ReadAtomic,
            "cc" => Consistency::Causal,
            "pre" => Consistency::Prefix,
            "si" => Consistency::SnapshotIsolation,
            "ser" => Consistency::Serializable,
            "" => Consistency::Inc,
            &_ => unreachable!(),
        }
    }

    pub fn sat(&mut self, flag: bool) {
        self.use_sat = flag;
    }

    pub fn bicomponent(&mut self, flag: bool) {
        self.use_bicomponent = flag;
    }

    pub fn get_logger<W>(io: W) -> Logger
    where
        W: Write + Send + 'static,
    {
        // let plain = slog_term::PlainSyncDecorator::new(io);
        // let root_logger = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());
        let root_logger = Logger::root(
            std::sync::Mutex::new(slog_json::Json::default(io)).map(slog::Fuse),
            o!(),
        );

        info!(root_logger, "Application started";
        "started_at" => format!("{}", chrono::Local::now()));

        root_logger
    }

    pub fn gen_write_map(histories: &[Session]) -> HashMap<(usize, usize), (usize, usize, usize)> {
        let mut write_map = HashMap::new();

        for (i_node, session) in histories.iter().enumerate() {
            for (i_transaction, transaction) in session.iter().enumerate() {
                for (i_event, event) in transaction.events.iter().enumerate() {
                    if event.write {
                        if write_map
                            .insert(
                                (event.variable, event.value),
                                (i_node + 1, i_transaction, i_event),
                            )
                            .is_some()
                        {
                            panic!("each write should be unique");
                        }
                    } else {
                        write_map.entry((event.variable, 0)).or_insert((0, 0, 0));
                    }
                }
            }
        }

        write_map
    }

    pub fn verify(&mut self, histories: &[Session]) -> Option<Consistency> {
        let moment = std::time::Instant::now();
        let decision = self.transactional_history_verify(histories);
        let duration = moment.elapsed();

        info!(
            self.log,
            #"information",
            "the algorithm finished";
                "model" => format!("{:?}", self.consistency_model),
                "sat" => self.use_sat,
                "bicomponent" => self.use_bicomponent,
                "duration" => duration.as_secs() as f64 + f64::from(duration.subsec_nanos()) * 1e-9,
                "minViolation" => match decision {
                    Some(e) => format!("{:?}",e),
                    None => format!("ok")
                },
        );

        decision
    }

    pub fn transactional_history_verify(&mut self, histories: &[Session]) -> Option<Consistency> {
        let write_map = Self::gen_write_map(histories);

        for (i_node_r, session) in histories.iter().enumerate() {
            for (i_transaction_r, transaction) in session.iter().enumerate() {
                if transaction.success {
                    for (i_event_r, event) in transaction.events.iter().enumerate() {
                        if !event.write && event.success {
                            if let Some(&(i_node, i_transaction, i_event)) =
                                write_map.get(&(event.variable, event.value))
                            {
                                if event.value == 0 {
                                    assert_eq!(i_node, 0);
                                    assert_eq!(i_transaction, 0);
                                    assert_eq!(i_event, 0);
                                } else {
                                    let transaction2 = &histories[i_node - 1][i_transaction];
                                    // let event2 = &transaction2.events[i_event];
                                    // info!(self.log,"{:?}\n{:?}", event, event2);
                                    if !transaction2.success {
                                        info!(
                                            self.log,
                                            "{:?} read from {:?}",
                                            (i_node_r + 1, i_transaction_r, i_event_r),
                                            (i_node, i_transaction, i_event),
                                        );
                                        info!(self.log, "finished early"; "reason" => "DIRTY READ", "description" => "read from uncommitted/aborted transaction");
                                        return Some(Consistency::ReadCommitted);
                                    }
                                }
                            } else {
                                info!(self.log, "finished early"; "reason" => "NO WRITE WITH SAME (VARIABLE, VALUE)");
                                panic!("In consistent write");
                                // return false;
                            }
                        }
                    }
                }
            }
        }

        // add code for serialization check

        let mut transaction_last_writes = HashMap::new();

        for (i_node, session) in histories.iter().enumerate() {
            for (i_transaction, transaction) in session.iter().enumerate() {
                if transaction.success {
                    let mut last_writes = HashMap::new();
                    for (i_event, event) in transaction.events.iter().enumerate() {
                        if event.write && event.success {
                            // goes first to last, so when finished, it is last write event
                            last_writes.insert(event.variable, i_event);
                        }
                    }
                    transaction_last_writes.insert((i_node + 1, i_transaction), last_writes);
                }
            }
        }

        // checking for non-committed read, non-repeatable read
        for (i_node, session) in histories.iter().enumerate() {
            for (i_transaction, transaction) in session.iter().enumerate() {
                let mut writes = HashMap::new();
                let mut reads: HashMap<usize, (usize, usize, usize)> = HashMap::new();
                if transaction.success {
                    for (i_event, event) in transaction.events.iter().enumerate() {
                        if event.success {
                            if event.write {
                                writes.insert(event.variable, i_event);
                                reads.remove(&event.variable);
                            } else {
                                let &(wr_i_node, wr_i_transaction, wr_i_event) =
                                    write_map.get(&(event.variable, event.value)).unwrap();
                                if let Some(pos) = writes.get(&event.variable) {
                                    // checking if read the last write in same transaction
                                    if !((i_node + 1 == wr_i_node)
                                        && (i_transaction == wr_i_transaction)
                                        && (*pos == wr_i_event))
                                    {
                                        info!(
                                            self.log,
                                            "wr:{:?}, rd:{:?}",
                                            (wr_i_node, wr_i_transaction, wr_i_event),
                                            (i_node + 1, i_transaction, i_event)
                                        );
                                        info!(self.log, "finished early"; "reason" => "LOST UPDATE", "description" => "did not read the latest write within transaction");
                                        return Some(Consistency::ReadCommitted);
                                    }
                                } else {
                                    if event.value != 0 {
                                        // checking if read the last write from other transaction
                                        if *transaction_last_writes
                                            .get(&(wr_i_node, wr_i_transaction))
                                            .unwrap()
                                            .get(&event.variable)
                                            .unwrap()
                                            != wr_i_event
                                        {
                                            info!(self.log, "finished early"; "reason" => "UNCOMMITTED READ", "description" => "read some non-last write from other transaction");
                                            return Some(Consistency::ReadCommitted);
                                        }
                                    }

                                    if let Some((wr_i_node2, wr_i_transaction2, wr_i_event2)) =
                                        reads.get(&event.variable)
                                    {
                                        // checking if the read the same write as the last read in same transaction
                                        if !((*wr_i_node2 == wr_i_node)
                                            && (*wr_i_transaction2 == wr_i_transaction)
                                            && (*wr_i_event2 == wr_i_event))
                                        {
                                            info!(self.log, "finished early"; "reason" => "NON REPEATABLE READ", "description" => "did not read same as latest read which is after lastest write");
                                            return Some(Consistency::RepeatableRead);
                                        }
                                    }
                                }
                                reads.insert(
                                    event.variable,
                                    (wr_i_node, wr_i_transaction, wr_i_event),
                                );
                            }
                        }
                    }
                }
            }
        }

        info!(self.log, "each read from latest write");
        info!(self.log, "atomic reads");

        let mut transaction_infos = HashMap::new();

        let mut root_write_info = HashSet::new();

        for (i_node, session) in histories.iter().enumerate() {
            for (i_transaction, transaction) in session.iter().enumerate() {
                let mut read_info = HashMap::new();
                let mut write_info = HashSet::new();
                if transaction.success {
                    for event in transaction.events.iter() {
                        if event.success {
                            if event.write {
                                write_info.insert(event.variable);
                            } else {
                                let &(wr_i_node, wr_i_transaction, _) =
                                    write_map.get(&(event.variable, event.value)).unwrap();
                                if event.value == 0 {
                                    assert_eq!(wr_i_node, 0);
                                    assert_eq!(wr_i_transaction, 0);
                                    root_write_info.insert(event.variable);
                                }
                                if wr_i_node != i_node + 1 || wr_i_transaction != i_transaction {
                                    if let Some((old_i_node, old_i_transaction)) = read_info
                                        .insert(event.variable, (wr_i_node, wr_i_transaction))
                                    {
                                        // should be same, because repeatable read
                                        assert_eq!(old_i_node, wr_i_node);
                                        assert_eq!(old_i_transaction, wr_i_transaction);
                                    }
                                }
                            }
                        }
                    }
                }
                if !read_info.is_empty() || !write_info.is_empty() {
                    transaction_infos.insert((i_node + 1, i_transaction), (read_info, write_info));
                }
            }
        }

        if !root_write_info.is_empty() {
            assert!(transaction_infos
                .insert((0, 0), (Default::default(), root_write_info))
                .is_none());
        }

        info!(self.log, "atleast not read commmitted";
        "number of transactions" => format!("{}", transaction_infos.len())
        );

        if self.use_sat {
            info!(self.log, "using SAT");
        }

        if self.use_bicomponent {
            info!(self.log, "using bicomponent");
        }

        if self.use_bicomponent {
            // communication graph
            info!(self.log, "doing bicomponent decomposition");
            let mut access_map = HashMap::new();
            {
                let mut access_vars = HashSet::new();
                for (i_node, session) in histories.iter().enumerate() {
                    for transaction in session.iter() {
                        if transaction.success {
                            for event in transaction.events.iter() {
                                if event.success {
                                    access_vars.insert(event.variable);
                                }
                            }
                        }
                    }
                    for x in access_vars.drain() {
                        access_map
                            .entry(x)
                            .or_insert_with(Vec::new)
                            .push(i_node + 1);
                    }
                }
            }

            let mut ug: UGraph<usize> = Default::default();

            for (_, ss) in access_map.drain() {
                for &s1 in ss.iter() {
                    for &s2 in ss.iter() {
                        if s1 != s2 {
                            ug.add_edge(s1, s2);
                        }
                    }
                }
            }

            let biconn = BiConn::new(ug);

            let biconnected_components = biconn.get_biconnected_vertex_components();

            if biconnected_components.iter().all(|component| {
                info!(self.log, "doing for component {:?}", component);
                let restrict_infos = self.restrict(&transaction_infos, component);

                self.do_hard_verification(&restrict_infos).is_none()
            }) {
                None
            } else {
                Some(self.consistency_model)
            }
        } else {
            self.do_hard_verification(&transaction_infos)
        }
    }

    fn restrict(
        &self,
        transaction_infos: &HashMap<
            (usize, usize),
            (HashMap<usize, (usize, usize)>, HashSet<usize>),
        >,
        component: &HashSet<usize>,
    ) -> HashMap<(usize, usize), (HashMap<usize, (usize, usize)>, HashSet<usize>)> {
        let mut new_info = transaction_infos.clone();

        new_info.retain(|k, _| component.contains(&k.0));

        new_info
            .values_mut()
            .for_each(|(read_info, _)| read_info.retain(|_, k| component.contains(&k.0)));

        new_info
    }

    fn do_hard_verification(
        &mut self,
        transaction_infos: &HashMap<
            (usize, usize),
            (HashMap<usize, (usize, usize)>, HashSet<usize>),
        >,
    ) -> Option<Consistency> {
        if self.use_sat {
            let mut sat_solver = Sat::new(&transaction_infos);

            sat_solver.pre_vis_co();
            sat_solver.session();
            sat_solver.wr();
            sat_solver.read_atomic();

            match self.consistency_model {
                Consistency::Causal => {
                    sat_solver.vis_transitive();
                }
                Consistency::Prefix => {
                    sat_solver.prefix();
                }
                Consistency::SnapshotIsolation => {
                    sat_solver.prefix();
                    sat_solver.conflict();
                }
                Consistency::Serializable => {
                    sat_solver.ser();
                }
                _ => unreachable!(),
            }

            if sat_solver.solve(&self.dir).is_some() {
                None
            } else {
                Some(self.consistency_model)
            }
        } else {
            info!(self.log, "using our algorithms");

            match self.consistency_model {
                Consistency::ReadAtomic => {
                    let mut ra_hist = AtomicHistoryPO::new(transaction_infos.clone());

                    let wr = ra_hist.get_wr();
                    ra_hist.vis_includes(&wr);
                    // ra_hist.vis_is_trans();
                    let ww = ra_hist.causal_ww();
                    for (_, ww_x) in ww.iter() {
                        ra_hist.vis_includes(ww_x);
                    }
                    // ra_hist.vis_is_trans();

                    if ra_hist.vis.has_cycle() {
                        Some(self.consistency_model)
                    } else {
                        None
                    }
                }
                Consistency::Causal => {
                    let mut causal_hist = AtomicHistoryPO::new(transaction_infos.clone());

                    let wr = causal_hist.get_wr();
                    causal_hist.vis_includes(&wr);
                    causal_hist.vis_is_trans();
                    let ww = causal_hist.causal_ww();
                    for (_, ww_x) in ww.iter() {
                        causal_hist.vis_includes(ww_x);
                    }
                    causal_hist.vis_is_trans();

                    if causal_hist.vis.has_cycle() {
                        Some(self.consistency_model)
                    } else {
                        None
                    }
                }
                Consistency::Prefix => {
                    let mut pre_hist =
                        PrefixConsistentHistory::new(transaction_infos.clone(), self.log.clone());

                    let wr = pre_hist.history.get_wr();
                    pre_hist.history.vis_includes(&wr);
                    pre_hist.history.vis_is_trans();
                    let ww = pre_hist.history.causal_ww();
                    for (_, ww_x) in ww.iter() {
                        pre_hist.history.vis_includes(ww_x);
                    }
                    pre_hist.history.vis_is_trans();

                    if pre_hist.history.vis.has_cycle() {
                        Some(self.consistency_model)
                    } else {
                        if pre_hist.get_linearization().is_some() {
                            None
                        } else {
                            Some(self.consistency_model)
                        }
                    }
                }
                Consistency::SnapshotIsolation => {
                    let mut si_hist =
                        SnapshotIsolationHistory::new(transaction_infos.clone(), self.log.clone());

                    let wr = si_hist.history.get_wr();
                    si_hist.history.vis_includes(&wr);
                    si_hist.history.vis_is_trans();
                    let ww = si_hist.history.causal_ww();
                    for (_, ww_x) in ww.iter() {
                        si_hist.history.vis_includes(ww_x);
                    }
                    si_hist.history.vis_is_trans();

                    if si_hist.history.vis.has_cycle() {
                        Some(self.consistency_model)
                    } else {
                        if si_hist.get_linearization().is_some() {
                            None
                        } else {
                            Some(self.consistency_model)
                        }
                    }
                }
                Consistency::Serializable => {
                    let mut ser_hist =
                        SerializableHistory::new(transaction_infos.clone(), self.log.clone());

                    let wr = ser_hist.history.get_wr();
                    ser_hist.history.vis_includes(&wr);
                    ser_hist.history.vis_is_trans();
                    let ww = ser_hist.history.causal_ww();
                    for (_, ww_x) in ww.iter() {
                        ser_hist.history.vis_includes(ww_x);
                    }
                    ser_hist.history.vis_is_trans();

                    if ser_hist.history.vis.has_cycle() {
                        Some(self.consistency_model)
                    } else {
                        // let lin_o = ser_hist.get_linearization();
                        // {
                        //     // checking correctness
                        //     if let Some(ref lin) = lin_o {
                        //         let mut curr_value_map: HashMap<usize, (usize, usize)> =
                        //             Default::default();
                        //         for txn_id in lin.iter() {
                        //             let (read_info, write_info) =
                        //                 transaction_infos.get(txn_id).unwrap();
                        //             for (x, txn1) in read_info.iter() {
                        //                 match curr_value_map.get(&x) {
                        //                     Some(txn1_) => assert_eq!(txn1_, txn1),
                        //                     _ => unreachable!(),
                        //                 }
                        //             }
                        //             for &x in write_info.iter() {
                        //                 curr_value_map.insert(x, *txn_id);
                        //             }
                        //             // if !write_info.is_empty() {
                        //             //     println!("{:?}", txn_id);
                        //             //     println!("{:?}", curr_value_map);
                        //             // }
                        //         }
                        //     }
                        // }
                        // lin_o.is_some();

                        if ser_hist.get_linearization().is_some() {
                            None
                        } else {
                            Some(self.consistency_model)
                        }
                    }
                }
                Consistency::Inc => {
                    self.consistency_model = Consistency::ReadAtomic;
                    let decision = self.do_hard_verification(transaction_infos);
                    if decision.is_some() {
                        return decision;
                    }
                    self.consistency_model = Consistency::Causal;
                    let decision = self.do_hard_verification(transaction_infos);
                    if decision.is_some() {
                        return decision;
                    }
                    self.consistency_model = Consistency::Prefix;
                    let decision = self.do_hard_verification(transaction_infos);
                    if decision.is_some() {
                        return decision;
                    }
                    self.consistency_model = Consistency::SnapshotIsolation;
                    let decision = self.do_hard_verification(transaction_infos);
                    if decision.is_some() {
                        return decision;
                    }
                    self.consistency_model = Consistency::Serializable;
                    let decision = self.do_hard_verification(transaction_infos);
                    if decision.is_some() {
                        return decision;
                    }
                    self.consistency_model = Consistency::Inc;
                    None
                }
                _ => {
                    unreachable!();
                }
            }
        }
    }
}
