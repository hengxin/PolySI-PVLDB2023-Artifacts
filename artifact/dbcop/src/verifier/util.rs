use hashbrown::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Default, Debug)]
pub struct UGraph<T>
where
    T: Hash + Eq + Copy + Debug,
{
    adj_map: HashMap<T, HashSet<T>>,
}

impl<T> UGraph<T>
where
    T: Hash + Eq + Copy + Debug,
{
    pub fn add_edge(&mut self, u: T, v: T) {
        self.adj_map.entry(u).or_insert_with(HashSet::new).insert(v);
        self.adj_map.entry(v).or_insert_with(HashSet::new).insert(u);
    }
}

#[derive(Default, Debug)]
pub struct BiConn<T>
where
    T: Hash + Eq + Copy + Debug,
{
    g: UGraph<T>,
}

impl<T> BiConn<T>
where
    T: Hash + Eq + Copy + Debug,
{
    pub fn new(g: UGraph<T>) -> Self {
        BiConn { g }
    }

    fn bcc_util(
        &self,
        u: T,
        depth: u64,
        disc: &mut HashMap<T, u64>,
        low: &mut HashMap<T, u64>,
        parent: &mut HashMap<T, T>,
        st: &mut Vec<(T, T)>,
        components: &mut Vec<Vec<(T, T)>>,
    ) {
        let mut children = 0;

        *disc.get_mut(&u).unwrap() = depth + 1;
        *low.get_mut(&u).unwrap() = depth + 1;

        if let Some(vs) = self.g.adj_map.get(&u) {
            for &v in vs.iter() {
                if disc[&v] == 0 {
                    *parent.get_mut(&v).unwrap() = u;
                    children += 1;
                    st.push((u, v));
                    self.bcc_util(v, depth + 1, disc, low, parent, st, components);

                    *low.get_mut(&u).unwrap() = std::cmp::min(low[&u], low[&v]);

                    if (parent[&u] == u && children > 1) || (parent[&u] != u && low[&v] >= disc[&u])
                    {
                        let mut component = Vec::new();
                        while let Some(p) = st.pop() {
                            component.push(p);
                            if p == (u, v) {
                                break;
                            }
                        }
                        components.push(component);
                    }
                } else if v != parent[&u] && low[&u] > disc[&v] {
                    *low.get_mut(&u).unwrap() = std::cmp::min(low[&u], disc[&v]);
                    st.push((u, v));
                }
            }
        }
    }

    pub fn get_biconnected_edge_components(&self) -> (Vec<T>, Vec<Vec<(T, T)>>) {
        let mut disc = HashMap::new();
        let mut low = HashMap::new();
        let mut parent: HashMap<_, T> = HashMap::new();

        let mut st = Vec::new();

        let mut components: Vec<Vec<_>> = Vec::new();

        let mut us: Vec<_> = self.g.adj_map.keys().collect();

        disc.extend(self.g.adj_map.keys().map(|&k| (k, 0)));
        low.extend(self.g.adj_map.keys().map(|&k| (k, 0)));
        parent.extend(self.g.adj_map.keys().map(|&k| (k, *us[0])));

        let mut alone = Vec::new();

        for &u in us.drain(..) {
            match self.g.adj_map.get(&u) {
                Some(vs) if vs.is_empty() => {
                    alone.push(u);
                }
                None => {
                    alone.push(u);
                }
                _ => {
                    if disc[&u] == 0 {
                        self.bcc_util(
                            u,
                            0,
                            &mut disc,
                            &mut low,
                            &mut parent,
                            &mut st,
                            &mut components,
                        );

                        if !st.is_empty() {
                            components.push(st.drain(..).collect());
                        }
                    }
                }
            }
        }
        (alone, components)
    }

    pub fn get_biconnected_vertex_components(&self) -> Vec<HashSet<T>> {
        let (alone, edge_components) = self.get_biconnected_edge_components();
        let mut components: Vec<HashSet<T>> = Vec::new();
        for u in alone {
            let mut component = HashSet::new();
            component.insert(u);
            components.push(component);
        }
        for uvs in edge_components {
            let mut component = HashSet::new();
            component.extend(uvs.iter().flat_map(|&(u, v)| vec![u, v]));
            components.push(component);
        }
        components
    }
}
