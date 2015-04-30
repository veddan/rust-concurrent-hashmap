use std::hash::{Hasher, Hash};
use std::collections::hash_state::HashState;
use std::collections::hash_map::RandomState;
use std::sync::{RwLock, RwLockReadGuard};
use std::default::Default;
use std::num::ToPrimitive;
use std::mem::replace;
use table::*;

// This is the user-facing part of the implementation.
// ConcHashMap wraps a couple of actual hash tables (Table) with locks around them.
// It uses the top bits of the hash to decide which Table to access for a given key.
// The size of an invidual Table is limited (to a still unreasonably large value) so
// that it will never use the forementioned to bits of the hash.
// That means that resizing a Table will never cause a key to cross between Tables.
// Therefore each table can be resized independently.

pub struct ConcHashMap<K, V, S=RandomState> {
    tables: Vec<RwLock<Table<K, V>>>,
    hash_state: S,
    table_shift: u64,
    table_mask: u64,
}

impl <K: Hash + Eq + ::std::fmt::Debug, V: ::std::fmt::Debug, S: HashState> ConcHashMap<K, V, S> {

    pub fn with_options(opts: Options<S>) -> ConcHashMap<K, V, S> {
        let capacity = (opts.capacity as f64 / 0.92).to_usize().expect("capacity overflow");
        let conc = opts.concurrency as usize;
        let partitions = conc.checked_next_power_of_two().unwrap_or((conc / 2).next_power_of_two());
        //println!("partitions: {}", partitions);
        let reserve = div_ceil(capacity, partitions).next_power_of_two();
        let mut tables = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            tables.push(RwLock::new(Table::new(reserve)));
        }
        ConcHashMap {
            tables: tables,
            hash_state: opts.hash_state,
            table_shift: if partitions == 1 { 0 } else { 64 - partitions.trailing_zeros() as u64 },
            table_mask: partitions as u64 - 1
        }
    }

    pub fn find<'a>(&'a self, key: &K) -> Option<Accessor<'a, K, V>> {
        let hash = self.hash(key);
        let table_idx = self.table_for(hash);
        let table = self.tables[table_idx].read().unwrap();
        match table.lookup(key, hash) {
            Some(idx) => Some(Accessor::new(table, idx)),
            None      => None
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let hash = self.hash(&key);
        let table_idx = self.table_for(hash);
        let mut table = self.tables[table_idx].write().unwrap();
        table.put(key, value, hash, &self.hash_state, |old, new| { replace(old, new); }, false);
    }

    pub fn upsert<U: Fn(&mut V)>(&self, key: K, value: V, updater: &U) {
        let hash = self.hash(&key);
        let table_idx = self.table_for(hash);
        let mut table = self.tables[table_idx].write().unwrap();
        table.put(key, value, hash, &self.hash_state, |old, _| { updater(old); }, false);
    }

    pub fn remove(&self, _key: &K) {
        panic!()
    }

    fn table_for(&self, hash: u64) -> usize {
        ((hash >> self.table_shift) & self.table_mask) as usize
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.hash_state.hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl <K, V> ConcHashMap<K, V> {
    pub fn iter<'a>(&'a self) -> Entries<'a, K, V> {
       Entries {
           map: self,
           table: self.tables[0].read().unwrap(),
           table_idx: 0,
           bucket: 0
       }
    }
}

impl <K: Hash + Eq+ ::std::fmt::Debug, V: ::std::fmt::Debug, S: HashState+Default> Default for ConcHashMap<K, V, S> {
    fn default() -> ConcHashMap<K, V, S> {
        ConcHashMap::with_options(Default::default())
    }
}

pub struct Entries<'a, K: 'a, V: 'a> {
    map: &'a ConcHashMap<K, V>,
    table: RwLockReadGuard<'a, Table<K, V>>,
    table_idx: usize,
    bucket: usize,
}

impl <'a, K, V> Entries<'a, K, V> {
    fn next_table(&mut self) {
        self.table_idx += 1;
        self.table = self.map.tables[self.table_idx].read().unwrap();
        self.bucket = 0;
    }
}

impl <'a, K, V> Iterator for Entries<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        loop {
            if self.bucket == self.table.bucket_count() {
                if self.table_idx + 1 == self.map.tables.len() {
                    return None;
                }
                self.next_table();
            }
            let res: Option<(&'a K, &'a V)> = unsafe { ::std::mem::transmute(self.table.iter_advance(&mut self.bucket)) };
            match res {
                Some(e) => return Some(e),
                None    => {
                    if self.table_idx + 1 == self.map.tables.len() {
                        return None;
                    }
                    self.next_table()
                }
            }
        }
    }
}

pub struct Options<S> {
    pub capacity: usize,
    pub hash_state: S,
    pub concurrency: u16,
}

impl <S: HashState+Default> Default for Options<S> {
    fn default() -> Options<S> {
        Options {
            capacity: 16,
            hash_state: Default::default(),
            concurrency: 4
        }
    }
}

fn div_ceil(n: usize, d: usize) -> usize {
    if n == 0 {
        0
    } else {
        n/d + if n % d == 0 { 1 } else { 0 }
    }
}

#[cfg(test)]
mod test {
    use std::hash::Hash;
    use std::collections::hash_state::{HashState, DefaultState};
    use std::hash::Hasher;
    use std::default::Default;
    use std::fmt::Debug;
    use super::*;

    struct BadHasher;

    impl Hasher for BadHasher {
        fn write(&mut self, _: &[u8]) { }

        fn finish(&self) -> u64 { 0 }
    }

    impl Default for BadHasher {
        fn default() -> BadHasher { BadHasher }
    }

    struct OneAtATimeHasher {
        state: u64
    }

    impl Hasher for OneAtATimeHasher {
        fn write(&mut self, bytes: &[u8]) {
            for &b in bytes.iter() {
                self.state = self.state.wrapping_add(b as u64);
                self.state = self.state.wrapping_add(self.state << 10);
                self.state ^= self.state >> 6;
            }
        }

        fn finish(&self) -> u64 { 
            let mut hash = self.state;
            hash = hash.wrapping_add(hash << 3);
            hash ^= hash >> 11;
            hash = hash.wrapping_add(hash << 15);
            hash
        }
    }

    impl Default for OneAtATimeHasher {
        fn default() -> OneAtATimeHasher {
            OneAtATimeHasher { state: 0x124C494467744825 }
        }
    }

    #[test]
    fn insert_is_found() {
        let map: ConcHashMap<i32, i32> = Default::default();
        assert!(map.find(&1).is_none());
        map.insert(1, 2);
        assert_eq!(map.find(&1).unwrap().get(), &2);
        assert!(map.find(&2).is_none());
        map.insert(2, 4);
        assert_eq!(map.find(&2).unwrap().get(), &4);
    }

    #[test]
    fn insert_replace() {
        let map: ConcHashMap<i32, &'static str> = Default::default();
        assert!(map.find(&1).is_none());
        map.insert(1, &"old");
        assert_eq!(map.find(&1).unwrap().get(), &"old");
        map.insert(1, &"new");
        assert_eq!(map.find(&1).unwrap().get(), &"new");
    }

    #[test]
    fn insert_lots() {
        let map: ConcHashMap<i32, i32, DefaultState<OneAtATimeHasher>> = Default::default();
        for i in 0..1000 {
            if i % 2 == 0 {
                map.insert(i, i * 2);
            }
        }
        for i in 0..1000 {
            if i % 2 == 0 {
                find_assert(&map, &i, &(i * 2));
            } else {
                assert!(map.find(&i).is_none());
            }
        }
    }

    #[test]
    fn insert_bad_hash_lots() {
        let map: ConcHashMap<i32, i32, DefaultState<BadHasher>> = Default::default();
        for i in 0..100 {
            if i % 2 == 0 {
                map.insert(i, i * 2);
            }
        }
        for i in 0..100 {
            if i % 2 == 0 {
                find_assert(&map, &i, &(i * 2));
            } else {
                assert!(map.find(&i).is_none());
            }
        }
    }

    #[test]
    fn find_none_on_empty() {
        let map: ConcHashMap<i32, i32> = Default::default();
        assert!(map.find(&1).is_none());
    }

    fn find_assert<K: Eq+Hash+Debug, V: Eq+Debug, S: HashState>(map: &ConcHashMap<K, V, S>, key: &K,
                                                                expected_val: &V) {
        match map.find(key) {
            None    => panic!("missing key {:?} should map to {:?}", key, expected_val),
            Some(v) => assert_eq!(*v.get(), *expected_val)
        }
    }
}
