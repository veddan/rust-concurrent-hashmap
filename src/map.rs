use std::hash::{Hasher, Hash};
use std::collections::hash_state::HashState;
use std::collections::hash_map::RandomState;
use spin::{RwLock, RwLockReadGuard};
use std::default::Default;
use std::mem::swap;
use std::cmp::min;
use std::u16;
use std::borrow::Borrow;
use std::iter::{FromIterator, IntoIterator};
use table::*;

// This is the user-facing part of the implementation.
// ConcHashMap wraps a couple of actual hash tables (Table) with locks around them.
// It uses the top bits of the hash to decide which Table to access for a given key.
// The size of an invidual Table is limited (to a still unreasonably large value) so
// that it will never use the forementioned to bits of the hash.
// That means that resizing a Table will never cause a key to cross between Tables.
// Therefore each table can be resized independently.

/// A concurrent hashmap using sharding and reader-writer locking.
pub struct ConcHashMap<K, V, S=RandomState> where K: Send + Sync, V: Send + Sync {
    tables: Vec<RwLock<Table<K, V>>>,
    hash_state: S,
    table_shift: u64,
    table_mask: u64,
}

impl <K, V, S> ConcHashMap<K, V, S> where K: Hash + Eq + Send + Sync, V: Send + Sync, S: HashState {

    pub fn new() -> ConcHashMap<K, V> {
        Default::default()
    }

    pub fn with_options(opts: Options<S>) -> ConcHashMap<K, V, S> {
        let conc = opts.concurrency as usize;
        let partitions = conc.checked_next_power_of_two().unwrap_or((conc / 2).next_power_of_two());
        let capacity = f64_to_usize(opts.capacity as f64 / 0.92).expect("capacity overflow");
        let reserve = div_ceil(capacity, partitions);
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

    #[inline(never)]
    pub fn find<'a, Q: ?Sized>(&'a self, key: &Q) -> Option<Accessor<'a, K, V>>
            where K: Borrow<Q> + Hash + Eq + Send + Sync, Q: Hash + Eq + Sync {
        let hash = self.hash(key);
        let table_idx = self.table_for(hash);
        let table = self.tables[table_idx].read();
        match table.lookup(hash, |k| k.borrow() == key) {
            Some(idx) => Some(Accessor::new(table, idx)),
            None      => None
        }
    }

    #[inline(never)]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hash(&key);
        let table_idx = self.table_for(hash);
        let mut table = self.tables[table_idx].write();
        table.put(key, value, hash, |old, mut new| { swap(old, &mut new); new })
    }

    pub fn upsert<U: Fn(&mut V)>(&self, key: K, value: V, updater: &U) {
        let hash = self.hash(&key);
        let table_idx = self.table_for(hash);
        let mut table = self.tables[table_idx].write();
        table.put(key, value, hash, |old, _| { updater(old); });
    }

    pub fn remove<'a, Q: ?Sized>(&'a self, key: &Q) -> Option<V>
            where K: Borrow<Q> + Hash + Eq + Send + Sync, Q: Hash + Eq + Sync {
        let hash = self.hash(key);
        let table_idx = self.table_for(hash);
        let mut table = self.tables[table_idx].write();
        table.remove(hash, |k| k.borrow() == key)
    }

    fn table_for(&self, hash: u64) -> usize {
        ((hash >> self.table_shift) & self.table_mask) as usize
    }

    fn hash<Q: ?Sized>(&self, key: &Q) -> u64
            where K: Borrow<Q> + Hash + Eq + Send + Sync, Q: Hash + Eq + Sync {
        let mut hasher = self.hash_state.hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl <K, V, S> Clone for ConcHashMap<K, V, S>
        where K: Hash + Eq + Send + Sync + Clone, V: Send + Sync + Clone, S: HashState + Clone {
    fn clone(&self) -> ConcHashMap<K, V, S> {
        let clone = ConcHashMap::<K, V, S>::with_options(Options {
            capacity: 16,  // TODO
            hash_state: self.hash_state.clone(),
            concurrency: min(u16::MAX as usize, self.tables.len()) as u16
        });
        for (k, v) in self.iter() {
            clone.insert(k.clone(), v.clone());
        }
        return clone;
    }
}

impl <K, V, S> FromIterator<(K, V)> for ConcHashMap<K, V, S>
        where K: Eq + Hash + Send + Sync, V: Send + Sync, S: HashState + Default {
    fn from_iter<T>(iterator: T) -> Self where T: IntoIterator<Item=(K, V)> {
        let iterator = iterator.into_iter();
        let mut options: Options<S> = Default::default();
        if let (_, Some(bound)) = iterator.size_hint() {
            options.capacity = bound;
        }
        let map = ConcHashMap::with_options(options);
        for (k, v) in iterator {
            map.insert(k, v);
        }
        return map;
    }
}

impl <K, V, S> ConcHashMap<K, V, S> where K: Send + Sync, V: Send + Sync {
    pub fn iter<'a>(&'a self) -> Entries<'a, K, V, S> {
       Entries {
           map: self,
           table: self.tables[0].read(),
           table_idx: 0,
           bucket: 0
       }
    }

    pub fn clear(&self) {
        for table in self.tables.iter() {
            table.write().clear();
        }
    }
}

impl <K, V, S> Default for ConcHashMap<K, V, S>
        where K: Hash + Eq + Send + Sync, V: Send + Sync, S: HashState + Default {
    fn default() -> ConcHashMap<K, V, S> {
        ConcHashMap::with_options(Default::default())
    }
}

pub struct Entries<'a, K, V, S> where K: 'a + Send + Sync, V: 'a + Send + Sync, S: 'a {
    map: &'a ConcHashMap<K, V, S>,
    table: RwLockReadGuard<'a, Table<K, V>>,
    table_idx: usize,
    bucket: usize,
}

impl <'a, K, V, S> Entries<'a, K, V, S> where K: Send + Sync, V: Send + Sync  {
    fn next_table(&mut self) {
        self.table_idx += 1;
        self.table = self.map.tables[self.table_idx].read();
        self.bucket = 0;
    }
}

impl <'a, K, V, S> Iterator for Entries<'a, K, V, S> where K: Send + Sync, V: Send + Sync {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        loop {
            if self.bucket == self.table.capacity() {
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

impl <S> Default for Options<S> where S: HashState+Default {
    fn default() -> Options<S> {
        Options {
            capacity: 0,
            hash_state: Default::default(),
            concurrency: 16
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

fn f64_to_usize(f: f64) -> Option<usize> {
    if f.is_nan() || f.is_sign_negative() || f > ::std::usize::MAX as f64 {
        None
    } else {
        Some(f as usize)
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
        let old = map.insert(1, &"new");
        assert_eq!(Some("old"), old);
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

    #[test]
    fn test_clone() {
        let orig: ConcHashMap<i32, i32> = Default::default();
        for i in 0..100 {
            orig.insert(i, i * i);
        }
        let clone = orig.clone();
        for i in 0..100 {
            assert_eq!(orig.find(&i).unwrap().get(), clone.find(&i).unwrap().get());
        }
    }

    #[test]
    fn test_clear() {
        let map: ConcHashMap<i32, i32> = Default::default();
        for i in 0..100 {
            map.insert(i, i * i);
        }
        map.clear();
        for i in 0..100 {
            assert!(map.find(&i).is_none());
        }
    }

    #[test]
    fn test_remove() {
        let map: ConcHashMap<i32, String> = Default::default();
        map.insert(1, "one".to_string());
        map.insert(2, "two".to_string());
        map.insert(3, "three".to_string());
        assert_eq!(Some("two".to_string()), map.remove(&2));
        assert_eq!("one", map.find(&1).unwrap().get());
        assert!(map.find(&2).is_none());
        assert_eq!("three", map.find(&3).unwrap().get());
    }

    #[test]
    fn test_remove_many() {
        let map: ConcHashMap<i32, String> = Default::default();
        for i in 0..100 {
            map.insert(i, (i * i).to_string());
        }
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(Some((i * i).to_string()), map.remove(&i));
            }
        }
        for i in 0..100 {
            let x = map.find(&i);
            if i % 2 == 0 {
                assert!(x.is_none());
            } else {
                assert_eq!(&(i * i).to_string(), x.unwrap().get());
            }
        }
    }

    #[test]
    fn test_remove_insert() {
        let map: ConcHashMap<i32, String> = Default::default();
        for i in 0..100 {
            map.insert(i, (i * i).to_string());
        }
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(Some((i * i).to_string()), map.remove(&i));
            }
        }
        for i in 0..100 {
            if i % 4 == 0 {
                map.insert(i, i.to_string());
            }
        }
        for i in 0..100 {
            let x = map.find(&i);
            if i % 4 == 0 {
                assert_eq!(&i.to_string(), x.unwrap().get());
            } else if i % 2 == 0 {
                assert!(x.is_none());
            } else {
                assert_eq!(&(i * i).to_string(), x.unwrap().get());
            }
        }
    }

    #[test]
    fn test_from_iterator() {
        let vec: Vec<(u32, u32)> = (0..100).map(|i| (i, i * i)).collect();
        let map: ConcHashMap<u32, u32> = vec.iter().map(|x| *x).collect();
        for &(k, v) in vec.iter() {
            find_assert(&map, &k, &v);
        }
    }

    fn find_assert<K, V, S> (map: &ConcHashMap<K, V, S>, key: &K,  expected_val: &V)
            where K: Eq + Hash + Debug + Send + Sync, V: Eq + Debug + Send + Sync, S: HashState {
        match map.find(key) {
            None    => panic!("missing key {:?} should map to {:?}", key, expected_val),
            Some(v) => assert_eq!(*v.get(), *expected_val)
        }
    }
}
