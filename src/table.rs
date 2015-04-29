use std::collections::BitVec;
use std::hash::{Hasher, Hash};
use std::collections::hash_state::HashState;
use std::sync::{RwLockReadGuard};
use std::ptr;
use std::mem;
use std::cmp::{min, max};
use std::mem::{align_of, size_of, drop};
use alloc::heap::{allocate, deallocate};

use std::io::Write;

const MIN_LEN: usize = 16;

// We want to be able to use the top 16 bits of the hash for choosing the partition.
// If we limit the size of the partition to 47 bits, elements will never change partition.
// Thus we can resize each partition individually.
const MAX_LEN: u64 = (1 << 48) - 1;

#[inline(always)]
fn neigh_len<K, V>() -> usize {
    max(8, 64 / size_of::<Bucket<K, V>>())
}

#[inline(always)]
fn max_probe_distance<K, V>() -> usize {
    min(::std::u8::MAX as usize, 4 * neigh_len::<K, V>())
}

#[inline(always)]
fn table_align<K, V>() -> usize { max(align_of::<Bucket<K, V>>(), 64) }

unsafe fn alloc_elems<K, V>(count: usize) -> *mut Bucket<K, V> {
    let alloc_size = match count.checked_mul(size_of::<Bucket<K, V>>()) {
        None    => panic!("allocation size overflow"),
        Some(s) => s
    };
    let p = allocate(alloc_size, table_align::<K, V>());
    if p.is_null() {
        ::alloc::oom()
    } else {
        println!("allocated table 0x{:x}, {}", p as usize, count);
        p as *mut Bucket<K, V>
    }
}

#[derive(Debug)]
struct Bucket<K, V> {
    key: K,
    value: V,
    displacement: u8
}

pub struct Table<K, V> {
    present: BitVec,
    entries: *mut Bucket<K, V>,
}

pub struct Accessor<'a, K: 'a, V: 'a> {
    table: RwLockReadGuard<'a, Table<K, V>>,
    idx: usize
}

impl <'a, K, V> Accessor<'a, K, V> {
    pub fn new(table: RwLockReadGuard<'a, Table<K, V>>, idx: usize) -> Accessor<'a, K, V> {
        Accessor {
            table: table,
            idx: idx
        }
    }

    pub fn get(&self) -> &'a V {
        debug_assert!(self.table.present[self.idx]);
        unsafe {
            &(*self.table.entries.offset(self.idx as isize)).value
        }
    }
}

impl <K: Hash+Eq+::std::fmt::Debug, V: ::std::fmt::Debug> Table<K, V> {
    pub fn new(reserve: usize) -> Table<K, V> {
        Table {
            present: BitVec::from_elem(reserve, false),
            entries: unsafe { alloc_elems(reserve) }
        }
    }

    pub fn lookup(&self, key: &K, hash: u64) -> Option<usize> {
        let (neigh_start, neigh_end) = self.get_neighborhood(hash);
        for i in neigh_start..neigh_end {
            if !self.present[i] { continue; }
            println!("looking for {:?} at {}: {:?}", key, i,
                     unsafe { &(*self.entries.offset(i as isize)).key });
            if self.compare_key_at(key, i) {
                return Some(i);
            }
        }
        return None;
    }

    pub fn put<H: HashState, U: Fn(&mut V, V)>(&mut self, key: K, value: V, hash: u64, hash_state: &H,
                                               update: U, is_resize: bool) {
        if !is_resize {
            println!("\ninserting {:?} => {:?}", key, value);
            self.dump_table();
        }
        let neighborhood = self.get_neighborhood(hash);
        let mut neigh_start = neighborhood.0;
        let mut neigh_end = neighborhood.1;
        let mut free;
        if !is_resize { println!("wanted bucket: {}", neigh_start); }
        loop {
            match self.find_free_slot(neigh_start) {
                Some(i) => { free = i; break; },
                None    => {
                    self.resize(hash_state);
                    let neighborhood = self.get_neighborhood(hash);
                    neigh_start = neighborhood.0;
                    neigh_end = neighborhood.1;
                }
            }
        }
       if !is_resize { println!("free: {}, bucket: ({}, {})", free, neigh_start, neigh_end); }
        if free < neigh_end {
            let displacement = (free - neigh_start) as u8;
            unsafe {
                self.put_at_empty(Bucket { key: key, value: value, displacement: displacement }, free);
            }
            return;
        }
        let mut l = free;
        'next_neighborhood: loop {
            l = max(neigh_start, l.saturating_sub(neigh_len::<K, V>() - 1));
            for i in l..free {
                unsafe {
                    if !is_resize { println!("considering swapping {}", i); }
                    let home = i - (*self.entries.offset(i as isize)).displacement as usize;
                    println!("free: {}, home: {}", free, home);
                    if free - home >= neigh_len::<K, V>() {
                        // If we swapped i with free, i would be too far from home
                        continue;
                    }
                    self.swap(i, free);
                    free = i;
                    if free - neigh_start < neigh_len::<K, V>() {
                        let displacement = (free - home) as u8;
                        self.put_at_empty(Bucket { key: key, value: value, displacement: displacement }, free);
                        return;
                    }
                    continue 'next_neighborhood;
                }
            }
            if l == neigh_start {
                // Nothing to swap with between the free bucket and the "home" bucket
                break;
            }
        }
        self.resize(hash_state);
        self.put(key, value, hash, hash_state,update, is_resize);
    }

    fn compare_key_at(&self, key: &K, idx: usize) -> bool {
        assert!(self.present[idx]);
        unsafe { &(*self.entries.offset(idx as isize)).key == key }
    }

    fn get_neighborhood(&self, hash: u64) -> (usize, usize) {
        let len = self.present.len();
        let start = (hash as usize) & (len - 1);  // TODO Are we sure len > 0?
        let end = min(len, start + neigh_len::<K, V>());
        (start, end)
    }

    fn find_free_slot(&self, start: usize) -> Option<usize> {
        for (i, _) in self.present.iter().enumerate().skip(start).take(max_probe_distance::<K, V>())
                                  .filter(|&(_, x)| !x) {
            return Some(i);
        }
        return None;
    }

    unsafe fn swap(&mut self, a: usize, b: usize) {
        debug_assert!(a != b);
        mem::swap(&mut *self.entries.offset(a as isize), &mut *self.entries.offset(b as isize));
        let xa = self.present[a];
        let xb = self.present[b];
        self.present.set(a, xb);
        self.present.set(b, xa);
    }

    unsafe fn put_at_empty(&mut self, bucket: Bucket<K, V>, idx: usize) {
        println!("inserting {:?}=>{:?} at {}", bucket.key, bucket.value, idx);
        let p = self.entries.offset(idx as isize);
        ptr::write(p, bucket);
        self.present.set(idx, true);
    }

    fn resize<H: HashState>(&mut self, hash_state: &H) {
        let used = self.present.iter().filter(|&x| x).count() as f64;
        let load_factor = used / (self.present.len() as f64);
        println!("resizing at load factor {}", load_factor);
        let len = self.present.len();
        let new_len = max(len.checked_add(len).expect("size overflow"), MIN_LEN);
        if new_len as u64 > MAX_LEN {
            panic!("requested size: {}, max size: {}", new_len, MAX_LEN);
        }
        println!("resize {} => {}", len, new_len);
        let mut new_table = Table::new(new_len);
        unsafe {
            for (i, _) in self.present.iter().enumerate().filter(|&(_, x)| x) {
                let mut hasher = hash_state.hasher();
                let slot = self.entries.offset(i as isize);
                (*slot).key.hash(&mut hasher);
                let hash = hasher.finish();
                new_table.put(ptr::read(&mut (*slot).key as *mut K),   // slot->key
                              ptr::read(&mut (*slot).value as *mut V), // slot->value
                              hash, hash_state, |_, _| { }, true);
            }
            let old_size = len * size_of::<Bucket<K, V>>();
            println!("deallocating table during resize 0x{:x}, {}", self.entries as usize, len);
            deallocate(self.entries as *mut u8, old_size, table_align::<K, V>());
            self.entries = ptr::null_mut();
        }
        mem::swap(self, &mut new_table);
    }

    fn dump_table(&self) {
        unsafe {
            let table = ::std::slice::from_raw_parts(self.entries, self.present.len());
            for (i, e) in table.iter().enumerate() {
                if self.present[i] {
                    println!("{}:\t{:?}\t=>\t{:?}\tdisplacement = {}\thome = {}",
                            i, e.key, e.value, e.displacement, i - e.displacement as usize);
                } else {
                    println!("{}:\tempty", i);
                }
            }
        }
    }
}

impl <K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        if self.entries.is_null() {
            return;
        }
        println!("dropping table 0x{:x}, {}", self.entries as usize, self.present.len());
        unsafe {
            for (i, _) in self.present.iter().enumerate().filter(|&(_, x)| x) {
                drop::<Bucket<K, V>>(ptr::read(self.entries.offset(i as isize)));
            }
            deallocate(self.entries as *mut u8, self.present.len() * size_of::<Bucket<K, V>>(), table_align::<K, V>());
        }
    }
}