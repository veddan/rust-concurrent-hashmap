use std::collections::BitVec;
use std::hash::{Hasher, Hash};
use std::collections::hash_state::HashState;
use std::sync::{RwLockReadGuard};
use std::ptr;
use std::mem;
use std::cmp::{max};
use std::mem::{align_of, size_of, drop};
use std::marker::{Send, Sync};
use alloc::heap::{allocate, deallocate};

const TOMBSTONE: u8 = 0xDE;

const MIN_LEN: usize = 16;

// We want to be able to use the top 16 bits of the hash for choosing the partition.
// If we limit the size of the partition to 47 bits, elements will never change partition.
// Thus we can resize each partition individually.
const MAX_LEN: u64 = (1 << 48) - 1;

#[inline(always)]
fn table_align<K, V>() -> usize { max(align_of::<Bucket<K, V>>(), 64) }

unsafe fn alloc_buckets<K, V>(count: usize) -> *mut Bucket<K, V> {
    let alloc_size = match count.checked_mul(size_of::<Bucket<K, V>>()) {
        None    => panic!("allocation size overflow"),
        Some(s) => s
    };
    let p = allocate(alloc_size, table_align::<K, V>());
    if p.is_null() {
        ::alloc::oom()
    } else {
        //println!("allocated table 0x{:x}, {}", p as usize, count);
        let buckets = p as *mut Bucket<K, V>;
        ptr::write_bytes(buckets, 0, count);
        buckets
    }
}

#[derive(Debug)]
struct Bucket<K, V> {
    key: K,
    value: V,
}

pub struct Table<K, V> {
    present: BitVec,
    buckets: *mut Bucket<K, V>,
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
            &(*self.table.buckets.offset(self.idx as isize)).value
        }
    }
}

impl <K: Hash+Eq+::std::fmt::Debug, V: ::std::fmt::Debug> Table<K, V> {
    pub fn new(reserve: usize) -> Table<K, V> {
        assert!(size_of::<Bucket<K, V>>() >= 1);
        Table {
            present: BitVec::from_elem(reserve, false),
            buckets: unsafe { alloc_buckets(reserve) }
        }
    }

    pub fn lookup(&self, key: &K, hash: u64) -> Option<usize> {
        let len = self.bucket_count();
        if len == 0 {
            return None;
        }
        let mask = len - 1;
        let hash = hash as usize;
        let mut i = hash & mask;
        let mut j = 0;
        loop {
            if self.is_present(i) && self.compare_key_at(key, i) {
                return Some(i);
            }
            if !self.is_present(i) && !self.is_deleted(i) {
                // The key we're searching for would have been placed here if it existed
                return None;
            }
            if i == len - 1 { break; }
            j += 1;
            i = (i + j) & mask;
        }
        return None;
    }

    pub fn put<H: HashState, U: Fn(&mut V, V)>(&mut self, key: K, value: V, hash: u64, hash_state: &H,
                                               update: U, is_resize: bool) {
        if !is_resize {
            //println!("thread {:?} inserting {:?} => {:?}", ::std::thread::current().name(), key, value);
            //self.dump_table();
        }
        if self.bucket_count() == 0 {
            self.resize(hash_state);
        }
        loop {
            let len = self.bucket_count();
            let mask = len - 1;
            let hash = hash as usize;
            let mut i = hash & mask;
            let mut j = 0;
            loop {
                if !self.is_present(i) {
                    let bucket = Bucket { key: key, value: value };
                    unsafe { self.put_at_empty(bucket, i); }
                    return;
                } else if self.compare_key_at(&key, i) {
                    let old_value = unsafe { &mut (*self.buckets.offset(i as isize)).value };
                    update(old_value, value);
                    return;
                }
                if i == len - 1 { break; }
                j += 1;
                i = (i + j) & mask;
            }
            self.resize(hash_state);
        }
    }

    fn compare_key_at(&self, key: &K, idx: usize) -> bool {
        assert!(self.present[idx]);
        unsafe { &(*self.buckets.offset(idx as isize)).key == key }
    }

    unsafe fn put_at_empty(&mut self, bucket: Bucket<K, V>, idx: usize) {
        //println!("inserting {:?}=>{:?} at {}", bucket.key, bucket.value, idx);
        let p = self.buckets.offset(idx as isize);
        ptr::write(p, bucket);
        self.present.set(idx, true);
    }

    fn resize<H: HashState>(&mut self, hash_state: &H) {
        let used = self.present.iter().filter(|&x| x).count() as f64;
        let load_factor = used / (self.present.len() as f64);
        //println!("resizing at load factor {}", load_factor);
        let len = self.bucket_count();
        let new_len = max(len.checked_add(len).expect("size overflow"), MIN_LEN);
        if new_len as u64 > MAX_LEN {
            panic!("requested size: {}, max size: {}", new_len, MAX_LEN);
        }
        //println!("resize {} => {}", len, new_len);
        let mut new_table = Table::new(new_len);
        unsafe {
            for (i, _) in self.present.iter().enumerate().filter(|&(_, x)| x) {
                let mut hasher = hash_state.hasher();
                let slot = self.buckets.offset(i as isize);
                (*slot).key.hash(&mut hasher);
                let hash = hasher.finish();
                new_table.put(ptr::read(&mut (*slot).key as *mut K),   // slot->key
                              ptr::read(&mut (*slot).value as *mut V), // slot->value
                              hash, hash_state, |_, _| { }, true);
            }
            let old_size = len * size_of::<Bucket<K, V>>();
            //println!("deallocating table during resize 0x{:x}, {}", self.buckets as usize, len);
            deallocate(self.buckets as *mut u8, old_size, table_align::<K, V>());
            self.buckets = ptr::null_mut();
        }
        mem::swap(self, &mut new_table);
    }

    fn dump_table(&self) {
        unsafe {
            let table = ::std::slice::from_raw_parts(self.buckets, self.bucket_count());
            for (i, e) in table.iter().enumerate() {
                if self.present[i] {
                    println!("{}:\t{:?}\t=>\t{:?}",
                            i, e.key, e.value,);
                } else {
                    println!("{}:\tempty", i);
                }
            }
        }
    }
}

impl <K, V> Table<K, V> {
    pub fn bucket_count(&self) -> usize {
        self.present.len()
    }

    /// Used to implement iteration.
    /// Search for a present bucket >= idx.
    /// If one is found, Some(..) is returned and idx is set to a value
    /// that can be passed back to iter_advance to look for the next bucket.
    /// When all bucket have been scanned, idx is set to bucket_count().
    pub fn iter_advance<'a>(&'a self, idx: &mut usize) -> Option<(&'a K, &'a V)> {
        if *idx >= self.bucket_count() {
            return None;
        }
        for i in *idx..self.bucket_count() {
            if self.is_present(i) {
                *idx = i + 1;
                let entry = unsafe {
                    let bucket = self.buckets.offset(i as isize);
                    (&(*bucket).key, &(*bucket).value)
                };
                return Some(entry);
            }
        }
        *idx = self.bucket_count();
        return None;
    }

    fn is_present(&self, idx: usize) -> bool {
        self.present[idx]
    }

    fn is_deleted(&self, idx: usize) -> bool {
        !self.is_present(idx) && unsafe {
            ptr::read(self.buckets.offset(idx as isize) as *const u8) != TOMBSTONE
        }
    }
}

impl <K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        if self.buckets.is_null() {
            return;
        }
        // Can't call the bucket_count() method here due to lack of restrictions on K and V
        //println!("dropping table 0x{:x}, {}", self.buckets as usize, self.bucket_count());
        unsafe {
            for (i, _) in self.present.iter().enumerate().filter(|&(_, x)| x) {
                drop::<Bucket<K, V>>(ptr::read(self.buckets.offset(i as isize)));
            }
            let size = self.bucket_count() * size_of::<Bucket<K, V>>();
            deallocate(self.buckets as *mut u8, size, table_align::<K, V>());
        }
    }
}

unsafe impl <K, V> Sync for Table<K, V> { }

unsafe impl <K, V> Send for Table<K, V> { }
