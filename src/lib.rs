#![feature(alloc)]
#![feature(hashmap_hasher)]
#![feature(heap_api)]
#![feature(oom)]

extern crate alloc;
extern crate num;
extern crate spin;

mod table;
mod map;

pub use map::*;
pub use table::Accessor;
