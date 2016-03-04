#![feature(alloc)]
#![feature(heap_api)]
#![feature(oom)]

extern crate alloc;
extern crate spin;

mod table;
mod map;

pub use map::*;
pub use table::Accessor;
