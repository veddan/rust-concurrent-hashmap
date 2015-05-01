#![feature(std_misc)]
#![feature(alloc)]
#![feature(collections)]
#![feature(core)]

extern crate alloc;

mod table;
mod map;

pub use map::*;
pub use table::Accessor;
