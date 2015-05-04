#![feature(std_misc)]
#![feature(alloc)]
#![feature(core)]

extern crate alloc;
extern crate num;

mod table;
mod map;

pub use map::*;
pub use table::Accessor;
