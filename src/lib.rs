#![feature(std_misc)]
#![feature(alloc)]

extern crate alloc;
extern crate num;

mod table;
mod map;

pub use map::*;
pub use table::Accessor;
