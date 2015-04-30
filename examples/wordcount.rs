#![feature(str_words)]
#![feature(scoped)]
#![feature(collections)]

extern crate concurrent_hashmap;

use std::io::{Read};
use std::io;
use std::thread;
use std::default::Default;
use concurrent_hashmap::*;

fn main() {
    let words = read_words();
    let word_counts: ConcHashMap<&str, u32> = Default::default();
    count_words(&words, &word_counts);
    let mut counts: Vec<(&str, u32)> = word_counts.iter().map(|(&s, &n)| (s, n)).collect();
    counts.sort_by(|&(_, a), &(_, b)| a.cmp(&b));
    for &(word, count) in counts.iter() {
        println!("{}\t{}", word, count);
    }
}

fn read_words() -> Vec<String> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).unwrap();
    input.words().map(|w| w.trim_matches(|c| ['.', '"', ':', ';', ',', '!', '?', ')', '(', '_'].contains(&c)))
                 .map(|w| w.to_lowercase()).filter(|w| !w.is_empty()).collect()
}

fn count_words<'a>(words: &'a [String], word_counts: &ConcHashMap<&'a str, u32>) {
    let (left, right) = words.split_at(words.len() / 2);
    let tleft = thread::Builder::new().name("left".to_string()).scoped(move || {
        for word in left.iter() {
            word_counts.upsert(word, 1, &|count| *count += 1);
        }
    }).unwrap();
    let tright = thread::Builder::new().name("right".to_string()).scoped(move || {
        for word in right.iter() {
            word_counts.upsert(word, 1, &|count| *count += 1);
        }
    }).unwrap();
    tright.join();
    tleft.join();
}