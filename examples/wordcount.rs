#![feature(str_words)]
#![feature(scoped)]
#![feature(collections)]

extern crate concurrent_hashmap;

use std::cmp::max;
use std::io::{Read};
use std::io;
use std::thread;
use std::default::Default;
use concurrent_hashmap::*;

fn main() {
    let words = read_words();
    let word_counts: ConcHashMap<&str, u32> = Default::default();
    count_words(&words, &word_counts, 4);
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

fn count_words<'a>(words: &'a [String], word_counts: &ConcHashMap<&'a str, u32>, nthreads: usize) {
    let mut threads = Vec::with_capacity(nthreads);
    for chunk in words.chunks(max(10, words.len() / nthreads)) {
        threads.push(thread::scoped(move || {
            for word in chunk.iter() {
                word_counts.upsert(word, 1, &|count| *count += 1);
            }
        }));
    }
}