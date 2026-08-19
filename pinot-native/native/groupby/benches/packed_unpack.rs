// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Regression micro-benchmark for the production `PackedKeyEncoder::unpack`
//! (the shipped layout: a `{shift, width}` array-of-structs decoded with a
//! branch-free on-demand mask — see `multi_key.rs`). Times `unpack` across GROUP
//! BY keys of 2..16 columns so a regression in the packed-key decode surfaces
//! quickly.
//!
//! `pack` is not benchmarked: it only ever touches `shift`, so it is trivial and
//! off the contested decode path. An earlier version of this bench crossed
//! several candidate layouts (SoA/AoS × branch/branchless/stored-mask) to justify
//! the current design during the PR #248 review; now that the layout is settled,
//! only the shipped encoder is measured.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pinot_native_groupby::PackedKeyEncoder;
use std::time::Duration;

// ---- deterministic key generation (no rand dependency) --------------------
struct XorShift(u64);
impl XorShift {
    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Build `n` random valid packed keys for the given per-column cardinalities,
/// using the production encoder's own `pack` so the keys are exactly what
/// `unpack` decodes in production.
fn gen_keys(cards: &[i32], n: usize) -> Vec<i64> {
    let enc = PackedKeyEncoder::new(cards).expect("cards fit in 64 bits");
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    let mut ids = vec![0i32; cards.len()];
    let mut keys = Vec::with_capacity(n);
    for _ in 0..n {
        for (j, &c) in cards.iter().enumerate() {
            ids[j] = (rng.next() % (c.max(1) as u64)) as i32;
        }
        keys.push(enc.pack(&ids));
    }
    keys
}

const SHAPES: &[(&str, &[i32])] = &[
    ("c02", &[200, 5000]),
    ("c03", &[200, 40, 1500]),
    ("c05", &[200, 16, 50, 8, 300]),
    ("c08", &[8, 8, 8, 8, 8, 8, 8, 8]),
    ("c16", &[16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16]),
];

const N: usize = 4096;

fn bench_unpack(c: &mut Criterion) {
    let mut g = c.benchmark_group("unpack");
    g.throughput(Throughput::Elements(N as u64));

    for (label, cards) in SHAPES {
        let keys = gen_keys(cards, N);
        let nc = cards.len();
        let enc = PackedKeyEncoder::new(cards).expect("cards fit in 64 bits");
        g.bench_with_input(BenchmarkId::from_parameter(label), &keys, |bn, keys| {
            let mut out = vec![0i32; nc];
            bn.iter(|| {
                for &k in keys.iter() {
                    enc.unpack(black_box(k), &mut out);
                    black_box(&out[..]);
                }
            });
        });
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(800))
        .measurement_time(Duration::from_secs(3))
        .sample_size(200);
    targets = bench_unpack
}
criterion_main!(benches);
