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

//! Micro-benchmark for the **tiered segment GROUP BY key table**
//! (`SegmentTierDriver::feed_block`) -- the hot path that turns a block of
//! dict-id rows into group ids.
//!
//! ## Scope -- Rust-only, tier-vs-tier
//!
//! This is a **Rust-internal** benchmark: it isolates the tier *mechanisms*
//! against each other, not against the Java engine. It is deliberately narrow and
//! fast so it can be re-run quickly as the tier ladder evolves. The broad,
//! authoritative comparison is the **Java<->Rust sweep test**
//! (`NativeGroupBySweepTest`), which runs *entire queries* end-to-end across many
//! GROUP BY shapes, cardinalities, and LIMITs over a ~10M-row dataset and
//! exercises every tier through the real JNI path. Prefer the sweep for
//! native-vs-Java claims; use this bench for quick, low-level "how do the tiers
//! compare to each other" checks.
//!
//! ## What it measures -- every shape through *every* tier
//!
//! The tier ladder (design section 23) picks, per key shape, the cheapest key
//! table that fits:
//!
//!   * **T1 `BitPackedDirect`** -- pack columns into one key, index a *dense slot
//!     array* directly. No hashing. Feed hot path is shift + OR.
//!   * **T0 `RadixDirect`** -- mixed-radix key into a dense slot array. No hashing.
//!     Feed hot path is multiply + add.
//!   * **T2 `BitPackedHash`** -- pack into an `i64` key, probe a hashbrown hash
//!     table. The generic fallback; the only tier that actually hashes.
//!
//! Rather than pin each shape to the tier the ladder *would* pick, this bench
//! **forces the same key data through all three tiers** (via
//! [`SegmentTierDriver::new_for_tier`]) so the comparison is apples-to-apples: the
//! workload is held constant and only the key-table mechanism varies. To be
//! constructible in all three tiers a shape must keep its dense arrays allocatable
//! (`2^Σbits` and `∏Ci` slots), so every shape here stays in that region and
//! asserts all three drivers build before timing.
//!
//! ## Results (median ns/row, N=16384, release, Apple-silicon dev box)
//!
//! **(1) Affordable range -- does the auto-picked tier win?** `auto` = what
//! `select_tier` chooses in production; the memory budget is `2^20` dense slots
//! (~4 MB). "best-in-budget" = fastest tier whose array fits that budget.
//!
//! ```text
//!  shape             T1     T0     T2   auto  raw-fast  T1 array  fits 4MB   auto==best?
//!  [64,64]          1.83   1.99   3.60   T1     T1         4 K     T1+T0        yes
//!  [256,256]        3.25   3.39   6.81   T1     T1        64 K     T1+T0        yes
//!  [64,64,64]       4.73   4.92   7.00   T1     T1       256 K     T1+T0        yes
//!  [1024,1024]      5.67   5.79   7.25   T1     T1         1 M     T1+T0        yes
//!  [2048,512]       5.61   5.83   7.39   T1     T1         1 M     T1+T0        yes
//!  [1025,1000]      5.63   6.15   7.22   T0     T1         2 M     T0 only      yes (T1 over budget)
//!  [100,100,100]    5.80   6.22   7.27   T0     T1         2 M     T0 only      yes (T1 over budget)
//!  [130,130,60]     5.71   6.01   7.35   T0     T1         4 M     T0 only      yes (T1 over budget)
//!  [1025,513]       5.27   5.49   7.05   T0     T1         2 M     T0 only      yes (T1 over budget)
//!  [33,33,17,17]    4.72   5.74   7.16   T0     T1         4 M     T0 only      yes (13x waste; T1 still fastest)
//!  [1025,1024]      5.54   6.20   7.15   T2     T1         2 M     hash only    yes (both dense over budget)
//!  [1100,1100]      5.52   5.63   7.03   T2     T1         4 M     hash only    yes
//!  [2048,1024]      5.32   5.55   7.26   T2     T1         2 M     hash only    yes
//!  [1448,1448]      4.91   5.74   7.27   T2     T1         4 M     hash only    yes
//!  -> auto-pick == fastest tier that fits the 4 MB budget in 14/14 shapes.
//! ```
//!
//! **(2) Scale sweep -- where T2 actually wins** (dense array grows past
//! cache/RAM; measured with a construct-outside-timer probe, not committed):
//!
//! ```text
//!  shape            Σbits  dense array   T1     T0     T2      winner
//!  [4096,4096]        24     67 MB      3.80   3.58   5.98   dense (T0)
//!  [8192,8192]        26    256 MB      3.06   3.32   5.10   dense (T1)
//!  [16384,16384]      28      1 GB      3.46   3.51   5.19   dense (T1)
//!  [65536,16384]      30      4 GB    164.61 162.28   5.43   T2  (~30x faster)
//!  [1M,32K]           35    128 GB    can't allocate (panic) 5.x   T2 only
//! ```
//!
//! ## Findings
//!
//! 1. **Dense beats hash whenever it fits** -- T1/T0 are 1.2x-2.3x faster than T2
//!    across the affordable range.
//! 2. **T1 is always the fastest arithmetic; T0 is a pure memory rescue.** Even at
//!    13x bit-packing waste (`[33,33,17,17]`), T1's shift+OR beats T0's radix
//!    multiply+add. The ladder only leaves T1 when its `2^Σbits` array busts the
//!    budget -- so the auto-pick is the fastest tier that fits the budget, 14/14.
//! 3. **T2 is justified at scale, and that scale can't be head-to-head tested.**
//!    The speed crossover appears once the dense array blows out of cache/RAM
//!    (~4 GB here, T2 ~30x faster); beyond that the dense rivals can't be
//!    *allocated at all*. T2 survives exactly where the forced comparison becomes
//!    impossible -- which is why this bench stays in the affordable range and
//!    documents the large-scale regime rather than executing it.
//!
//! `Throughput::Elements(N)` makes Criterion report rows/second; compare the T0 /
//! T2 bars against the T1 bar for a given shape to read the ladder's payoff.

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use pinot_native_groupby::segment_tier::SegmentTierDriver;
use pinot_native_groupby::tier::Tier;
use pinot_native_groupby::AggKind;
use std::time::Duration;

/// Rows per fed block.
const N: usize = 16_384;

/// The three tiers each shape is driven through, with a stable label.
const TIERS: &[(&str, Tier)] = &[
    ("T1_bitpacked_direct", Tier::BitPackedDirect),
    ("T0_radix_direct", Tier::RadixDirect),
    ("T2_bitpacked_hash", Tier::BitPackedHash),
];

/// Key shapes spanning the tier boundaries, each with dense arrays kept
/// allocatable (`2^Σbits` and `∏Ci` slots both small enough to force through T1
/// and T0). Grouped by the tier `select_tier` auto-picks: T1 (Σbits ≤ 20),
/// T0 (Σbits > 20, ∏ ≤ 2^20), and T2 (∏ > 2^20). The T0/T2 groups include
/// "wasteful" shapes (cardinalities just over powers of two) that maximize T1's
/// bit-packing waste -- the strongest case for a dense-radix or hash win.
struct Shape {
    label: &'static str,
    cards: &'static [i32],
}

const SHAPES: &[Shape] = &[
    // ---- ladder picks T1 (Σbits ≤ 20): dense bit-packed array ----------------
    Shape { label: "2col_64x64_12bit", cards: &[64, 64] },
    Shape { label: "2col_256x256_16bit", cards: &[256, 256] },
    Shape { label: "3col_64x64x64_18bit", cards: &[64, 64, 64] },
    Shape { label: "2col_1024x1024_20bit", cards: &[1024, 1024] },
    Shape { label: "2col_2048x512_20bit", cards: &[2048, 512] },
    // ---- ladder picks T0 (Σbits > 20, ∏ ≤ 2^20): dense radix array -----------
    Shape { label: "2col_1025x1000_21bit", cards: &[1025, 1000] },
    Shape { label: "3col_100x100x100_21bit", cards: &[100, 100, 100] },
    Shape { label: "3col_130x130x60_22bit", cards: &[130, 130, 60] },
    // High bit-packing waste: cards just over powers of two → T1's 2^Σbits array
    // is mostly empty, so T0's exact ∏Cᵢ array should have far better locality.
    Shape { label: "2col_1025x513_wasteful", cards: &[1025, 513] },      // T1 2^21 vs ∏ 526K (~4x waste)
    Shape { label: "4col_33x33x17x17_wasteful", cards: &[33, 33, 17, 17] }, // T1 2^22 vs ∏ 315K (~13x waste)
    // ---- ladder picks T2 (∏ > 2^20): hash. Boundary shapes (∏ just over) -----
    Shape { label: "2col_1025x1024_over1", cards: &[1025, 1024] }, // ∏ only 1024 over budget
    Shape { label: "2col_1100x1100_22bit", cards: &[1100, 1100] },
    Shape { label: "2col_2048x1024_21bit", cards: &[2048, 1024] },
    Shape { label: "2col_1448x1448_22bit", cards: &[1448, 1448] },
];

/// Deterministic, allocation-free xorshift64 so blocks are identical across runs.
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

/// One column-major block of `n` dict-id rows: column `c` is `flat[c*n..(c+1)*n]`.
fn gen_block(cards: &[i32], n: usize) -> Vec<i32> {
    let num_columns = cards.len();
    let mut flat = vec![0i32; num_columns * n];
    let mut rng = XorShift(0x5EED_1234_ABCD_0001);
    for (c, &card) in cards.iter().enumerate() {
        let card = card.max(1) as u64;
        for row in 0..n {
            flat[c * n + row] = (rng.next() % card) as i32;
        }
    }
    flat
}

fn bench_feed_block(c: &mut Criterion) {
    // COUNT alone: `feed_block` builds the key table (what we measure); a single
    // cheap agg keeps per-group state minimal so timing reflects the key table.
    let agg_kinds = [AggKind::Count];

    let mut group = c.benchmark_group("segment_tier_feed_block");
    group.throughput(Throughput::Elements(N as u64));

    for shape in SHAPES {
        let num_columns = shape.cards.len();
        let block = gen_block(shape.cards, N);

        for (tier_label, tier) in TIERS {
            // Guard: the shape must be constructible in this forced tier.
            assert!(
                SegmentTierDriver::new_for_tier(shape.cards, &agg_kinds, *tier).is_some(),
                "shape {} not constructible in {tier_label}",
                shape.label
            );

            // Group by shape, parameterize by tier -> Criterion clusters the three
            // tier bars per shape for a direct read.
            let id = BenchmarkId::new(shape.label, tier_label);
            group.bench_with_input(id, &block, |b, block| {
                b.iter_batched(
                    // setup (untimed): fresh, empty driver for this tier -- its dense
                    // array / hash table allocation is excluded from the timing.
                    || SegmentTierDriver::new_for_tier(shape.cards, &agg_kinds, *tier).unwrap(),
                    // routine (timed): build all N groups from empty.
                    |mut driver| {
                        driver.feed_block(black_box(block), num_columns, N);
                        black_box(driver.num_groups())
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50);
    targets = bench_feed_block
}
criterion_main!(benches);
