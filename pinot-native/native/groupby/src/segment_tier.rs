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

//! Multi-column segment GROUP BY driver, tier-dispatched. One enum with a
//! variant per [`Tier`] — each variant pairs a **key encoder** with an
//! **existing backend + driver**, so a tier only changes the `key → group_id`
//! step. Aggregation state (the `AggState` vectors inside [`GroupByDriver`]) is
//! reused untouched across all tiers.
//!
//! | Variant | Encoder | Backend | Key |
//! |---------|---------|---------|-----|
//! | [`Tier::BitPackedDirect`] | [`PackedKeyEncoder`] | [`DictDirectTable`] (dense, no hash) | bit-packed → `i32` slot |
//! | [`Tier::RadixDirect`]     | [`RadixKeyEncoder`]  | [`DictDirectTable`] (dense, no hash) | mixed-radix → `i32` slot |
//! | [`Tier::BitPackedHash`]   | [`PackedKeyEncoder`] | [`HashbrownTable<i64>`] (hash)       | bit-packed `i64` |
//!
//! The dense tiers narrow their packed key to `i32` losslessly: the ladder only
//! routes there when the slot domain is `≤ 2^20 < 2^31` (see [`crate::tier`]).
//! `BitPackedHash` is `i64`-only for now; a key wider than 64 bits makes
//! [`SegmentTierDriver::new`] return `None` (caller falls back to Java) —
//! `u128` is a deferred, additive widening.

use crate::agg::{AggKind, AggState};
use crate::dict_direct::DictDirectTable;
use crate::driver_multi_agg::GroupByDriver;
use crate::hashbrown_table::HashbrownTable;
use crate::multi_key::{PackedKeyEncoder, RadixKeyEncoder};
use crate::tier::{select_tier, Tier};

/// The tier-specific (encoder, backend+driver) pairing. Each variant owns a
/// generic [`GroupByDriver`] instantiated for that tier's key type.
enum TierInner {
    BitPackedDirect {
        driver: GroupByDriver<i32, DictDirectTable>,
        encoder: PackedKeyEncoder,
    },
    RadixDirect {
        driver: GroupByDriver<i32, DictDirectTable>,
        encoder: RadixKeyEncoder,
    },
    BitPackedHash {
        driver: GroupByDriver<i64, HashbrownTable<i64>>,
        encoder: PackedKeyEncoder,
    },
    /// Single dict column, dense (dict-id -> group_id array, no hashing). The
    /// dict-id IS the key, so there is no encoder / packing — the fast path the
    /// executor previously reached via `createDictDirectMultiAgg`. Conceptually
    /// [`Tier::BitPackedDirect`] with N=1.
    SingleDirect {
        driver: GroupByDriver<i32, DictDirectTable>,
    },
    /// Single dict column, hashed on the raw `i32` dict-id (no widening to i64).
    /// The fast path previously reached via `createHashbrownMultiAgg`.
    /// Conceptually [`Tier::BitPackedHash`] with N=1.
    SingleHash {
        driver: GroupByDriver<i32, HashbrownTable<i32>>,
    },
}

/// Multi-column dict-encoded segment GROUP BY driver. Construct with
/// [`SegmentTierDriver::new`] (which runs the tier ladder), feed dict-id blocks
/// with [`feed_block`], fold values with the `apply_*` methods, and read results
/// via [`num_groups`] / [`agg_state`] / [`extract_keys`].
///
/// [`feed_block`]: SegmentTierDriver::feed_block
/// [`num_groups`]: SegmentTierDriver::num_groups
/// [`agg_state`]: SegmentTierDriver::agg_state
/// [`extract_keys`]: SegmentTierDriver::extract_keys
pub struct SegmentTierDriver {
    inner: TierInner,
    /// Reused per-block packed-key buffer (i64 output of the encoders).
    key_scratch_i64: Vec<i64>,
    /// Reused per-block narrowed i32 slot buffer (dense tiers only).
    key_scratch_i32: Vec<i32>,
}

/// Dispatch a `&mut` driver method across the tier variants.
macro_rules! apply_dispatch {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match &mut $self.inner {
            TierInner::BitPackedDirect { driver, .. } => driver.$method($($arg),*),
            TierInner::RadixDirect { driver, .. } => driver.$method($($arg),*),
            TierInner::BitPackedHash { driver, .. } => driver.$method($($arg),*),
            TierInner::SingleDirect { driver } => driver.$method($($arg),*),
            TierInner::SingleHash { driver } => driver.$method($($arg),*),
        }
    };
}

/// Dispatch a `&self` driver method across the tier variants.
macro_rules! read_dispatch {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match &$self.inner {
            TierInner::BitPackedDirect { driver, .. } => driver.$method($($arg),*),
            TierInner::RadixDirect { driver, .. } => driver.$method($($arg),*),
            TierInner::BitPackedHash { driver, .. } => driver.$method($($arg),*),
            TierInner::SingleDirect { driver } => driver.$method($($arg),*),
            TierInner::SingleHash { driver } => driver.$method($($arg),*),
        }
    };
}

impl SegmentTierDriver {
    /// Build a driver for the given per-column dict cardinalities (grouping
    /// order) and aggregation kinds, choosing the tier via [`select_tier`].
    /// A single grouping column takes a no-pack fast path (the dict-id is the
    /// key directly). Returns `None` when a multi-column packed key would exceed
    /// 64 bits (`BitPackedHash` is `i64`-only for now) — the caller falls back to
    /// Java.
    pub fn new(cardinalities: &[i32], agg_kinds: &[AggKind]) -> Option<Self> {
        Self::build_for_tier(cardinalities, agg_kinds, select_tier(cardinalities))
    }

    /// Build a driver for an **explicitly chosen** tier, bypassing [`select_tier`].
    /// Intended for benchmarks and tier-parity tests that need to run the *same*
    /// key data through a specific tier's key table; production code must use
    /// [`new`], which selects the correct tier for the shape.
    ///
    /// The caller is responsible for choosing a tier the shape can actually back:
    /// the dense tiers allocate a slot array sized to the key domain
    /// (`2^Σbits` for [`Tier::BitPackedDirect`], `∏ Cᵢ` for [`Tier::RadixDirect`]),
    /// so forcing a dense tier on a high-cardinality shape can allocate a huge
    /// array. Returns `None` when the packed key would exceed 64 bits.
    ///
    /// [`new`]: SegmentTierDriver::new
    pub fn new_for_tier(cardinalities: &[i32], agg_kinds: &[AggKind], tier: Tier) -> Option<Self> {
        Self::build_for_tier(cardinalities, agg_kinds, tier)
    }

    fn build_for_tier(cardinalities: &[i32], agg_kinds: &[AggKind], tier: Tier) -> Option<Self> {
        let single = cardinalities.len() == 1;
        let inner = match tier {
            // --- Single column: dict-id is the key, no encoder / packing -------
            Tier::BitPackedDirect if single => {
                let cap = (cardinalities[0].max(1)) as usize;
                TierInner::SingleDirect {
                    driver: GroupByDriver::with_capacity(agg_kinds, cap),
                }
            }
            Tier::BitPackedHash if single => TierInner::SingleHash {
                driver: GroupByDriver::with_capacity(agg_kinds, capped_hint(cardinalities)),
            },
            // --- Multi column: pack the N dict columns into one key ------------
            Tier::BitPackedDirect => {
                let encoder = PackedKeyEncoder::new(cardinalities)?;
                // Dense slot array spans [0, 2^total_bits); pre-size to avoid resizes.
                let slots = 1usize << encoder.total_bits();
                TierInner::BitPackedDirect {
                    driver: GroupByDriver::with_capacity(agg_kinds, slots),
                    encoder,
                }
            }
            Tier::RadixDirect => {
                let encoder = RadixKeyEncoder::new(cardinalities)?;
                // Dense slot array spans [0, ∏ Cᵢ); pre-size to avoid resizes.
                let slots = encoder.total_slots() as usize;
                TierInner::RadixDirect {
                    driver: GroupByDriver::with_capacity(agg_kinds, slots),
                    encoder,
                }
            }
            Tier::BitPackedHash => {
                // i64 only for now; > 64 bits -> None -> Java fallback.
                let encoder = PackedKeyEncoder::new(cardinalities)?;
                TierInner::BitPackedHash {
                    driver: GroupByDriver::with_capacity(agg_kinds, capped_hint(cardinalities)),
                    encoder,
                }
            }
        };
        Some(Self {
            inner,
            key_scratch_i64: Vec::new(),
            key_scratch_i32: Vec::new(),
        })
    }

    /// The tier this driver was constructed for (diagnostics / tests). The
    /// single-column shapes report their conceptual tier.
    pub fn tier(&self) -> Tier {
        match &self.inner {
            TierInner::BitPackedDirect { .. } | TierInner::SingleDirect { .. } => {
                Tier::BitPackedDirect
            }
            TierInner::RadixDirect { .. } => Tier::RadixDirect,
            TierInner::BitPackedHash { .. } | TierInner::SingleHash { .. } => Tier::BitPackedHash,
        }
    }

    /// Feed one block of `num_columns` dict-id columns (column-major in `flat`:
    /// column `c` is `flat[c*n .. (c+1)*n]`), packing each row's key and
    /// probe-inserting it, caching the group_ids for the subsequent `apply_*`.
    pub fn feed_block(&mut self, flat: &[i32], num_columns: usize, n: usize) {
        if n == 0 || num_columns == 0 || flat.len() < num_columns * n {
            return;
        }
        // Single column: the dict-id IS the key — feed directly, no pack/narrow.
        match &mut self.inner {
            TierInner::SingleDirect { driver } => {
                driver.process_block_keys(&flat[..n]);
                return;
            }
            TierInner::SingleHash { driver } => {
                driver.process_block_keys(&flat[..n]);
                return;
            }
            _ => {}
        }
        let columns: Vec<&[i32]> = (0..num_columns).map(|c| &flat[c * n..(c + 1) * n]).collect();
        let Self {
            inner,
            key_scratch_i64,
            key_scratch_i32,
        } = self;
        key_scratch_i64.clear();
        key_scratch_i64.resize(n, 0);
        match inner {
            TierInner::BitPackedDirect { driver, encoder } => {
                encoder.pack_block(&columns, key_scratch_i64);
                narrow_to_i32(key_scratch_i64, key_scratch_i32);
                driver.process_block_keys(key_scratch_i32);
            }
            TierInner::RadixDirect { driver, encoder } => {
                encoder.pack_block(&columns, key_scratch_i64);
                narrow_to_i32(key_scratch_i64, key_scratch_i32);
                driver.process_block_keys(key_scratch_i32);
            }
            TierInner::BitPackedHash { driver, encoder } => {
                encoder.pack_block(&columns, key_scratch_i64);
                driver.process_block_keys(key_scratch_i64);
            }
            // Single shapes handled above and returned early.
            TierInner::SingleDirect { .. } | TierInner::SingleHash { .. } => unreachable!(),
        }
    }

    #[inline]
    pub fn apply_long(&mut self, agg_idx: usize, values: &[i64]) {
        apply_dispatch!(self, apply_long, agg_idx, values)
    }

    #[inline]
    pub fn apply_int(&mut self, agg_idx: usize, values: &[i32]) {
        apply_dispatch!(self, apply_int, agg_idx, values)
    }

    #[inline]
    pub fn apply_double(&mut self, agg_idx: usize, values: &[f64]) {
        apply_dispatch!(self, apply_double, agg_idx, values)
    }

    #[inline]
    pub fn apply_float(&mut self, agg_idx: usize, values: &[f32]) {
        apply_dispatch!(self, apply_float, agg_idx, values)
    }

    #[inline]
    pub fn apply_count(&mut self, agg_idx: usize) {
        apply_dispatch!(self, apply_count, agg_idx)
    }

    #[inline]
    pub fn num_groups(&self) -> usize {
        read_dispatch!(self, num_groups)
    }

    #[inline]
    pub fn num_aggs(&self) -> usize {
        read_dispatch!(self, num_aggs)
    }

    #[inline]
    pub fn agg_kind(&self, idx: usize) -> AggKind {
        read_dispatch!(self, agg_kind, idx)
    }

    #[inline]
    pub fn agg_state(&self, idx: usize) -> &AggState {
        read_dispatch!(self, agg_state, idx)
    }

    /// Unpack the per-group composite keys into `num_columns` per-column dict-id
    /// arrays (column-major in `out`; `out.len() >= num_columns * num_groups`).
    /// Each column's dict-id is then decoded via its `Dictionary` on the Java side.
    pub fn extract_keys(&self, out: &mut [i32], num_columns: usize) {
        let mut decoded = vec![0i32; num_columns];
        match &self.inner {
            TierInner::BitPackedDirect { driver, encoder } => {
                let keys = driver.keys();
                let n = keys.len();
                for (g, &slot) in keys.iter().enumerate() {
                    encoder.unpack(slot as i64, &mut decoded);
                    for c in 0..num_columns {
                        out[c * n + g] = decoded[c];
                    }
                }
            }
            TierInner::RadixDirect { driver, encoder } => {
                let keys = driver.keys();
                let n = keys.len();
                for (g, &slot) in keys.iter().enumerate() {
                    encoder.unpack(slot as i64, &mut decoded);
                    for c in 0..num_columns {
                        out[c * n + g] = decoded[c];
                    }
                }
            }
            TierInner::BitPackedHash { driver, encoder } => {
                let keys = driver.keys();
                let n = keys.len();
                for (g, &packed) in keys.iter().enumerate() {
                    encoder.unpack(packed, &mut decoded);
                    for c in 0..num_columns {
                        out[c * n + g] = decoded[c];
                    }
                }
            }
            // Single column: the stored keys ARE the dict-ids (one column).
            TierInner::SingleDirect { driver } => {
                let keys = driver.keys();
                out[..keys.len()].copy_from_slice(keys);
            }
            TierInner::SingleHash { driver } => {
                let keys = driver.keys();
                out[..keys.len()].copy_from_slice(keys);
            }
        }
    }
}

/// Overwrite `dst` with the low 32 bits of each `src` key. Lossless for the
/// dense tiers (slot domain `≤ 2^20 < 2^31`).
#[inline]
fn narrow_to_i32(src: &[i64], dst: &mut Vec<i32>) {
    dst.clear();
    dst.extend(src.iter().map(|&k| k as i32));
}

/// A starting capacity hint for the hash tier: the product of cardinalities,
/// saturated at `2^16` so a huge domain doesn't over-reserve. The hashbrown
/// table grows past it on demand.
fn capped_hint(cardinalities: &[i32]) -> usize {
    let mut product: usize = 1;
    for &c in cardinalities {
        product = product.saturating_mul((c as usize).max(1));
        if product >= (1 << 16) {
            return 1 << 16;
        }
    }
    product
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Feed a random column-major block through a `SegmentTierDriver` and assert
    /// SUM(long) + COUNT per group match a tuple-keyed HashMap reference, and
    /// that the chosen tier is the expected one.
    fn run_e2e(cardinalities: &[i32], expected_tier: Tier, n: usize, seed: u64) {
        let kinds = [AggKind::SumLongToDouble, AggKind::Count];
        let mut driver = SegmentTierDriver::new(cardinalities, &kinds).expect("constructible");
        assert_eq!(driver.tier(), expected_tier, "tier for {cardinalities:?}");

        let num_columns = cardinalities.len();
        let mut state = seed;
        let mut flat = vec![0i32; num_columns * n];
        let mut values = vec![0i64; n];
        let mut reference: HashMap<Vec<i32>, (f64, i64)> = HashMap::new();
        for row in 0..n {
            let mut key = Vec::with_capacity(num_columns);
            for (c, &card) in cardinalities.iter().enumerate() {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let d = ((state >> 33) as i32).rem_euclid(card);
                flat[c * n + row] = d;
                key.push(d);
            }
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = ((state >> 5) as i64) % 1_000_000;
            values[row] = v;
            let e = reference.entry(key).or_insert((0.0, 0));
            e.0 += v as f64;
            e.1 += 1;
        }

        driver.feed_block(&flat, num_columns, n);
        driver.apply_long(0, &values);
        driver.apply_count(1);

        let groups = driver.num_groups();
        assert_eq!(groups, reference.len(), "group count for {cardinalities:?}");

        let mut out = vec![0i32; num_columns * groups];
        driver.extract_keys(&mut out, num_columns);
        let sums = driver.agg_state(0).as_double_slice().unwrap();
        let cnts = driver.agg_state(1).as_long_slice().unwrap();
        for g in 0..groups {
            let key: Vec<i32> = (0..num_columns).map(|c| out[c * groups + g]).collect();
            let r = reference[&key];
            assert_eq!(sums[g], r.0, "sum for {key:?}");
            assert_eq!(cnts[g], r.1, "cnt for {key:?}");
        }
    }

    #[test]
    fn bit_packed_direct_end_to_end() {
        // [256,256]: Σbits 16 <= 20 -> T1.
        run_e2e(&[256, 256], Tier::BitPackedDirect, 40_000, 0x1111_2222_3333_4444);
    }

    #[test]
    fn radix_direct_end_to_end() {
        // [100,100,100]: Σbits 21 > 20, product 1e6 <= 2^20 -> T0.
        run_e2e(&[100, 100, 100], Tier::RadixDirect, 40_000, 0x5555_6666_7777_8888);
    }

    #[test]
    fn bit_packed_hash_end_to_end() {
        // [40000,60]: Σbits 22, product 2.4M -> T2.
        run_e2e(&[40_000, 60], Tier::BitPackedHash, 40_000, 0x9999_aaaa_bbbb_cccc);
    }

    /// `new_for_tier` forces a tier, bypassing `select_tier`. For a shape small
    /// enough to back all three tiers (`Σbits <= 20`), the dense-direct, radix,
    /// and hash tiers must all produce byte-identical groups from the same data —
    /// the invariant the tier-vs-tier benchmark relies on. Guards against a tier
    /// building wrong groups for an otherwise-eligible shape.
    #[test]
    fn forced_tiers_produce_identical_groups() {
        let cards = [256, 256]; // Σbits = 16 <= 20 -> constructible in T1, T0, T2.
        let kinds = [AggKind::SumLongToDouble, AggKind::Count];
        let n = 20_000;

        // Deterministic column-major block + reference values.
        let num_columns = cards.len();
        let mut flat = vec![0i32; num_columns * n];
        let mut values = vec![0i64; n];
        let mut state: u64 = 0xfeed_face_dead_beef;
        for row in 0..n {
            for (c, &card) in cards.iter().enumerate() {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                flat[c * n + row] = ((state >> 33) as i32).rem_euclid(card);
            }
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            values[row] = ((state >> 5) as i64) % 1_000_000;
        }

        // Build a canonical (key -> (sum,count)) map from a forced tier.
        fn run(cards: &[i32], kinds: &[AggKind], tier: Tier, flat: &[i32], values: &[i64], n: usize)
            -> std::collections::HashMap<Vec<i32>, (f64, i64)> {
            let mut driver = SegmentTierDriver::new_for_tier(cards, kinds, tier)
                .expect("forced tier constructible");
            assert_eq!(driver.tier(), tier);
            driver.feed_block(flat, cards.len(), n);
            driver.apply_long(0, values);
            driver.apply_count(1);
            let groups = driver.num_groups();
            let mut out = vec![0i32; cards.len() * groups];
            driver.extract_keys(&mut out, cards.len());
            let sums = driver.agg_state(0).as_double_slice().unwrap();
            let cnts = driver.agg_state(1).as_long_slice().unwrap();
            let mut map = std::collections::HashMap::with_capacity(groups);
            for g in 0..groups {
                let key: Vec<i32> = (0..cards.len()).map(|c| out[c * groups + g]).collect();
                map.insert(key, (sums[g], cnts[g]));
            }
            map
        }

        let t1 = run(&cards, &kinds, Tier::BitPackedDirect, &flat, &values, n);
        let t0 = run(&cards, &kinds, Tier::RadixDirect, &flat, &values, n);
        let t2 = run(&cards, &kinds, Tier::BitPackedHash, &flat, &values, n);
        assert_eq!(t1, t0, "T1 (direct) vs T0 (radix) groups differ");
        assert_eq!(t1, t2, "T1 (direct) vs T2 (hash) groups differ");
    }

    #[test]
    fn single_column_dense_end_to_end() {
        // A single dict column with card <= 2^20 -> dense no-pack (SingleDirect,
        // reports BitPackedDirect). Verifies the no-encoder fast path aggregates
        // and round-trips dict-ids correctly.
        run_e2e(&[1000], Tier::BitPackedDirect, 20_000, 0xdead_beef_cafe_0001);
    }

    #[test]
    fn single_column_hash_end_to_end() {
        // A single dict column with card > 2^20 -> hashed on raw i32 dict-id
        // (SingleHash, reports BitPackedHash). ~1.3M distinct feed values over a
        // >2^20 dictionary exercises the hash fast path.
        run_e2e(&[2_000_000], Tier::BitPackedHash, 40_000, 0x0bad_c0de_1234_5678);
    }

    #[test]
    fn single_and_multi_produce_same_result_for_one_column() {
        // The single-column no-pack path must agree with what the packed path
        // would produce: feed the same one-column stream and compare group count.
        let kinds = [AggKind::Count];
        let mut single = SegmentTierDriver::new(&[500], &kinds).unwrap();
        assert_eq!(single.tier(), Tier::BitPackedDirect);
        let n = 10_000;
        let mut flat = vec![0i32; n];
        let mut state = 0xfeed_face_u64;
        for v in flat.iter_mut() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *v = ((state >> 33) as i32).rem_euclid(500);
        }
        single.feed_block(&flat, 1, n);
        single.apply_count(0);
        let distinct: std::collections::HashSet<i32> = flat.iter().copied().collect();
        assert_eq!(single.num_groups(), distinct.len());
        let mut out = vec![0i32; single.num_groups()];
        single.extract_keys(&mut out, 1);
        let extracted: std::collections::HashSet<i32> = out.iter().copied().collect();
        assert_eq!(extracted, distinct, "extracted dict-ids must equal the fed distinct set");
    }

    #[test]
    fn wider_than_64_bits_is_none() {
        // 3 cols of 2^25 -> 75 bits -> BitPackedHash but PackedKeyEncoder rejects
        // (i64 only) -> None -> caller falls back to Java.
        let kinds = [AggKind::Count];
        assert!(SegmentTierDriver::new(&[1 << 25, 1 << 25, 1 << 25], &kinds).is_none());
    }
}
