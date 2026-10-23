# Multithreaded Radix Hash Join

Cache-conscious, multithreaded radix-partitioned hash join in C++17.

**Context.** This is the implementation part of *Project 2 — Radix Join* from **Advanced Data
Management Systems** (TU Darmstadt, FG Data Management, 2026). The course provided the project
skeleton: tuple/relation types, the test driver and the benchmark harness. This repository contains
**only the files I wrote** — the join itself — so it does not build standalone.

## What I implemented

**Radix-bit selection.** The number of radix bits is derived from the machine instead of being
hard-coded: with `N = 4 * num_tuples * sizeof(tuple) * NUM_CORES / L3_CACHE_SIZE`, the join uses
`bits = ceil(log2(N))` partitions, so that the build-side hash table of every partition is expected
to fit in L3. `partitions = 1 << bits` and `mask = partitions - 1` are stored in the join object.

**Partitioning (`partition_helper`)** — two parallel passes over the relation:

1. Per-thread histograms: thread `c` counts keys in its own contiguous chunk into
   `local_counts[c][partition]`.
2. Prefix sums: for every partition, the per-core start offsets are computed by walking the cores
   in order, which produces both the write offsets per (core, partition) and the global
   `start_points` array for the partition boundaries.
3. Scatter: a second parallel pass places each tuple at its precomputed offset and increments it,
   so each partition ends up contiguous in the output buffer.

The same routine partitions both build (R) and probe (S) relations.

**Probing.** Each of the `NUM_CORES` threads owns a contiguous block of partitions. For every
partition it allocates a linear-probing hash table sized `4 * build_partition_size`, inserts the
build rows hashing on `key >> bits`, then probes with the probe rows, appending
`(build_rid, probe_rid)` pairs to a per-thread result list. Per-thread lists are concatenated at the
end, so no synchronization is needed in the hot loop.

## Files

| Path | Contents |
|---|---|
| `src/RadixJoin.cpp` | Radix-bit computation, `partition_helper` (histograms, prefix sums, scatter), parallel probe |
| `include/RadixJoin.hpp` | `RadixJoin` declaration with the `bits` / `partitions` / `mask` members and the partition helper |
| `include/Types.hpp` | Adds a `tuple_t(key, rid)` constructor used by the join |

## Notes

- One partitioning pass only, as required by the assignment; the benchmark's test cases were sized
  so that a second pass is not needed.
- Exercised on the course's basic and advanced test cases through its CI pipeline.
