# Multithreaded Radix Hash Join

Cache-conscious, multithreaded radix-partitioned hash join in C++17.

**Context.** This is the implementation part of *Project 2 — Radix Join* from **Advanced Data
Management Systems** (TU Darmstadt, FG Data Management, 2026). The course provided the project
skeleton: tuple/relation types, the test driver and the benchmark harness. This repository contains
**only the files I wrote** — the join itself — so it does not build standalone.

## Architecture

### Choosing the number of radix bits

The partitioning fan-out is derived from the machine rather than hard-coded:

```
N    = 4 * |R| * sizeof(tuple_t) * NUM_CORES / L3_CACHE_SIZE
bits = ceil(log2(N))
partitions = 1 << bits        mask = partitions - 1
```

The factor 4 is the target load-factor headroom for the per-partition hash table, so each partition
is expected to fit in L3 together with its hash table. The constructor stores `bits`, `partitions`
and `mask` in the join object; both relations are partitioned with the same fan-out.

### Partitioning (`partition_helper`)

Two parallel passes per relation, with a sequential prefix-sum step in between:

1. **Local histograms.** Thread `c` counts the keys of its contiguous chunk into
   `local_counts[c][partition]`, where `partition = key & mask`.
2. **Offsets.** Walking partitions outer and cores inner produces, for every `(core, partition)`, a
   unique write offset inside that partition's destination range — this is both the inter-partition
   layout and the per-thread write cursor. It also yields `start_points[partition]`, the beginning of
   every partition in the output buffer.
3. **Scatter.** A second parallel pass writes each tuple to `offsets[c][partition]` and increments
   the cursor. Each partition ends up contiguous, which is what makes probing cache-friendly.

`start_points` is returned with one extra sentinel entry so that the probe phase can address
`[start_points[p], start_points[p+1])` without bounds checks.

### Probing

Each of the `NUM_CORES` threads owns a contiguous block of partitions and processes them
independently:

- allocates a linear-probing table of size `4 * build_partition_size`;
- inserts the build rows, hashing on `key >> bits` and resolving collisions by linear probing with
  wraparound;
- probes with the probe rows, appending `(build_rid, probe_rid)` matches to a per-thread vector.

Per-thread result vectors are concatenated once at the end, so the hot loop never synchronizes.

## Limitations

- **At most 8 radix bits.** The `bits <= 8` guard selects the partitioning path; the `else` branch is
  empty, so inputs that would need more than 256 partitions are not partitioned at all. Supporting
  them requires a second partitioning pass or a different fan-out rule.
- **4× memory per partition.** The fixed load factor of 25% is what keeps probing fast, but it
  quadruples the build-side memory.
- **No skew handling.** A hot key produces one oversized partition whose hash table then misses L3,
  which is exactly the case the partitioning is meant to avoid.
- **No SIMD or software prefetching** in the probe loop; keys are compared with plain equality.
- **Static thread count.** The work split assumes `partitions` divides evenly across `NUM_CORES`;
  the last core takes the remainder.
- **Full copies.** Both relations are materialized into new partition buffers, so peak memory is
  roughly 2× the input.
- **Single pass.** One-pass partitioning is required by the assignment and assumed by the tests; the
  code does not verify that a partition actually fits in cache.

## Files

| Path | Contents |
|---|---|
| `src/RadixJoin.cpp` | Radix-bit computation, `partition_helper` (histograms, prefix sums, scatter), parallel probe |
| `include/RadixJoin.hpp` | Course-provided header, extended by me with the `bits` / `partitions` / `mask` members and the partition helper |

## Notes

- The repository deliberately leaves out the trivial edits I made to the course's `Types.hpp`
  and `CMakeLists.txt`, since those are course files rather than implementation.
- Exercised on the course's basic and advanced test cases through its CI pipeline.
