# Benchmarks

`AVRO_PERF=true` in the test suite runs the performance gates in `test/perf.jl`. Each number is the
median of 5 fresh processes. Within each process, workload metrics use the best of 3 repetitions
after one warm-up; load and time to first table measure cold startup directly. Workloads use 1 M
deterministic `{id, x, name, flag}` rows. The gates
are calibrated for an Apple M3 Max and compare against Avro.jl 1.1.2 measured there with Julia
1.12.6 (2026-08-24): `writetable` 0.946 s, `readtable` plus materialisation 5.051 s.

| Gate | Threshold |
|---|---|
| `Avro.write`, null codec, 1 thread | ≤ 0.25 s and ≥ 4× faster than 1.1.2 |
| `Avro.Table`, null codec, 1 thread | ≤ 0.30 s and ≥ 10× faster than 1.1.2 |
| `Avro.Table` with `ntasks=8` (1 MiB blocks) | ≥ 3× faster than 1 thread |
| zstandard / deflate / snappy `Table` | ≤ 1.3× (null-codec `Table` + raw decompression) |
| `select=(:id,)` | ≥ 2× faster than the full decode |
| prepared typed decode | ≤ 1 allocation, ≤ 150 ns |
| prepared typed encode into a reused buffer | 0 allocations |
| `Avro.parseschema` of the interop schema | ≤ 100 µs |
| package load / time to first table | ≤ 0.5 s / ≤ 1.5 s |

A run on that host on 2026-11-01 (Julia 1.12.6, with other work running) wrote the 1 M rows in 0.058 s,
16× faster than 1.1.2, and read them into a table in 0.083 s, 61× faster.
