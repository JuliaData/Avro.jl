# Benchmarks

`AVRO_PERF=true` in the test suite runs the performance gates in `test/perf.jl`. Each number is the
median of 5 cold processes; workloads use 1 M deterministic `{id, x, name, flag}` rows. The gates
are calibrated for an Apple M3 Max and compare against Avro.jl 1.1.2 measured there with Julia
1.12.6 (2026-08-24): `writetable` 0.946 s, `readtable` plus materialisation 5.051 s.

| Gate | Threshold |
|---|---|
| `Avro.write`, null codec, 1 thread | ≤ 0.25 s and ≥ 4× faster than 1.1.2 |
| `Avro.Table`, null codec, 1 thread | ≤ 0.30 s and ≥ 10× faster than 1.1.2 |
| `Avro.Table` with `ntasks=8` and 4 GiB limits | ≥ 3× faster than 1 thread |
| zstandard / deflate / snappy `Table` | ≤ 1.3× (null-codec `Table` + raw decompression) |
| `select=(:id,)` with `validate=:fast` | ≥ 2× faster than the full decode |
| prepared typed decode | ≤ 1 allocation, ≤ 150 ns |
| prepared typed encode into a reused buffer | 0 allocations |
| `Avro.parseschema` of the interop schema | ≤ 100 µs |
| package load / time to first table | ≤ 0.5 s / ≤ 1.5 s |

The last recorded run on that host (Julia 1.12.6, 2026-08-28) wrote the 1 M rows in 0.185 s, 5.1×
faster than 1.1.2.
