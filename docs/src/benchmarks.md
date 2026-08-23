# Benchmarks

Recorded on the authoring host (Apple silicon, macOS, Julia 1.12.6, 2026-08-22); reproduce with
`AVRO_PERF=true` in the test suite (`test/perf.jl`, best-of-3 timings). Baselines are Avro.jl 1.1.2
on the same host and file (1 M rows: `id::Int64`, `name::String`, `score::Float64`, `flag::Bool`).

| Operation | Avro.jl 2.0 | Avro.jl 1.1.2 | ratio |
|---|---|---|---|
| `Avro.write`, null codec, 1 thread | 0.19 s | 1.40 s | 7.4× |
| `Avro.Table`, null codec, 1 thread | 0.24 s | 5.69 s (read + materialise) | 23× |
| `Avro.Table`, `ntasks=8`, 4 GiB limits | 0.07 s | — | 3.35× vs 1 thread |
| `select=(:id,)`, `validate=:fast` | 0.04 s | — | 3.3× vs full decode |
| zstandard `Table` vs null + raw transcode | 1.04× | — | ≤ 1.3× gate |

Micro-kernels (same host): prepared typed encode of a 4-field record into a reused encoder —
**0 allocations**, ≈ 300 ns; prepared typed decode ≈ 850 ns (8 allocations through the
`DatumReader` wrapper; the plan-level kernel allocates only the `String`);
`Avro.parseschema(interop.avsc)` ≈ 58 µs; package load ≈ 0.37 s.

For comparison (different implementations, same class of file): fastavro reads the 1 M-row file in
≈ 0.5 s; warm Java ≈ 0.05 s.
