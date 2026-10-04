# Benchmarks

Recorded on the authoring host (Apple silicon, macOS, Julia 1.12.6, 2026-08-23); reproduce with
`AVRO_PERF=true` in the test suite (`test/perf.jl`: the §10.1 protocol, medians of 5 cold processes). Baselines are Avro.jl 1.1.2 on the same host under the same §10.1 protocol (medians of 5 cold
processes, deterministic 1 M-row `{id, x, name, flag}` data; `benchmarks/logs/avro112.log`).

| Operation | Avro.jl 2.0 | Avro.jl 1.1.2 | ratio |
|---|---|---|---|
| `Avro.write`, null codec, 1 thread | 0.145 s | 0.92 s | 6.4× |
| `Avro.Table`, null codec, 1 thread | 0.126 s | 4.85 s (read + materialise) | 38× |
| `Avro.Table`, `ntasks=8`, 4 GiB limits | — | — | 3.39× vs 1 thread |
| `select=(:id,)`, `validate=:fast` | — | — | 2.4× vs full decode |
| zstandard / deflate / snappy `Table` vs null + raw transcode | 1.03× / 0.99× / 1.06× | — | ≤ 1.3× gate |

Micro-kernels (same host, §10.1 protocol — median of 5 cold processes): prepared typed decode —
**1 allocation (the string)**, ≈ 40 ns; prepared typed encode into a reused encoder —
**0 allocations**, ≈ 57 ns; one-shot decode/encode (plan construction per call) ≈ 12 µs / 11 µs;
`Avro.parseschema(interop.avsc)` ≈ 78 µs; package load ≈ 0.31 s; time-to-first-table ≈ 0.68 s.

For comparison (different implementations, same class of file): fastavro reads the 1 M-row file in
≈ 0.5 s; warm Java ≈ 0.05 s.
