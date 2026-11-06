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
| `select=(:id,)` | ≥ 1.5× faster than the full decode |
| prepared typed decode | ≤ 1 allocation, ≤ 150 ns |
| prepared typed encode into a reused buffer | 0 allocations |
| `Avro.parseschema` of the interop schema | ≤ 100 µs |
| package load / time to first table | ≤ 0.5 s / ≤ 1.5 s |

A run on that host on 2026-11-04 (Julia 1.12.6, with other work running) wrote the 1 M rows in 0.059 s,
16× faster than 1.1.2, and read them into a table in 0.033 s, about 150× faster.

The default test suite checks what does not depend on the host (`test/gates.jl`): parsing the interop schema
takes at most 1,000 allocations, a prepared typed decode at most 1, and after a warm-up, decoding random schemas
creates no new method instances.

## Against other Avro libraries

`benchmark/peers` measures Avro.jl against fastavro, Apache Avro Java, apache-avro and arrow-avro (Rust) and
hamba/avro (Go) on four files of 200 to 470 MB: fixed-width numbers, short strings, an event log and nested data.
Every reader must report the same checksums, another library reads back every written file, and the sides run in
fresh processes in rotating order. Its README has the method, every table and the analysis behind each gap.

On the same host (Julia 1.12.6, 3 runs per side, other work keeping the load average at 20 to 65), the fastest
single-threaded Avro.jl reader, typed rows, against the fastest other library:

| file | null | deflate | zstandard | snappy |
|---|---|---|---|---|
| numeric, 10M rows | 1.96× arrow-avro | 1.50× arrow-avro | 1.66× arrow-avro | 2.45× Java |
| event log, 4M rows | 1.10× arrow-avro | 1.55× arrow-avro | 1.03× arrow-avro | 1.36× Java |
| strings, 2M rows | 0.72× Java | 1.17× arrow-avro | 0.91× arrow-avro | 0.74× Java |
| nested, 3M rows | 0.48× arrow-avro | 0.78× arrow-avro | 0.62× arrow-avro | 1.05× Java |

Above 1, Avro.jl is faster. With 14 threads, `Avro.Table` reads the numeric, strings and event-log files faster
than any single-threaded reader with every codec. Deflate writes are 1.43× to 1.91× the fastest other writer on
one thread, and with more threads Avro.jl compresses several blocks at once. Uncompressed writes are level with
arrow-avro on numbers and 0.40× to 0.65× the fastest other writer on strings, the event log and nested data.
