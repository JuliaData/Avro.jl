# Performance

Measured on an Apple-silicon authoring host, Julia 1.12 (see `benchmarks.md` for the recorded runs):

* `Avro.write` of 1 M rows (4 columns, null codec): ≈ 0.19 s — about 7× faster than Avro.jl 1.1.2.
* `Avro.Table` of the same file: ≈ 0.24 s single-threaded (≈ 23× faster than 1.1.2
  read + materialise); ≈ 3.3× further with `ntasks=8` under raised limits.
* Projection `select=(:id,)` with `validate=:fast`: ≈ 3× faster than the full decode.
* Codec overhead: a zstandard `Table` costs ≈ 1.04× of the null-codec decode plus the raw transcode.

## Getting the numbers

* **Prepared beats one-shot**: `DatumReader`/`DatumWriter` skip per-call plan construction. The typed
  `DatumWriter(schema, T)` encodes aligned `NamedTuple`s with zero allocations.
* **Writers fast-path aligned NamedTuples** automatically — rows whose fields match the record schema
  in order encode monomorphized.
* **Projection**: `select=` skips what you don't read; `validate=:fast` additionally jumps sized
  blocks (choose it only for trusted data — strict is the default for a reason).
* **Parallelism needs headroom**: default limits deliberately leave almost none, so raise the ceiling
  (`Limits(max_total_bytes=4 << 30)`) for `ntasks > 1` to engage, and give files multi-megabyte blocks
  (`block_bytes=` on the writer) so per-block overheads amortise.
* **Column access beats row access** for bulk work: `Avro.Table` decodes straight into typed columns.
* Typed decoding into concrete structs avoids the generic model's boxing for isbits fields.
