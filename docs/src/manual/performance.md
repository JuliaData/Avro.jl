# Performance

[Benchmarks](../benchmarks.md) lists the performance gates and how to run them.

## Getting the numbers

* **Prepared beats one-shot**: `DatumReader`/`DatumWriter` skip per-call plan construction. The typed
  `DatumWriter(schema, T)` encodes aligned `NamedTuple`s with zero allocations.
* **Writers fast-path aligned NamedTuples** automatically — rows whose fields match the record schema
  in order (up to 32 fields) encode monomorphized. Other column tables with a column for every field
  encode through one call per cell, without building a row value.
* **Projection**: `select=` skips what you don't read; `validate=:fast` additionally jumps sized
  blocks (choose it only for trusted data).
* **Parallelism needs headroom** under `max_total_bytes` (ample at the default) and files with
  multi-megabyte blocks (`block_bytes=` on the writer) so per-block overheads amortise.
* **Column access beats row access** for bulk work: `Avro.Table` decodes straight into typed columns.
* Typed decoding into concrete structs avoids the generic model's boxing for isbits fields.
