# Performance

[Benchmarks](../benchmarks.md) lists the performance gates and how to run them.

## Getting the numbers

* **Prepared beats one-shot**: `DatumReader`/`DatumWriter` skip per-call plan construction. The typed
  `DatumWriter(schema, T)` encodes aligned `NamedTuple`s with zero allocations.
* **Writers fast-path aligned NamedTuples** automatically — rows whose fields match the record schema
  in order encode monomorphized.
* **Projection**: `select=` skips what you don't read; `validate=:fast` additionally jumps sized
  blocks (choose it only for trusted data).
* **Parallelism needs headroom**: default limits leave none, so raise the ceiling
  (`Limits(max_total_bytes=4 << 30)`) for `ntasks > 1` to engage, and give files multi-megabyte blocks
  (`block_bytes=` on the writer) so per-block overheads amortise.
* **Column access beats row access** for bulk work: `Avro.Table` decodes straight into typed columns.
* Typed decoding into concrete structs avoids the generic model's boxing for isbits fields.
