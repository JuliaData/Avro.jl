# Performance

[Benchmarks](../benchmarks.md) lists the performance gates and how to run them.

## Getting the numbers

* **Prepared beats one-shot**: `DatumReader`/`DatumWriter` skip per-call plan construction. For repeated
  writes, reuse a typed `DatumWriter(schema, T)` and an output `IO` or `Encoder`; calling `writer(row)`
  returns a new owned byte vector each time.
* **Writers fast-path aligned NamedTuples** automatically — rows whose fields match the record schema
  in order (up to 32 fields) encode monomorphized. Other column tables and strided vectors of concrete
  `NamedTuple` rows use typed field cells when every schema field is present. Wide and reordered rows
  reuse the same encoding and limit checks without building a generic record for each row. Custom
  and lazy row vectors retain row-by-row access.
* **Projection**: `select=` skips what you don't read; `validate=:fast` additionally jumps sized
  blocks (choose it only for trusted data).
* **Parallelism needs headroom** under `max_total_bytes` (ample at the default) and files with
  multi-megabyte blocks (`block_bytes=` on the writer) so per-block overheads amortise.
* **Column access beats row access** for bulk work: `Avro.Table` decodes straight into typed columns.
* Typed decoding into concrete structs avoids the generic model's boxing for isbits fields.
