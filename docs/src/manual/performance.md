# Performance

[Benchmarks](../benchmarks.md) lists the performance gates and how to run them.

## Getting the numbers

* **Prepared beats one-shot**: `DatumReader`/`DatumWriter` skip per-call plan construction. For repeated
  writes, reuse a typed `DatumWriter(schema, T)` and an output `IO`; calling `writer(row)` returns a new
  owned byte vector each time, `writer(io, row)` appends without allocating one.
* **Writers fast-path aligned rows** automatically: NamedTuples and structs whose fields match the record
  schema in order (up to 32 fields) encode monomorphized. Other column tables and strided vectors of concrete
  `NamedTuple` rows write through typed field cells; wide and reordered rows never build a generic
  record per row.
* **Message streams**: a `SingleObjectReader` looks up and verifies each schema fingerprint once, where
  `decodesingle` repeats that work per message.
* **Projection**: `select=` skips what you don't read, jumping sized array and map blocks by their
  recorded size.
* **Parallelism** wants files with multi-megabyte blocks (`block_bytes=` on the writer) so per-block
  overheads amortise; `Avro.Table(src; ntasks=…)` then decodes blocks straight into preallocated
  columns.
* **Column access beats row access** for bulk work: `Avro.Table` decodes straight into typed columns.
* Typed decoding into concrete structs avoids the generic model's boxing for isbits fields.
* **`juliac --trim=safe`** builds are not supported: readers and writers dispatch on schema-driven plans
  at run time, which a trimmed executable cannot resolve.
