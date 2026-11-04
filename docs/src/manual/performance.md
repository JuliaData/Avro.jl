# Performance

[Benchmarks](../benchmarks.md) lists the performance gates and how to run them.

## Getting the numbers

* **Prepared beats one-shot**: `DatumReader`/`DatumWriter` skip per-call plan construction. For repeated
  writes, reuse a typed `DatumWriter(schema, T)` and an output `IO`; calling `writer(row)` returns a new
  owned byte vector each time, `writer(io, row)` appends without allocating one.
* **Writers encode through the datum's type**: NamedTuples, structs, vectors, dictionaries and nullable
  values of the types Avro encodes directly compile their encoding once per Julia type, in any field order,
  whatever the schema. Column tables of up to 32 columns write as `NamedTuple` rows of their element types;
  wider ones compile once per column type. `Any` and other abstract types, `Avro.Record` and other generic
  values, and unions of several types encode by each value's run-time type.
* **Message streams**: a `SingleObjectReader` looks up and verifies each schema fingerprint once, where
  `decodesingle` repeats that work per message.
* **Projection**: `select=` skips what you don't read, jumping sized array and map blocks by their
  recorded size.
* **Parallelism** wants files with multi-megabyte blocks (`block_bytes=` on the writer) so per-block
  overheads amortise; `Avro.Table(src; ntasks=…)` then decodes blocks straight into preallocated
  columns.
* **Column access beats row access** for bulk work: `Avro.Table` decodes straight into typed columns.
* Typed decoding into concrete structs avoids the generic model's boxing for isbits fields.
* **Typed readers and writers compile once per Julia type**, not per schema, so reading files of many
  schemas into one type, or writing one type under many schemas, reuses the same code.

## Trimmed programs

Programs compiled with `juliac --trim=safe` read and write Avro through the types they name:

* **Reading**: `Avro.Rows(src, T)`, `Avro.DatumReader(schema, T)`, `Avro.decode(schema, bytes, T)` and
  `Avro.SingleObjectReader(store, T)`, where `T` is a `NamedTuple` or a plain struct whose fields Avro
  decodes directly (see [Typed decoding](encoding.md#Typed-decoding)).
* **Writing**: `Avro.write(dst, rows)` for vectors of `NamedTuple`s or structs and for `NamedTuple`s of
  vectors, `Avro.Writer`, `Avro.DatumWriter(schema, T)` and `Avro.encode(schema, x)`.
* **Sources and codecs**: paths (memory-mapped or `mmap=false`), byte vectors, `IOBuffer`s and streams; the
  null, deflate, snappy and zstandard codecs, compressed on several tasks when the program runs threads.

Keyword arguments need concrete types, so pass a parsed schema with its type asserted:
`schema=Avro.parseschema(json)::Avro.RecordSchema`.

The rest needs the full Julia runtime: the generic value model (`Avro.Table(src)`, `Avro.Rows(src)` without
a type, `Avro.Record` or `Any` fields), schema resolution against a reader schema, conversions through
StructUtils hooks, values encoded by their run-time type, and the bzip2 and xz codecs. The verifier rejects
a program that calls the first two; a typed read or write that reaches a conversion or a run-time-typed
value raises an `ArgumentError` saying so. A program that must read files of varying shape reads them into a
type it declares.

On Julia 1.13 and earlier, a bug in how `--trim` prunes the image (JuliaLang/julia#63664) can crash a
trimmed program before `main` runs, inside its first dynamic call. Whether it does depends on everything the
program compiles, not on Avro; Julia 1.14 fixes it. Avro's CI builds and runs a trimmed program on both.
