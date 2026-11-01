# Limits and security

Avro.jl reads untrusted files safely by default: malformed or hostile input fails with an
`Avro.AvroError` instead of exhausting memory, and the memory an operation uses stays proportional to
the input it reads. Four limits, all raisable, bound that proportion:

```julia
Avro.Limits(; max_depth=1024, max_bytes=256 * 2^20, max_values_per_byte=16, value_allowance=65_536)
```

* `max_depth` bounds the nesting of values, of JSON documents and of schemas, following named references.
* `max_bytes` bounds the largest buffer one operation materialises from its input: a decompressed
  container block, a schema or metadata document, the names a schema's namespaces expand to, or one datum
  read from a stream. A small block that decompresses to gigabytes fails as soon as the output passes the cap.
* `max_values_per_byte` and `value_allowance` bound how many values one container block or one
  standalone datum may decode: at most `max_values_per_byte × input_bytes + value_allowance`. Values that
  take no input bytes (nulls, empty records) are legal in Avro; this rule keeps a few bytes from standing for
  billions of them. Defaults from a reader schema are the caller's values, so they do not count.

Exceeding a limit raises [`Avro.LimitError`](@ref), which names the limit, the observed value and the
keyword to raise. Pass `limits=` to the readers (`Avro.Table`, `Avro.Rows`, `Avro.Reader`,
`Avro.decode`, `Avro.DatumReader`, `Avro.fromjson`, `Avro.decodesingle`, `Avro.SingleObjectReader`,
`Avro.comparebytes`, `Avro.inspect`, `Avro.parseschema`) and to the writers (`Avro.write`, `Avro.Writer`),
which enforce the same bounds so that everything a successfully closed writer emits is readable under the
same limits, including that the schema's JSON fits them. The datum encoders (`Avro.encode`, `Avro.encode!`,
`Avro.DatumWriter`, `Avro.tojson`, `Avro.encodesingle`, `Avro.SingleObjectWriter`, `Avro.compare`) take `limits=`
for the nesting depth of the values they encode.

## What is always checked

Independently of the limits, every claimed length is checked against the bytes that remain before
anything is allocated: a string, bytes or fixed value longer than the rest of its buffer, an array or
map block that declares more items than the remaining bytes can hold, or a block count or size that is
negative, is an `Avro.DataError` carrying the byte position. Booleans, UTF-8 strings, enum and union
indices, time-of-day values, UUID text and decimal precision are validated as they are decoded.

Schema documents are validated against every rule of the specification; `allow_invalid_names=true`
and `allow_invalid_defaults=true` admit historically produced schemas and mark the graph as repaired.

## Memory

`Avro.Table` holds its whole result, so a large file needs the memory for its columns; `Avro.Rows`,
`Avro.eachdatum` and `Avro.eachblock` hold one decompressed block at a time. `Avro.Table` decodes blocks
in parallel into preallocated columns when the file's block headers claim no more storage than
`max(256 MiB, 256 × the file's size)`; larger claims are decoded block by block and checked as they
arrive, so a tiny file cannot make the reader allocate gigabytes before its first block is verified.

Decoded values take more memory than their encoded bytes, a compressed block can expand up to `max_bytes`, and
the value limits apply per block. So `Avro.Table` output has no ceiling of its own: a kilobyte of well-formed
compressed input can decode to hundreds of megabytes. For input from an untrusted source, stream with `Avro.Rows`,
which holds one block at a time, and lower `max_bytes` and `max_values_per_byte`.

Column names and typed `Symbol` values are interned, and Julia never frees a `Symbol`. A file's names are bounded
by its schema and so by `max_bytes`, but a long-running service that reads schemas from untrusted sources
accumulates them, so give it a low `max_bytes`.

## The error guarantee

Malformed data raises `Avro.DataError`, schema problems `Avro.SchemaError`, resource exhaustion
`Avro.LimitError`, codec problems `Avro.CodecError`/`Avro.UnsupportedCodecError`, conversion problems
`Avro.ConversionError`/`Avro.EncodeError` — never a crash, hang, or unbounded allocation. A
`DataError` carries the byte position; `SchemaError` and `EncodeError` carry a JSON path. A seeded
mutation fuzzer in the test suite checks this.

## Recorded deviations and oracle readability

Every file Avro.jl writes from a schema the Java (1.12.2) and fastavro (1.12.2) oracles accept is
readable by both. Recorded exceptions where Avro.jl follows the specification and an oracle does not:

* unions with colliding JSON branch labels — Java rejects the schema;
* files carrying invalid-but-repaired legacy schemas — readable only with the repair options;
* user metadata values that are not valid UTF-8 — the spec's header is `map<string,bytes>` and Java
  accepts them; fastavro 1.12.2 raises `UnicodeDecodeError`.

Deviations in favour of the spec: decimal precision is validated on decode (Java validates only on
encode); time values are never silently truncated on encode (Java truncates).

## Concurrency and ownership

`DatumReader`, `SingleObjectReader`, schemas, plans and `Limits` are safe to share across tasks. `DatumWriter` retains reusable mutable state and is single-owner, as is `SingleObjectWriter`. `Reader`/`Rows`/`Writer` are also single-operation objects.
Parallel decoding never changes results or acceptance: a failing file fails with the same error at the
same block at every `ntasks`. Truncating a memory-mapped file during a read is undefined at the OS
level — use `mmap=false` for files that may change.
