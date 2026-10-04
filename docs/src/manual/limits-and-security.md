# Limits and security

Avro.jl is designed to read **untrusted files safely by default**. Every operation runs under an
[`Avro.Limits`](@ref) value with fixed, portable defaults; exceeding any limit raises
`Avro.LimitError` naming the limit, the observed value, and the keyword to raise. Limits can reject
valid data by design — every one is raisable for trusted bulk workloads.

## The ceiling

One per-operation memory ceiling — `max_total_bytes`, default **256 MiB** — bounds package-owned
memory through reservations made before every allocation (buffers, decoded values, column storage,
codec workspaces, scratch). The effective ceiling is `min(max_total_bytes, available ÷ 2)` with a
best-effort available-memory guard. Streaming consumers (`Rows`, `eachdatum`, `eachblock`,
`mmap=false`) hold one block at a time and read files far larger than the ceiling; `Avro.Table`
materialises and therefore requires the whole result to fit.

## The work rule

Decompression bombs are bounded by work rules, not guesses: `values ≤ max_values_per_byte ×
input_bytes + work_allowance` is enforced per datum, per block and per operation (a million-empty-string
block is legal; a datum claiming billions of values in a few bytes is not), and comparisons are
bounded the same way. The constants are calibrated so the worst-density legal input at default limits
decodes in ≤ 10 s single-threaded on supported Julia versions.

## Selected limits

`max_depth` (1024), `max_bytes`/`max_datum_bytes` (64 MiB), `max_block_bytes` (16 MiB),
`max_block_output_bytes` (64 MiB), `max_codec_memory` (32 MiB — a zstandard frame demanding a larger
window fails fast), `max_schema_bytes`/`max_schema_nodes`/`max_schema_depth`,
`max_metadata_bytes`/`max_metadata_entries`, `max_total_values`/`max_rows`/`max_blocks`,
`max_resolution_work`, `max_inflight_blocks`. Construction validates cross-limit consistency.

## The writer/reader invariant

With identical `Limits` on both sides, **everything a successfully closed `Writer` emits is accepted**
by the guaranteed consumers (`Reader`/`Rows` for every root schema, `Table` for record roots, from
bytes, a mapped path, `mmap=false`, and non-seekable `IO`): the writer enforces every reader limit and
preflights the reader's complete memory peak — including the streamed consumer's chunk materialisation
— refusing at write time (with `LimitError`) files a consumer could not decode.

## Symbol admission

`Symbol`s intern permanently, so untrusted strings pass a bounded admission table before interning
([`Avro.SymbolAdmission`](@ref); the process default `Avro.DEFAULT_ADMISSION` admits 1,000,000 names /
64 MiB counting its fixed buffers, retained strings and run slots, run shells, and live merge state).
Generic iteration interns nothing; `names=:trusted` bypasses admission.

## The error guarantee

Malformed data raises `Avro.DataError`, schema problems `Avro.SchemaError`, resource exhaustion
`Avro.LimitError`, codec problems `Avro.CodecError`/`Avro.UnsupportedCodecError`, conversion problems
`Avro.ConversionError`/`Avro.EncodeError` — never a crash, hang, or unbounded allocation. Errors carry
positions. The fuzz harness (differential against the Java and Python implementations) enforces this.

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

`DatumReader`, schemas, plans and `Limits` are safe to share across tasks. `DatumWriter` retains
reusable mutable state and is single-owner. `Reader`/`Rows`/`Writer` are also single-operation objects.
Parallel decoding uses only headroom under the ceiling and never changes results or acceptance.
Truncating a memory-mapped file during a read is undefined at the OS level — use `mmap=false` for files
that may change.
