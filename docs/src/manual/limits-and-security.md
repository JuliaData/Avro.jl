# Limits and security

Avro.jl is designed to read **untrusted files safely by default**. Every operation runs under an
[`Avro.Limits`](@ref) value with fixed, portable defaults; exceeding a limit raises
`Avro.LimitError` naming the limit, the observed value, and the keyword to raise (a codec frame that
needs more decoder memory than `max_codec_memory` raises `Avro.CodecError` naming that limit). Limits
can reject valid data by design — every one is raisable for trusted bulk workloads.

## How memory stays proportional to the input

Every claimed size is checked before anything is allocated for it:

* a string, bytes or fixed length against `max_bytes` and the bytes that remain;
* an array or map block count against `max_block_count` and the remaining bytes divided by the item's
  minimum encoded size;
* a container block against `max_block_bytes`, compressed and decompressed: decompression stops as soon
  as the output passes the cap, so a small block that expands to gigabytes fails after 16 MiB;
* values against the work rule `values ≤ max_values_per_byte × input_bytes + work_allowance`, per datum,
  per block and per operation, which bounds items whose encoding takes no bytes (a million-empty-string
  block is legal; a datum claiming billions of nulls in a few bytes is not).

`Avro.Table` over bytes or a mapped file preallocates its final columns from the blocks' declared row
counts only while those claims fit 256 MiB or 256 times the file's block bytes; larger claims are decoded
block by block and checked as they arrive.

## The ceiling

One per-operation memory ceiling — `max_total_bytes`, default **16 GiB** — bounds package-owned
memory through reservations made before every allocation (buffers, decoded values, column storage,
codec workspaces, scratch). It is a backstop for materialising reads: `Avro.Table` must fit the whole
result, while streaming consumers (`Rows`, `eachdatum`, `eachblock`, `mmap=false`) hold one block at a
time. Acceptance depends only on the input bytes and the options: the ceiling does not shrink with the
host's free memory or with other operations in the process. Lower it for services that read untrusted
files on small machines.

Name registries, alias sets and key indexes are sorted runs or sort-once vectors (no hashing of
untrusted names), so their work grows as `n log n` in the input.

## Selected limits

`max_depth` (1024), `max_bytes`/`max_datum_bytes` (64 MiB), `max_block_bytes` (16 MiB),
`max_block_output_bytes` (64 MiB), `max_codec_memory` (32 MiB — a zstandard frame demanding a larger
window fails fast), `max_schema_bytes`/`max_schema_nodes`/`max_schema_depth`,
`max_metadata_bytes`/`max_metadata_entries`, `max_resolution_work`, `max_inflight_blocks`;
`max_total_values`/`max_rows`/`max_blocks` are unlimited by default and cap an operation's cumulative
totals when set. Construction validates cross-limit consistency.

## The writer/reader invariant

With identical `Limits` on both sides, **everything a successfully closed `Writer` emits is accepted
by `Reader` and `Rows`** for every root schema and source mode (bytes, a mapped path, `mmap=false`,
non-seekable `IO`): the writer enforces every per-schema, per-block and per-datum limit a reader enforces,
including each block's decoded output. `Avro.Table` accepts it too when the materialised table fits
`max_total_bytes`.

## Symbol admission

Column names and typed `Symbol` values are interned directly by default, so acceptance never depends
on what the process read before; a file's names are bounded by `max_schema_bytes` and friends.
`Symbol`s are never freed, so a long-running service reading untrusted files can pass a caller-owned
[`Avro.SymbolAdmission`](@ref) as `names=` to cap the strings interned through it (that table is
shared state, so a read fails once it is full). Generic iteration interns nothing.

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

`DatumReader`, schemas, plans and `Limits` are safe to share across tasks. `DatumWriter` retains
reusable mutable state and is single-owner. `Reader`/`Rows`/`Writer` are also single-operation objects.
Parallel decoding uses only headroom under the ceiling and never changes results or acceptance.
Truncating a memory-mapped file during a read is undefined at the OS level — use `mmap=false` for files
that may change.
