# Tables.jl integration

## `Avro.Table`

[`Avro.Table`](@ref) materialises a record-root container as a column table: plain `Vector` columns,
`Tables.columnnames`/`Tables.getcolumn`, `Tables.partitions` (one sub-table per block), `length`
(row count), and read-only DataAPI metadata (`DataAPI.metadata(t, "key")` for the file's metadata).
The stored `Tables.Schema` carries names and eltypes in fields, never in type parameters, so untrusted
files cannot force compilation.

```julia
t = Avro.Table("data.avro")
df = DataFrame(t)                              # or any Tables.jl sink
```

`Avro.schema(t)` is the effective (reader) schema, `Avro.writerschema(t)` the file's writer schema.

## `Avro.Rows`

[`Avro.Rows`](@ref) streams with bounded memory. Three modes:

* **Generic record mode** (record root): a Tables.jl row source yielding [`Avro.Row`](@ref)s.
  Column names intern lazily — iterating an untrusted file interns nothing until you ask for
  `Tables.schema`/`Tables.columnnames`.
* **Typed mode** (`T=`): a plain iterator of `T` values.
* **Non-record mode**: a plain iterator of the generic values.

`close(rows)` releases the source (do-block form available); `Tables.partitions(rows)` yields one
`Avro.Table` per block, so `Avro.write(dst, Avro.Rows(src))` re-encodes with bounded memory.

## Projection pushdown

`select=` on both readers skips unselected fields (strict mode walks them, fast mode jumps sized
blocks) and derives an effective schema of exactly the selected fields, which `Avro.write`
round-trips:

```julia
t = Avro.Table("data.avro"; select=(:id, :name), validate=:fast)
```

`select=()` yields a zero-column table with the authoritative row count.

## Parallel decoding

`Avro.Table(src; ntasks=n)` decodes blocks concurrently for byte and memory-mapped sources.
The design guarantees, at every `ntasks`:

* **identical results, acceptance and errors** — the lowest uncommitted block always decodes under
  the sequential rules, and a failing file fails with the same error at the same block;
* **bounded memory** — higher blocks are admitted strictly in order, each under a complete worst-case
  reservation, and only into headroom under the ceiling. With default limits there is essentially no
  headroom (that is by design); parallel speedups need raised limits:

```julia
lim = Avro.Limits(max_total_bytes=4 << 30)
t = Avro.Table("big.avro"; ntasks=8, limits=lim)
```

## Symbol admission

`Symbol`s intern permanently in Julia, so every `Symbol`-producing path (column names, typed `Symbol`
values) passes an admission table with name and byte budgets. The process-wide default admits 1 M
names / 64 MiB; pass `names=Avro.SymbolAdmission(...)` for per-tenant budgets or `names=:trusted` to
bypass admission for trusted data. See [`Avro.SymbolAdmission`](@ref).
