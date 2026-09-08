# Migration from 1.x

Avro.jl 2.0 is a rewrite; this page maps every 1.x usage to its 2.0 form. `Avro.readtable` and
`Avro.writetable` keep working for one major cycle with deprecation warnings.

## Tables API

| 1.x | 2.0 |
|---|---|
| `Avro.writetable("f.avro", tbl; compress=:zstd)` | `Avro.write("f.avro", tbl; codec=:zstandard)` |
| `Avro.readtable("f.avro")` | `Avro.Table("f.avro")` (columns; add `legacy=:avrojl1` for 1.x-written files — the shim does) |
| — | `Avro.Rows("f.avro")` for streaming |

`readtable` returned lazily materialised records; `Avro.Table` returns plain columns. Wrap with
`Tables.rows`/`DataFrame` as needed.

## Datum API

| 1.x | 2.0 |
|---|---|
| `Avro.write(x)` / `Avro.write(io, x)` (datum) | `Avro.encode(x)` / `Avro.encode!(io, s, x)` |
| `Avro.read(buf, T)` | `Avro.DatumReader(Avro.schema(T), T)(buf)` or `Avro.decode(s, buf)` |
| `Avro.schematype(T)` | `Avro.schema(T)` |

`Avro.write(dst, x)` now always means the container writer for a Tables.jl source.

## Value model changes

* Enums decode as `Avro.EnumValue` (was `Avro.Enum{names}`), fixed as `Avro.Fixed`, nested records as
  `Avro.Record` (was `Avro.Record{names,T,N}`), non-nullable unions as `Avro.UnionValue`. Positions
  are 1-based; `Avro.ordinal` is the wire index.
* Timestamps decode as exact `Avro.Timestamp{P}`/`Avro.LocalTimestamp{P}` wrappers; `DateTime(x)` is
  an explicit, range-checked conversion (or decode typed with a `DateTime` field).
* `Avro.Decimal` is an alias for `DataDecimals.DecimalValue{Int128}`. It carries its scale at runtime and reads big-endian per the specification (1.x wrote
  native-endian; see below). `Avro.Duration` fields are `UInt32`.
* Customisation moved from StructTypes to StructUtils (field tags: `&(avro=(name="…", default=…),)`).

## Behaviour changes

* Container reading validates strictly by default (`validate=:fast` opts out of the walk).
* Tables sources without a `Tables.schema` need an explicit `schema=`.
* Julia-derived names that are invalid Avro names are errors (1.x silently emitted them).
* Every operation runs under fixed resource limits; see
  [Limits and security](manual/limits-and-security.md).

## Files written by Avro.jl ≤ 1.1.2

1.x wrote files with defects the strict reader rejects: null-codec blocks padded with garbage bytes,
the non-standard codec name `zstd`, unnamed fixed schemas, and `decimal` values written
**native-endian** as fixed-16. `Avro.inspect(file)` reports header and framing issues, including the
codec name. It walks datum structure and diagnoses null-codec padding, but it cannot infer decimal
byte order. Read
with:

```julia
t = Avro.Table("old.avro"; legacy=:avrojl1, decimal_byteorder=:little)  # byteorder only if decimals
```

and rewrite once to clean 2.0 output:

```julia
Avro.write("clean.avro", Avro.Rows("old.avro"; legacy=:avrojl1, decimal_byteorder=:little);
           codec=:zstandard)
```

## Shared values

The writer accepts `DataDecimals.AbstractDecimal` values with an explicit decimal
schema. Fixed-scale decimal types can also infer their precision and scale.
`Avro.WideDecimal` remains the arbitrary-precision fallback for large schemas.

The writer accepts `Durations.Duration` for the duration logical type when all
components are nonnegative, the time component is an exact number of milliseconds,
and all three wire fields fit UInt32. `Avro.Duration` preserves the full unsigned
wire range on reads. `Durations.Duration(x::Avro.Duration)` checks the narrower
signed month/day range.
