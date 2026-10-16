# Schemas

## Parsing

[`Avro.parseschema`](@ref) parses schema JSON from a string, byte vector or `IO`, validating every
rule of the specification (names, defaults, unions, logical-type attributes) and raising
`Avro.SchemaError` with a JSON path, or `Avro.LimitError` when a schema exceeds the configured limits:

```julia
s = Avro.parseschema("""
{"type": "record", "name": "Person", "namespace": "example",
 "fields": [
   {"name": "id", "type": "long"},
   {"name": "email", "type": ["null", "string"], "default": null}
 ]}
""")
```

Schema nodes are immutable; records, enums and fixed carry their full names
([`Avro.fullname`](@ref)) and compare by structure. [`Avro.json`](@ref) prints a schema back to JSON.

Two repair options admit historically produced schemas: `allow_invalid_names=true` (invalid names are
kept and the graph is marked repaired) and `allow_invalid_defaults=true`. Operations that depend on
canonical form reject repaired schemas, and a `Writer` requires the matching option to write one.

## Constructing schemas programmatically

The schema node types (`Avro.RecordSchema`, `Avro.EnumSchema`, `Avro.FixedSchema`,
`Avro.ArraySchema`, `Avro.MapSchema`, `Avro.UnionSchema`, and the primitives) have public
constructors; recursive records use a builder form. See the [Reference](../reference.md) for the
constructor signatures.

## Deriving schemas from Julia types and values

[`Avro.schema`](@ref) derives a schema from a Julia type or value — `NamedTuple`s and plain structs
become records, `Union{Missing,T}` becomes `["null", T]`, `Vector`/`AbstractDict` become
arrays/maps, `Tables.Schema` becomes a record:

```julia
Avro.schema(@NamedTuple{id::Int64, email::Union{Missing,String}})
Avro.schema((id=1, email=missing))
```

Field tags from StructUtils customise names and defaults; `Avro.avroname` and `Avro.avrosymbol`
control the derived record and enum names. Names derived from Julia identifiers must be valid Avro
names — invalid ones are errors (not silently rewritten).

## Canonical form and fingerprints

[`Avro.canonical`](@ref) produces the Parsing Canonical Form; [`Avro.fingerprint`](@ref) computes
CRC-64-AVRO (the spec's default), MD5 or SHA-256 fingerprints; [`Avro.parsingequivalent`](@ref)
compares two schemas by canonical form. These power the [single-object encoding](singleobject.md).
