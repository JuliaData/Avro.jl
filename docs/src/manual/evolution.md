# Schema evolution

Avro resolves data written under one schema against a reader's schema. Use `reader_schema=` on
`Avro.decode`, `Avro.DatumReader`, `Avro.decodesingle`, `Avro.Table`, and `Avro.Rows`:

```julia
reader = Avro.parseschema("""{"type":"record","name":"Person","fields":[
  {"name":"id","type":"double"},
  {"name":"nick","type":"string","aliases":["name"]},
  {"name":"active","type":"boolean","default":true}]}""")

t = Avro.Table("people.avro"; reader_schema=reader)
```

[`Avro.resolve`](@ref)`(writer, reader)` computes the resolution once and reports it as an
[`Avro.ResolvedSchema`](@ref); unresolvable schemas raise `Avro.ResolutionError`.

The full "Schema Resolution" rules are implemented:

* records match by fullname, then reader aliases; fields by name, then field aliases; missing reader
  fields take their defaults; writer-only fields are skipped.
* promotions: `int → long → float → double`, `string ↔ bytes`.
* enums remap by symbol; unknown writer symbols fall to the reader's `default` symbol.
* unions resolve per branch. `union_resolution=:spec` (default) takes the reader branch of the
  writer's own type (the same primitive, or a named type with the writer's fullname or a reader alias of
  it), else the only branch sharing a named writer type's unqualified name, else the first branch the
  writer promotes to: `long` data read with `["double","long"]` stays `long`. When several branches share
  the unqualified name and none has the fullname, resolution raises `Avro.ResolutionError` instead of
  guessing. `:java` reproduces Java's choice, which also matches records structurally.
* the decoded representation follows the **reader** schema (a resolved value looks as if it had been
  written under the reader schema).

A writer union branch that no reader branch accepts raises `Avro.ResolutionError` when a datum uses
it, not when the schemas are resolved, matching Java.

Resolution work is bounded by `max_resolution_work` (see [Limits](limits-and-security.md)); results
are memoised per `(writer, reader)` pair within an operation.
