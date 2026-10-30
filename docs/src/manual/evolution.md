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

* records match by fullname, reader aliases, or unqualified name; fields by name and field aliases; missing reader
  fields take their defaults; writer-only fields are skipped.
* promotions: `int → long → float → double`, `string ↔ bytes`.
* enums remap by symbol; unknown writer symbols fall to the reader's `default` symbol.
* unions resolve per branch. `union_resolution=:spec` (default) chooses the **first matching reader
  branch**, including promotions, in schema order. Thus `long` data read with `["double","long"]`
  becomes `Float64`, which can lose integer precision. Named types match by fullname, reader alias,
  or unqualified name; the first match wins even if a later branch has the exact fullname.
  `union_resolution=:java` reproduces Java's choice: it prefers the writer's exact primitive type or
  fullname (including aliases), then considers structural record matches and promotions. Use this
  policy when interoperating with Java's union choices or to prefer an exact integer branch.
* the decoded representation follows the **reader** schema (a resolved value looks as if it had been
  written under the reader schema).

A writer union branch that no reader branch accepts raises `Avro.ResolutionError` when a datum uses
it, not when the schemas are resolved, matching Java.

Resolution work is bounded by `max_resolution_work` (see [Limits](limits-and-security.md)); results
are memoised per `(writer, reader)` pair within an operation.
