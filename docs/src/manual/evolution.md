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
* unions resolve per branch. `union_resolution=:spec` (default) follows the specification's
  first-match rule; `:java` reproduces Java's exact-match-first behaviour where they differ.
* the decoded representation follows the **reader** schema (a resolved value looks as if it had been
  written under the reader schema).

Branch selection failures inside data (a writer branch no reader branch accepts) surface as
`Avro.DataError` at the offending datum, not at resolution time, matching Java.

Resolution work is bounded by `max_resolution_work` (see [Limits](limits-and-security.md)); results
are memoised per `(writer, reader)` pair within an operation.
