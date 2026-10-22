# Single-object encoding

The single-object encoding frames one datum with the marker `0xC3 0x01` and the writer schema's
CRC-64-AVRO fingerprint — the format used by message brokers:

```julia
msg = Avro.encodesingle(s, (id=1, name="alice"))

store = Avro.SchemaCache()          # fingerprint → schema (bounded, lock-protected)
Avro.register!(store, s)
value = Avro.decodesingle(msg, store)
```

[`Avro.SchemaCache`](@ref) is the built-in store: bounded, indexed by a sorted fingerprint vector, and
idempotent for structurally equal schemas — the shape of a Kafka-style consumer:

```julia
for msg in messages
    value = Avro.decodesingle(msg, store)     # reader_schema= and T= resolve/type per call site
end
```

[`Avro.SchemaStore`](@ref) is the interface for custom registries: implement
[`Avro.lookup`](@ref)`(store, fingerprint)`, `Avro.lookup(store, fingerprint, budget)`, and
[`Avro.register!`](@ref). The three-argument lookup must charge the supplied operation budget.

Unknown fingerprints raise [`Avro.UnknownSchemaError`](@ref); a registry may map one fingerprint to
several parsing-equivalent schemas, and ambiguity raises [`Avro.AmbiguousSchemaError`](@ref).
