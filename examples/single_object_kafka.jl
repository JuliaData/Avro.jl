# Kafka-style single-object messaging: each message frames one datum with its schema fingerprint;
# consumers resolve fingerprints through a schema store and decode via a cache of prepared readers.

using Avro

schema_v1 = Avro.parseschema("""
{"type":"record","name":"Click","fields":[
  {"name":"user","type":"long"},
  {"name":"page","type":"string"}]}
""")

# Producer: encode datums as self-describing single-object messages.
function produce(click)
    return Avro.encodesingle(schema_v1, click)
end

messages = [produce((user=1, page="/home")), produce((user=2, page="/docs"))]

# Consumer: a bounded store maps fingerprints to schemas; unknown fingerprints raise
# Avro.UnknownSchemaError, ambiguous re-registrations Avro.AmbiguousSchemaError.
store = Avro.SchemaCache()
Avro.register!(store, schema_v1)

for msg in messages
    click = Avro.decodesingle(msg, store)
    println(click.user, " visited ", click.page)
end
