# Kafka-style single-object messaging: each message frames one datum with its schema fingerprint;
# consumers resolve fingerprints through a schema store and keep a prepared decoder per schema.

using Avro

schema_v1 = Avro.parseschema("""
{"type":"record","name":"Click","fields":[
  {"name":"user","type":"long"},
  {"name":"page","type":"string"}]}
""")

# Producer: a prepared writer encodes datums as self-describing single-object messages.
produce = Avro.SingleObjectWriter(schema_v1)
messages = [produce((user=1, page="/home")), produce((user=2, page="/docs"))]

# Consumer: a bounded store maps fingerprints to schemas; unknown fingerprints raise
# Avro.UnknownSchemaError, ambiguous re-registrations Avro.AmbiguousSchemaError.
store = Avro.SchemaCache()
Avro.register!(store, schema_v1)

consume = Avro.SingleObjectReader(store)
for msg in messages
    click = consume(msg)
    println(click.user, " visited ", click.page)
end
