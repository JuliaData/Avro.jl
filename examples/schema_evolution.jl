# Schema evolution: reading old data under a new reader schema — renames via aliases, promotions,
# added fields with defaults, and dropped fields, all resolved per the specification.

using Avro

writer = Avro.parseschema("""
{"type":"record","name":"Person","fields":[
  {"name":"id","type":"int"},
  {"name":"name","type":"string"},
  {"name":"email","type":"string"}]}
""")

io = IOBuffer()
Avro.write(io, [(id=Int32(1), name="alice", email="a@x.io")]; schema=writer)
seekstart(io)

# The new schema: `id` promoted int → long, `name` renamed (alias), `email` dropped,
# `active` added with a default.
reader = Avro.parseschema("""
{"type":"record","name":"Person","fields":[
  {"name":"id","type":"long"},
  {"name":"handle","type":"string","aliases":["name"]},
  {"name":"active","type":"boolean","default":true}]}
""")

for row in Avro.Rows(io; reader_schema=reader)
    @assert row.id === Int64(1) && row.handle == "alice" && row.active === true
end

# Avro.resolve reports how (and whether) two schemas resolve, without decoding anything:
Avro.resolve(writer, reader)
