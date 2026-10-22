# CSV → Avro → Arrow: Avro as the schema-carrying interchange step of a pipeline.
# Dependencies: CSV, Arrow, DataFrames (any Tables.jl source and sink work the same way).

using Avro, CSV, Arrow, DataFrames

csvpath, avropath, arrowpath = "input.csv", "data.avro", "data.arrow"

# CSV.File is a Tables.jl source: the Avro schema derives from its column types.
Avro.write(avropath, CSV.File(csvpath); codec=:zstandard)

# The Avro file carries its schema; downstream consumers need nothing else.
t = Avro.Table(avropath)
Arrow.write(arrowpath, t)

# Round-trip check through DataFrames.
df = DataFrame(Avro.Table(avropath))
@assert df == DataFrame(Arrow.Table(arrowpath))
