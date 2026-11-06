# CSV → Avro → Arrow: Avro as the schema-carrying interchange step of a pipeline.
# Dependencies: CSV, Arrow, DataFrames (any Tables.jl source and sink work the same way).

using Avro, CSV, Arrow, DataFrames

dir = mktempdir()
csvpath, avropath, arrowpath = joinpath(dir, "input.csv"), joinpath(dir, "data.avro"), joinpath(dir, "data.arrow")
write(csvpath, "id,name,score\n1,alice,9.5\n2,bob,7.0\n")

# CSV.File is a Tables.jl source: the Avro schema derives from its column types.
Avro.write(avropath, CSV.File(csvpath); codec=:zstandard)

# The Avro file carries its schema; downstream consumers need nothing else.
Arrow.write(arrowpath, Avro.Table(avropath))

# Round-trip check through DataFrames.
df = DataFrame(Avro.Table(avropath))
@assert df == DataFrame(Arrow.Table(arrowpath))
println(df)
