# The read and write calls a trimmed program makes with default options: `juliac --trim=safe` must compile
# this program with no verifier errors, and the program must print "ok".
using Avro

struct Reading
    station::String
    time::Int64
    temp::Union{Missing,Float64}
end

const Row = @NamedTuple{id::Int64, name::String, score::Union{Missing,Float64}, tags::Vector{String}}

const SCHEMA_JSON = """{"type":"record","name":"Row","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},
    {"name":"score","type":["null","double"]},{"name":"tags","type":{"type":"array","items":"string"}}]}"""

samerow(a::Row, b::Row) = a.id == b.id && a.name == b.name && isequal(a.score, b.score) && a.tags == b.tags

function readsum(path::String)
    total = 0
    for r in Avro.Rows(path, Reading)
        total += r.time
    end
    return total
end

function (@main)(args::Vector{String})::Cint
    dir = mktempdir(; cleanup=false)
    try
        readings = [Reading("s$(i % 7)", Int64(i), isodd(i) ? missing : i / 4) for i in 1:1000]
        path = joinpath(dir, "readings.avro")
        Avro.write(path, readings)
        readsum(path) == 500500 || return Cint(1)
        for codec in (:deflate, :zstandard, :snappy)
            file = joinpath(dir, "readings-$codec.avro")
            Avro.write(file, readings; codec=codec)
            readsum(file) == 500500 || return Cint(2)
        end
        rows = Row[(id=Int64(i), name="n$i", score=isodd(i) ? missing : i / 2, tags=["a", "b"]) for i in 1:100]
        s = Avro.parseschema(SCHEMA_JSON)::Avro.RecordSchema       # keyword arguments need concrete types
        file = joinpath(dir, "rows.avro")
        Avro.write(file, rows; schema=s)
        n = 0
        for row in Avro.Rows(file, Row)
            n += row.id + length(row.tags)
        end
        n == 5250 || return Cint(3)
        columns = (id=collect(Int64, 1:10), name=["n$i" for i in 1:10])
        Avro.write(joinpath(dir, "columns.avro"), columns)
        bytes = Avro.DatumWriter(s, Row)(rows[2])
        samerow(Avro.DatumReader(s, Row)(bytes), rows[2]) || return Cint(4)
        samerow(Avro.decode(s, Avro.encode(s, rows[3]), Row), rows[3]) || return Cint(5)
        Avro.decode(Avro.schema(Reading), Avro.encode(readings[5]), Reading) === readings[5] || return Cint(6)
        println(Core.stdout, "ok")
        return Cint(0)
    finally
        rm(dir; recursive=true)
    end
end
