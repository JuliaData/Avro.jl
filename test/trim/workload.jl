# The read and write calls a trimmed program makes with default options: `juliac --trim=safe` must compile
# this program with no verifier errors, and the program must print "ok".
using Avro, Dates

struct Reading
    station::String
    time::Int64
    temp::Union{Missing,Float64}
end

const Row = @NamedTuple{id::Int64, name::String, score::Union{Missing,Float64}, tags::Vector{String}}

struct Event
    id::Int64
    at::DateTime
end

struct Tree                                     # refers to itself through a union and through a vector
    v::Int64
    next::Union{Missing,Tree}
    kids::Vector{Tree}
end

const Wide = NamedTuple{Tuple(Symbol(:c, i) for i in 1:40),NTuple{40,Int64}}
const WideColumns = NamedTuple{fieldnames(Wide),NTuple{40,Vector{Int64}}}

# The encoding of the `Tree` value `main` builds. Julia 1.14.0-alpha2's `--trim` leaves a call in the write-node
# builder of a type that refers to itself unresolved, so there the program reads these bytes without writing them.
const TREE_BYTES = UInt8[0x02, 0x02, 0x04, 0x00, 0x00, 0x02, 0x06, 0x00, 0x00, 0x00]

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
        events = [Event(Int64(i), DateTime(2026, 1, 1) + Second(i)) for i in 1:10]
        file = joinpath(dir, "events.avro")
        Avro.write(file, events)
        sum(e -> e.id, Avro.Rows(file, Event)) == 55 || return Cint(7)
        ts = Avro.schema(Tree)
        @static if VERSION < v"1.14.0-"
            tree = Tree(1, Tree(2, missing, Tree[]), [Tree(3, missing, Tree[])])
            Avro.encode(ts, tree) == TREE_BYTES || return Cint(8)
        end
        back = Avro.decode(ts, TREE_BYTES, Tree)
        (back.v == 1 && (back.next::Tree).v == 2 && back.kids[1].v == 3) || return Cint(8)
        wide = [Wide(ntuple(j -> Int64(i * j), Val(40))) for i in 1:5]
        file = joinpath(dir, "wide.avro")
        Avro.write(file, wide)
        sum(r -> r.c40, Avro.Rows(file, Wide)) == 600 || return Cint(9)
        Avro.write(file, WideColumns(ntuple(j -> Int64[i * j for i in 1:5], Val(40))))
        sum(r -> r.c40, Avro.Rows(file, Wide)) == 600 || return Cint(10)
        println(Core.stdout, "ok")
        return Cint(0)
    finally
        rm(dir; recursive=true)
    end
end
