struct WideOffsetRows{T} <: AbstractVector{T}
    data::Vector{T}
end
Base.size(rows::WideOffsetRows) = size(rows.data)
Base.axes(rows::WideOffsetRows) = (0:length(rows.data)-1,)
Base.getindex(rows::WideOffsetRows, i::Int) = rows.data[i + 1]

struct CountingWideRows{T} <: AbstractVector{T}
    data::Vector{T}
    reads::Base.RefValue{Int}
end
Base.size(rows::CountingWideRows) = size(rows.data)
function Base.getindex(rows::CountingWideRows, i::Int)
    rows.reads[] += 1
    return rows.data[i]
end

StructUtils.@tags struct WideTaggedValue
    value::Int64 &(name="renamed",)
end

@testset "Wide row tables share the field-cell writer" begin
    function rowwise(schema, rows; kwargs...)
        io = IOBuffer()
        Avro.Writer(io, schema; sync=zeros(UInt8, 16), block_bytes=1024, kwargs...) do writer
            for row in rows
                push!(writer, row)
            end
        end
        return take!(io)
    end
    failure(f) = try
        f()
        nothing
    catch err
        err
    end
    names = Tuple(Symbol("f", i) for i in 1:40)
    columns = NamedTuple{names}(ntuple(40) do k
        k % 4 == 0 ? Union{Missing,Int64}[isodd(i) ? missing : Int64(i) for i in 1:100] :
        k % 4 == 1 ? ["row$i" for i in 1:100] :
        k % 4 == 2 ? [i / k for i in 1:100] : [isodd(i) for i in 1:100]
    end)
    rows = Tables.rowtable(columns)
    schema = Avro.schema(Tables.schema(columns))
    reversed = Avro.RecordSchema("ReversedRows"; fields=reverse(collect(schema.fields)))
    for s in (schema, reversed)
        bytes = take!(Avro.tobuffer(rows; schema=s, sync=zeros(UInt8, 16), block_bytes=1024))
        @test bytes == rowwise(s, rows)
        @test take!(Avro.tobuffer(WideOffsetRows(rows); schema=s, sync=zeros(UInt8, 16),
                                 block_bytes=1024)) == bytes
        back = Tables.columntable(Avro.Table(bytes))
        @test all(isequal(back[k], columns[k]) for k in keys(columns))
    end
    @test length(Avro.Table(Avro.tobuffer(rows[1:0]; schema=schema))) == 0
    counted = CountingWideRows(rows, Ref(0))
    @test take!(Avro.tobuffer(counted; schema=schema, sync=zeros(UInt8, 16), block_bytes=1024)) ==
          rowwise(schema, rows)
    @test counted.reads[] == length(rows)
    selected = @view rows[2:2:10]
    @test take!(Avro.tobuffer(selected; schema=schema, sync=zeros(UInt8, 16), block_bytes=1024)) ==
          rowwise(schema, selected)

    narrow = [(b="x", a=Int64(1)), (b="y", a=Int64(2))]
    ns = Avro.parseschema("""{"type":"record","name":"NarrowRows","fields":[
        {"name":"a","type":"long"},{"name":"b","type":"string"}]}""")
    @test take!(Avro.tobuffer(narrow; schema=ns, sync=zeros(UInt8, 16), block_bytes=1024)) ==
          rowwise(ns, narrow)

    bad = [merge(row, (f2="invalid",)) for row in rows]
    rowerror = failure(() -> rowwise(schema, bad))
    cellerror = failure(() -> Avro.tobuffer(bad; schema=schema))
    @test rowerror isa Avro.EncodeError && cellerror isa Avro.EncodeError
    @test rowerror.msg == cellerror.msg && rowerror.path == cellerror.path == "\$.f2"
    @test rowerror.schema === cellerror.schema === schema.fields[2].schema
    missingfield = Avro.RecordSchema("MissingField";
        fields=vcat(collect(schema.fields), [Avro.Field("absent", Avro.IntSchema())]))
    @test_throws Avro.EncodeError Avro.tobuffer(rows; schema=missingfield)

    tagged = [merge(row, (f1=WideTaggedValue(Int64(i)),)) for (i, row) in enumerate(rows)]
    tagged_schema = Avro.schema(Tables.schema(tagged))
    @test take!(Avro.tobuffer(tagged; schema=tagged_schema, sync=zeros(UInt8, 16),
                             block_bytes=1024)) == rowwise(tagged_schema, tagged)
    @test Avro.Table(Avro.tobuffer(tagged)).f1[1].renamed == 1

    array_schema = Avro.RecordSchema("StagedWideRows";
        fields=vcat([Avro.Field("f1", Avro.ArraySchema(Avro.LongSchema()))],
                    collect(schema.fields)[2:end]))
    staged = [merge(row, (f1=Iterators.Stateful(Int64[1, 2]),)) for row in rows]
    @test all(x == [1, 2] for x in Avro.Table(Avro.tobuffer(staged; schema=array_schema)).f1)

    for limits in (Avro.Limits(max_rows=5), Avro.Limits(max_depth=0),
                   Avro.Limits(max_total_values=20), Avro.Limits(max_block_output_bytes=128))
        a = failure(() -> rowwise(schema, rows; limits=limits))
        b = failure(() -> Avro.tobuffer(rows; schema=schema, limits=limits))
        @test a isa Avro.LimitError && b isa Avro.LimitError && a.limit == b.limit
    end
    writer = Avro.Writer(IOBuffer(), schema)
    budget = writer.budget
    # Leave space for field positions but refuse the cells, without allocating a large input.
    filler = budget.limits.max_total_bytes - budget.reserved - 1024
    Avro.reserve!(budget, filler)
    before = budget.reserved
    @test_throws Avro.LimitError Avro.pushrowcells!(writer, rows)
    @test budget.reserved == before
    Avro.release!(budget, filler)
    close(writer; abort=true)

    function floats(width, count)
        keys = Tuple(Symbol("c", i) for i in 1:width)
        return NamedTuple{keys}(ntuple(i -> fill(Float64(i), count), width))
    end
    for width in (32, 50)
        small, large = floats(width, 1000), floats(width, 2000)
        for (a, b) in ((small, large), (Tables.rowtable(small), Tables.rowtable(large)))
            Avro.tobuffer(a)
            Avro.tobuffer(b)
            @test @allocations(Avro.tobuffer(b)) - @allocations(Avro.tobuffer(a)) < 1000
        end
    end
    mixedsmall = repeat(rows, 4)
    mixedlarge = repeat(rows, 8)
    Avro.tobuffer(mixedsmall)
    Avro.tobuffer(mixedlarge)
    @test @allocations(Avro.tobuffer(mixedlarge)) -
          @allocations(Avro.tobuffer(mixedsmall)) < 1000
end
