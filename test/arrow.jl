import Arrow

@testset "Arrow preserves exact Avro timestamps" begin
    for (period, unit) in ((Millisecond, Arrow.Meta.TimeUnit.MILLISECOND),
                           (Microsecond, Arrow.Meta.TimeUnit.MICROSECOND),
                           (Nanosecond, Arrow.Meta.TimeUnit.NANOSECOND))
        for (make, zone) in ((Avro.timestamp, :UTC), (Avro.localtimestamp, nothing))
            ticks = Int64[typemin(Int64), -1, 0, 1, typemax(Int64)]
            stamps = [make(period, t) for t in ticks]
            T = eltype(stamps)
            for values in (stamps, Union{Missing,T}[missing; stamps])
                table = Avro.Table(Avro.tobuffer((time=values,)))
                io = IOBuffer()
                Arrow.write(io, table)
                bytes = take!(io)
                restored = Arrow.Table(bytes)
                @test eltype(restored.time) === eltype(values)
                @test isequal(collect(restored.time), values)
                native = Arrow.Table(bytes; convert=false)
                @test Base.nonmissingtype(eltype(native.time)) === Arrow.Timestamp{unit,zone}
                @test [x.x for x in skipmissing(native.time)] == ticks
                @test isequal(Avro.Table(Avro.tobuffer(restored)).time, values)
            end
        end
    end
end
