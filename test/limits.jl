@testset "Limits" begin
    l = Avro.Limits()
    @test l.max_total_bytes == 256 << 20
    @test l.max_block_bytes == 16 << 20
    @test l.max_codec_memory == 32 << 20
    @test l.max_values_per_byte == 16
    @test l.work_allowance == 65_536
    @test l.max_compare_bytes_per_byte == 64
    @test l.max_inflight_blocks == 0
    @test Avro.Limits(max_total_bytes=1 << 30).max_total_bytes == 1 << 30
    @test_throws ArgumentError Avro.Limits(bogus=1)
    @test_throws ArgumentError Avro.Limits(max_depth=-1)
    @test sprint(show, MIME("text/plain"), l) isa String

    @testset "constructor relations at equality and one byte beyond" begin
        # max_codec_memory ≥ 16 MiB
        @test Avro.Limits(max_codec_memory=16 << 20).max_codec_memory == 16 << 20
        @test_throws ArgumentError Avro.Limits(max_codec_memory=(16 << 20) - 1)
        # max_datum_bytes ≤ max_bytes + 1 MiB
        @test Avro.Limits(max_datum_bytes=(64 << 20) + (1 << 20)).max_datum_bytes == (64 << 20) + (1 << 20)
        @test_throws ArgumentError Avro.Limits(max_datum_bytes=(64 << 20) + (1 << 20) + 1)
        # max_block_bytes ≤ ceiling ÷ 4
        @test Avro.Limits(max_block_bytes=64 << 20).max_block_bytes == 64 << 20
        @test_throws ArgumentError Avro.Limits(max_block_bytes=(64 << 20) + 1)
        # max_codec_memory + 4 MiB ≤ ceiling ÷ 4
        @test Avro.Limits(max_codec_memory=60 << 20).max_codec_memory == 60 << 20
        @test_throws ArgumentError Avro.Limits(max_codec_memory=(60 << 20) + 1)
        # max_block_output_bytes ≤ ceiling ÷ 2
        @test Avro.Limits(max_block_output_bytes=128 << 20).max_block_output_bytes == 128 << 20
        @test_throws ArgumentError Avro.Limits(max_block_output_bytes=(128 << 20) + 1)
        # max_metadata_bytes + max_schema_bytes ≤ ceiling ÷ 2
        @test Avro.Limits(max_metadata_bytes=112 << 20).max_metadata_bytes == 112 << 20
        @test_throws ArgumentError Avro.Limits(max_metadata_bytes=(112 << 20) + 1)
        # the first unit of progress fits under the default ceiling, and must fit any ceiling
        @test Avro.first_unit_bytes(l) <= l.max_total_bytes
        lowceiling = 4 * ((32 << 20) + (4 << 20))   # the smallest ceiling the codec relation admits
        @test Avro.first_unit_bytes(Avro.Limits(max_total_bytes=lowceiling)) <= lowceiling
        @test_throws ArgumentError Avro.Limits(max_total_bytes=lowceiling, max_bytes=100 << 20,
                                               max_datum_bytes=101 << 20)
    end

    @testset "the ceiling is max_total_bytes, independent of host memory" begin
        @test Avro.Budget(l).ceiling == l.max_total_bytes
        @test Avro.Budget(Avro.Limits(max_total_bytes=1 << 40)).ceiling == 1 << 40
        @test !isdefined(Avro, :available_memory) && !isdefined(Avro, :GUARD)
    end

    @testset "Budget: reservations, pending counter, release" begin
        b = Avro.Budget(l)
        Avro.reserve!(b, 1000 + (1 << 20))
        @test b.reserved == 1000 + (1 << 20) && b.peak == b.reserved && b.pending == b.reserved
        Avro.allocated!(b, 400)
        @test b.pending == 600 + (1 << 20)
        @test_throws ArgumentError Avro.release!(b, 401)                            # only the resident 400 can release
        @test_throws ArgumentError Avro.allocated!(b, 601 + (1 << 20))              # settlement mismatch fails, no clamp
        @test_throws ArgumentError Avro.unreserve!(b, 601 + (1 << 20))              # only pending returns via unreserve!
        Avro.release!(b, 400)                                                       # the resident portion
        Avro.unreserve!(b, 600 + (1 << 20))                                         # the never-resident headroom
        @test b.reserved == 0 && b.pending == 0
        Avro.reserve!(b, 10)
        underflow_state = (b.reserved, b.pending)
        @test_throws ArgumentError Avro.release!(b, 11)
        @test_throws ArgumentError Avro.release!(b, 10)                             # reserved but pending: nothing resident yet
        @test (b.reserved, b.pending) == underflow_state
        Avro.unreserve!(b, 10)
        @test_throws Avro.LimitError Avro.reserve!(b, b.ceiling + 1)
        Avro.reserve!(b, b.ceiling)   # exactly the ceiling is admitted
        @test_throws Avro.LimitError Avro.reserve!(b, 1)
        Avro.close!(b)
        @test b.pending == 0
        @test_throws ArgumentError Avro.reserve!(b, -1)
        r = Avro.withbudget(l) do bb
            Avro.reserve!(bb, 10)
            42
        end
        @test r == 42
        @test_throws ErrorException Avro.withbudget(l) do bb
            Avro.reserve!(bb, 10)
            error("boom")
        end
    end

    @testset "BuildBuf accounts for its mutable shell" begin
        b = Avro.Budget(l)
        bb = Avro.BuildBuf{Int}(b, 4)
        @test b.pending == 0
        @test b.reserved == Avro.vectorbytes(Int, 4) + Avro.shellbytes(typeof(bb))
        push!(bb, b, 1)
        data = Avro.finishbuild!(bb, b)
        @test data == [1]
        @test b.reserved == Avro.vectorbytes(Int, 1)
        data = nothing
        Avro.release!(b, Avro.vectorbytes(Int, 1))
        @test b.reserved == 0
        Avro.close!(b)
    end

    @testset "settlement contract" begin
        b = Avro.Budget(l)
        Avro.reserve!(b, 1000)
        @test b.pending == 1000 && b.reserved == 1000
        @test_throws ArgumentError Avro.allocated!(b, -1)               # negative settlements are mismatches too
        @test_throws ArgumentError Avro.release!(b, -1)
        @test_throws ArgumentError Avro.unreserve!(b, -1)
        @test b.pending == 1000 && b.reserved == 1000
        @test_throws ArgumentError Avro.allocated!(b, 1001)              # settlement mismatch fails, no clamp
        @test_throws ArgumentError Avro.release!(b, 1)                   # nothing resident yet
        Avro.allocated!(b, 600)
        @test b.pending == 400
        @test_throws ArgumentError Avro.unreserve!(b, 401)               # only pending returns via unreserve!
        Avro.unreserve!(b, 400)
        @test b.pending == 0 && b.reserved == 600
        Avro.release!(b, 600)
        @test b.reserved == 0
        Avro.close!(b)
    end

    @testset "a live Writer settles everything but its preflight model" begin
        meta = Dict("user.blob" => rand(UInt8, 2 << 20))
        s = Avro.parseschema("{\"type\":\"record\",\"name\":\"RP\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        io = IOBuffer()
        w = Avro.Writer(io, s; metadata=meta)
        try
            b = w.budget
            # the retained header copies (schema json, codec, metadata) are resident and settled;
            # only the preflight reader-model (never materialised in this process) may stay pending
            @test b.pending <= w.preflightbase
            @test b.reserved - b.pending >= Avro.bytesbytes(2 << 20)     # the resident metadata copy
        finally
            close(w)
        end
        # decode-side: a completed datum decode leaves nothing pending
        db = Avro.Budget(Avro.Limits())
        bytes = Avro.encode(s, (a=Int64(1),))
        d = Avro.Decoder(bytes, db)
        v = Avro.decode(Avro.readplan(s), d)
        @test v.a == 1
        @test db.pending == 0 && db.reserved > 0                         # all live output is settled resident
        Avro.close!(db)
    end

    @testset "a prepared reader recovers after a failed call" begin
        schema = Avro.parseschema("""{"type":"record","name":"Guarded","fields":[{"name":"payload","type":"bytes"},{"name":"valid","type":"boolean"}]}""")
        good = Avro.encode(schema, (payload=zeros(UInt8, 1 << 20), valid=true))
        bad = copy(good)
        bad[end] = 0x02
        reader = Avro.DatumReader(schema)
        @test_throws Avro.DataError reader(bad)
        @test reader(good).valid === true
    end

    @testset "guarded IO source normalization" begin
        input = fill(UInt8(' '), 64 << 10)
        budget = Avro.Budget(l)
        bytes = Avro.sourcebytes(IOBuffer(input), length(input), budget, Avro.SchemaError)
        @test bytes == input
        @test budget.peak >= 2 * Avro.bytesbytes(length(input))
        Avro.close!(budget)

        datum_budget = Avro.Budget(l)
        err = try
            Avro.sourcebytes(IOBuffer(UInt8[0x01, 0x02]), 1, datum_budget, Avro.DataError)
            nothing
        catch e
            e
        end
        @test err isa Avro.LimitError && err.limit == :max_datum_bytes && err.keyword == :max_datum_bytes
        Avro.close!(datum_budget)
    end

    @testset "Budget: work, comparison and count rules" begin
        b = Avro.Budget(l)
        Avro.addinput!(b, 10)
        # values ≤ 16 × 10 + 65_536
        Avro.countvalues!(b, 16 * 10 + 65_536)
        @test_throws Avro.LimitError Avro.countvalues!(b, 1)
        b2 = Avro.Budget(Avro.Limits(work_allowance=0))
        Avro.addinput!(b2, 1)
        Avro.countvalues!(b2, 16)
        e = try Avro.countvalues!(b2, 1); nothing catch err; err end
        @test e isa Avro.LimitError && e.limit == :max_values_per_byte && e.observed == 17 && e.value == 16
        b3 = Avro.Budget(Avro.Limits(work_allowance=0))
        Avro.addinput!(b3, 1)
        Avro.addcompare!(b3, 64)
        @test_throws Avro.LimitError Avro.addcompare!(b3, 1)
        b4 = Avro.Budget(Avro.Limits(max_total_values=5, work_allowance=1 << 20))
        Avro.countvalues!(b4, 5)
        @test_throws Avro.LimitError Avro.countvalues!(b4, 1)
        b5 = Avro.Budget(Avro.Limits(max_rows=2, max_blocks=1, max_resolution_work=3))
        Avro.addrows!(b5, 2); @test_throws Avro.LimitError Avro.addrows!(b5, 1)
        Avro.addblocks!(b5); @test_throws Avro.LimitError Avro.addblocks!(b5)
        Avro.addresolution!(b5, 3); @test_throws Avro.LimitError Avro.addresolution!(b5)
        @test Avro.checkdepth(b5, l.max_depth) === nothing
        @test_throws Avro.LimitError Avro.checkdepth(b5, l.max_depth + 1)
        # codec members count as values
        b6 = Avro.Budget(Avro.Limits(work_allowance=0))
        Avro.addinput!(b6, 1)
        Avro.addmembers!(b6, 16)
        @test_throws Avro.LimitError Avro.addmembers!(b6, 1)
        @test_throws ArgumentError Avro.Budget(l; direction=:sideways)
        e2 = try
            Avro.reserve!(Avro.Budget(l; direction=:encode), typemax(Int))
        catch err
            err
        end
        @test e2 isa Avro.LimitError && e2.direction == :encode
        @test occursin("encoding", sprint(showerror, e2))

        allowance = 17
        exactinput = (typemax(Int) - allowance) ÷ 2
        @test Avro.muladdcap(2, exactinput, allowance) == 2 * exactinput + allowance
        @test Avro.muladdcap(2, exactinput + 1, allowance) == typemax(Int)
        @test Avro.muladdcap(typemax(Int), 1, 0) == typemax(Int)
        @test Avro.muladdcap(typemax(Int), 2, typemax(Int)) == typemax(Int)
        @test Avro.satadd(typemax(Int), 1) == typemax(Int)
        raisedlimits = Avro.Limits(max_bytes=typemax(Int),
                                   max_datum_bytes=typemax(Int),
                                   max_total_bytes=typemax(Int))
        @test raisedlimits.max_bytes == typemax(Int)
        @test Avro.first_unit_bytes(raisedlimits) == typemax(Int)
        @test Avro.Budget(raisedlimits).ceiling == typemax(Int)
        @test_throws ArgumentError Avro.Limits(max_bytes=big(typemax(Int)) + 1)
        @test issorted(Avro.muladdcap(typemax(Int), input, 1)
                       for input in (0, 1, 2, typemax(Int)))
        saturated = Avro.Limits(max_values_per_byte=typemax(Int),
                                max_compare_bytes_per_byte=typemax(Int))
        saturatedbudget = Avro.Budget(saturated)
        Avro.addinput!(saturatedbudget, 2)
        Avro.countvalues!(saturatedbudget, 4)
        Avro.addcompare!(saturatedbudget, 4)
        @test saturatedbudget.workcap == saturated.max_total_values
        @test Avro.checkoperationwork!(saturatedbudget) == 0
        @test Avro.checkcomparisonwork!(saturatedbudget) === nothing
        Avro.close!(saturatedbudget)

        ints = Avro.IntSchema()
        encoded = Avro.encode(ints, Int32(7); limits=saturated)
        @test Avro.decode(ints, encoded; limits=saturated) == Int32(7)
        @test Avro.fromjson(ints, "7"; limits=saturated) == Int32(7)
        @test Avro.parseschema("{\"type\":\"record\",\"name\":\"Saturated\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
                               limits=saturated) isa Avro.RecordSchema
        saturatedio = IOBuffer()
        saturatedwriter = Avro.Writer(saturatedio, ints; limits=saturated)
        push!(saturatedwriter, Int32(7))
        close(saturatedwriter)
        @test Avro.Reader(reader -> collect(Avro.eachdatum(reader)), take!(saturatedio);
                          limits=saturated) == Any[Int32(7)]
    end

    @testset "error types" begin
        @test sprint(showerror, Avro.SchemaError("bad", "\$.fields[0]")) == "SchemaError: bad (at \$.fields[0])"
        @test sprint(showerror, Avro.DataError("truncated", 7)) == "DataError: truncated (at byte 7)"
        @test sprint(showerror, Avro.UnsupportedCodecError("bzip2", "CodecBzip2")) == "UnsupportedCodecError: codec \"bzip2\" requires `using CodecBzip2`"
        @test sprint(showerror, Avro.UnknownSchemaError(UInt64(255))) == "UnknownSchemaError: no schema registered for fingerprint 0x00000000000000ff"
        @test occursin("poisoned", sprint(showerror, Avro.WriterClosedError(ErrorException("x"))))
        @test Avro.DataError <: Avro.DecodeError <: Avro.AvroError
        for T in (Avro.SchemaError, Avro.EncodeError, Avro.ResolutionError, Avro.LimitError, Avro.CodecError,
                  Avro.UnsupportedCodecError, Avro.ConversionError, Avro.UnknownSchemaError,
                  Avro.AmbiguousSchemaError, Avro.WriterClosedError)
            @test T <: Avro.AvroError
        end
    end
end
