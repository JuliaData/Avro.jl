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
        # the first unit of progress fits under the default ceiling
        @test Avro.first_unit_bytes(l) <= l.max_total_bytes
    end

    @testset "effective ceiling and the available-memory guard" begin
        @test Avro.effective_ceiling(l; available=1 << 40) == l.max_total_bytes
        @test Avro.effective_ceiling(l; available=100 << 20) == 50 << 20
        @test Avro.effective_ceiling(l; available=0) == 0
        @test Avro.available_memory() > 0
        @test Avro.host_free_memory() >= Int(min(Sys.free_memory(), typemax(Int) % UInt64))
        Sys.isapple() && @test Avro.darwin_available_memory() > Sys.free_memory()      # inactive pages are reclaimable
        # injected available memory below the first unit of progress fails before any allocation
        e = try
            Avro.Budget(l; available=2 * Avro.first_unit_bytes(l) - 2)
            nothing
        catch err
            err
        end
        @test e isa Avro.LimitError
        @test e.limit == :available_memory
        @test e.keyword == :max_total_bytes
        @test Avro.Budget(l; available=2 * Avro.first_unit_bytes(l)) isa Avro.Budget
        @test occursin("raise `Avro.Limits(max_total_bytes=...)`", sprint(showerror, e))
    end

    @testset "Budget: reservations, pending counter, release" begin
        b = Avro.Budget(l; available=1 << 40)
        pending0 = @atomic Avro.GUARD.pending
        Avro.reserve!(b, 1000)
        @test b.reserved == 1000 && b.peak == 1000 && b.pending == 1000
        @test (@atomic Avro.GUARD.pending) == pending0 + 1000
        Avro.allocated!(b, 400)
        @test b.pending == 600
        @test (@atomic Avro.GUARD.pending) == pending0 + 600
        Avro.release!(b, 1000)
        @test b.reserved == 0 && b.pending == 0
        @test (@atomic Avro.GUARD.pending) == pending0
        @test_throws Avro.LimitError Avro.reserve!(b, b.ceiling + 1)
        Avro.reserve!(b, b.ceiling)   # exactly the ceiling is admitted
        @test_throws Avro.LimitError Avro.reserve!(b, 1)
        Avro.close!(b)
        @test (@atomic Avro.GUARD.pending) == pending0
        @test_throws ArgumentError Avro.reserve!(b, -1)
        r = Avro.withbudget(l; available=1 << 40) do bb
            Avro.reserve!(bb, 10)
            42
        end
        @test r == 42 && (@atomic Avro.GUARD.pending) == pending0
        # every exit path restores the guard
        @test_throws ErrorException Avro.withbudget(l; available=1 << 40) do bb
            Avro.reserve!(bb, 10)
            error("boom")
        end
        @test (@atomic Avro.GUARD.pending) == pending0
    end

    @testset "Budget: work, comparison and count rules" begin
        b = Avro.Budget(l; available=1 << 40)
        Avro.addinput!(b, 10)
        # values ≤ 16 × 10 + 65_536
        Avro.countvalues!(b, 16 * 10 + 65_536)
        @test_throws Avro.LimitError Avro.countvalues!(b, 1)
        b2 = Avro.Budget(Avro.Limits(work_allowance=0); available=1 << 40)
        Avro.addinput!(b2, 1)
        Avro.countvalues!(b2, 16)
        e = try Avro.countvalues!(b2, 1); nothing catch err; err end
        @test e isa Avro.LimitError && e.limit == :max_values_per_byte && e.observed == 17 && e.value == 16
        b3 = Avro.Budget(Avro.Limits(work_allowance=0); available=1 << 40)
        Avro.addinput!(b3, 1)
        Avro.addcompare!(b3, 64)
        @test_throws Avro.LimitError Avro.addcompare!(b3, 1)
        b4 = Avro.Budget(Avro.Limits(max_total_values=5, work_allowance=1 << 20); available=1 << 40)
        Avro.countvalues!(b4, 5)
        @test_throws Avro.LimitError Avro.countvalues!(b4, 1)
        b5 = Avro.Budget(Avro.Limits(max_rows=2, max_blocks=1, max_resolution_work=3); available=1 << 40)
        Avro.addrows!(b5, 2); @test_throws Avro.LimitError Avro.addrows!(b5, 1)
        Avro.addblocks!(b5); @test_throws Avro.LimitError Avro.addblocks!(b5)
        Avro.addresolution!(b5, 3); @test_throws Avro.LimitError Avro.addresolution!(b5)
        @test Avro.checkdepth(b5, l.max_depth) === nothing
        @test_throws Avro.LimitError Avro.checkdepth(b5, l.max_depth + 1)
        # codec members count as values
        b6 = Avro.Budget(Avro.Limits(work_allowance=0); available=1 << 40)
        Avro.addinput!(b6, 1)
        Avro.addmembers!(b6, 16)
        @test_throws Avro.LimitError Avro.addmembers!(b6, 1)
        @test_throws ArgumentError Avro.Budget(l; direction=:sideways, available=1 << 40)
        e2 = try
            Avro.reserve!(Avro.Budget(l; direction=:encode, available=1 << 40), typemax(Int))
        catch err
            err
        end
        @test e2 isa Avro.LimitError && e2.direction == :encode
        @test occursin("encoding", sprint(showerror, e2))
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
