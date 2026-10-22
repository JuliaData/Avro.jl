@noinline function admitcallerownership(admission)
    caller = join(("caller", "-", "owned"))
    reference = WeakRef(caller)
    Avro.admit!(admission, caller)
    return reference
end

@testset "Symbol admission" begin
    @test Avro.admit!(nothing, "anything") === :anything            # the default interns directly
    @test Avro.admission(nothing) === nothing
    @test_throws ArgumentError Avro.admission(:trusted)
    @test_throws ArgumentError Avro.admission(1)
    @test_throws ArgumentError Avro.SymbolAdmission(max_names=-1)
    @test_throws ArgumentError Avro.SymbolAdmission(max_bytes=-1)
    a = Avro.SymbolAdmission(max_names=3)
    @test Avro.admission(a) === a
    @test Avro.admit!(a, "x") === :x
    @test Avro.admit!(a, "x") === :x                                # already admitted: counted once
    @test length(a) == 1
    @test Avro.admit!(a, "y") === :y && Avro.admit!(a, "z") === :z
    e = try Avro.admit!(a, "w"); nothing catch err; err end
    @test e isa Avro.LimitError && e.limit == :max_names && e.observed == 4 && e.value == 3
    @test length(a) == 3 && Avro.admit!(a, "x") === :x              # existing names still admit after the failure
    one = Avro.stringbytes(3) + Avro.ADMISSION_SLOT_BYTES
    b = Avro.SymbolAdmission(max_names=10, max_bytes=one)
    Avro.admit!(b, "abc")
    e2 = try Avro.admit!(b, "def"); nothing catch err; err end
    @test e2 isa Avro.LimitError && e2.limit == :max_bytes && e2.observed == 2 * one
    @test length(b) == 1 && b.bytes == one && Avro.admit!(b, "abc") === :abc   # the failure left the table unchanged

    # admitted strings are owned copies: the caller's string and a view's parent can be collected
    owned = Avro.SymbolAdmission()
    ref = admitcallerownership(owned)
    GC.gc(true)
    GC.gc(true)
    @test ref.value === nothing && length(owned) == 1
    @test Avro.admit!(owned, "caller-owned") === Symbol("caller-owned") && length(owned) == 1
    @test Avro.admit!(owned, SubString("xsubstringx", 2, 10)) === :substring && length(owned) == 2

    # lookups stay exact across every run merge
    big = Avro.SymbolAdmission(max_names=10_000)
    names = ["n$(lpad(i, 5, '0'))" for i in 5000:-1:1]
    foreach(n -> Avro.admit!(big, n), names)
    @test length(big) == 5000
    foreach(n -> Avro.admit!(big, n), names)
    @test length(big) == 5000 && big.bytes == sum(n -> Avro.stringbytes(sizeof(n)) + Avro.ADMISSION_SLOT_BYTES, names)

    # concurrent admissions are safe
    c = Avro.SymbolAdmission()
    tasks = [Threads.@spawn begin
        for i in 1:2000
            Avro.admit!(c, "t$(i % 300)")
        end
        nothing
    end for _ in 1:4]
    foreach(errormonitor, tasks)
    foreach(wait, tasks)
    @test length(c) == 300

    mm = Avro.SymbolAdmission(max_names=1 << 21)
    t0 = time()
    for i in 1:1_000_000
        Avro.admit!(mm, "m$(i)")
    end
    elapsed = time() - t0
    @info "admission gate" million_admissions_seconds = round(elapsed; digits=2)
    @test length(mm) == 1_000_000 && Avro.admit!(mm, "m1") === :m1 && Avro.admit!(mm, "m999999") === :m999999
    @test elapsed < 60
end
