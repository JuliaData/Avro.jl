@noinline function admitcallerownership(admission)
    caller = join(("caller", "-", "owned"))
    reference = WeakRef(caller)
    sourcepointer = pointer(caller)
    nbytes = sizeof(caller)
    Avro.admit!(admission, caller)
    return reference, sourcepointer, nbytes
end

@noinline function admitsubstringownership(admission)
    backing = join(("prefix", "substring", "suffix"))
    substring = SubString(backing, 7, 15)
    reference = WeakRef(backing)
    Avro.admit!(admission, substring)
    return reference
end

@testset "Symbol admission" begin
    base = Avro.admissionbasebytes()
    @test_throws ArgumentError Avro.SymbolAdmission(max_bytes=base - 1)   # the fixed structure must fit
    emptytable = Avro.SymbolAdmission(max_bytes=base)
    @test emptytable.bytes == base                                        # counted from construction (round-4 item 4)
    @test emptytable.bytes >= Base.summarysize(emptytable)
    a = Avro.SymbolAdmission(max_names=3, max_bytes=base + 100)
    @test Avro.admit!(a, "x") === :x
    @test Avro.admit!(a, "x") === :x      # already admitted: counted once
    @test length(a) == 1
    @test Avro.admit!(a, "y") === :y
    @test Avro.admit!(a, "z") === :z
    e = try Avro.admit!(a, "w"); nothing catch err; err end
    @test e isa Avro.LimitError && e.limit == :max_names && e.observed == 4 && e.value == 3
    b = Avro.SymbolAdmission(max_names=10, max_bytes=base + 19)   # base covers the fixed structure; 19 one string and slot
    Avro.admit!(b, "abc")
    e2 = try Avro.admit!(b, "def"); nothing catch err; err end
    @test e2 isa Avro.LimitError && e2.limit == :max_bytes && e2.observed == base + 38
    @test length(b) == 1 && Avro.admit!(b, "abc") === :abc  # the failed admission left the table unchanged
    @test Avro.admit!(:trusted, "anything") === :anything
    @test Avro.admission(:trusted) === :trusted
    @test_throws ArgumentError Avro.admission(:untrusted)
    @test_throws ArgumentError Avro.admission(1)
    @test Avro.admission(Avro.DEFAULT_ADMISSION) === Avro.DEFAULT_ADMISSION
    @test_throws ArgumentError Avro.SymbolAdmission(max_names=-1)

    owned = Avro.SymbolAdmission(max_names=4, max_bytes=base + 1_000)
    callerref, callerpointer, callerbytes = admitcallerownership(owned)
    @test only(owned.recent) == "caller-owned"
    @test pointer(only(owned.recent)) != callerpointer
    @test owned.bytes == base + Avro.stringbytes(callerbytes)
    GC.gc(true)
    GC.gc(true)
    @test callerref.value === nothing
    backingref = admitsubstringownership(owned)
    @test owned.recent[end] == "substring"
    GC.gc(true)
    GC.gc(true)
    @test backingref.value === nothing

    # sorted-runs structure: carries across every power-of-two boundary, lookups stay exact
    big = Avro.SymbolAdmission(max_names=10_000, max_bytes=1 << 20)
    names = ["n$(lpad(i, 5, '0'))" for i in 1:5000]
    for n in names
        Avro.admit!(big, n)
    end
    @test length(big) == 5000
    @test all(length(r) == Avro.RUN_BASE << k for (k, r) in zip(reverse(0:length(big.runs) - 1), big.runs)) || length(big.runs) >= 1
    for n in names
        Avro.admit!(big, n)   # no double counting after carries
    end
    @test length(big) == 5000
    @test Avro.admit!(big, "n00001") === :n00001
    # one-symbol admissions across power-of-two boundaries keep the count exact
    boundaries = [Avro.RUN_BASE << k for k in 0:3]
    small = Avro.SymbolAdmission(max_names=1 << 20, max_bytes=1 << 24)
    for i in 1:(Avro.RUN_BASE << 3) + 1
        Avro.admit!(small, "s$i")
        if (i in boundaries) || i == (Avro.RUN_BASE << 3) + 1
            @test length(small) == i
        end
    end
    @test Avro.admit!(small, "s1") === :s1 && length(small) == (Avro.RUN_BASE << 3) + 1

    # concurrent admissions are safe
    c = Avro.SymbolAdmission(max_names=1 << 20, max_bytes=1 << 24)
    tasks = [Threads.@spawn begin
        for i in 1:2000
            Avro.admit!(c, "t$(i % 300)")
        end
        nothing
    end for _ in 1:4]
    foreach(errormonitor, tasks)
    foreach(wait, tasks)
    @test length(c) == 300

    # deamortised merges: mid-merge lookups stay exact across every power-of-two boundary (plan §4.4)
    dm = Avro.SymbolAdmission(max_names=1 << 20, max_bytes=1 << 24)
    limit = (Avro.RUN_BASE << 6) + 3
    for i in 1:limit
        Avro.admit!(dm, "d$(lpad(i, 7, '0'))")
        if count_ones(i) == 1 && i >= Avro.RUN_BASE      # power-of-two boundaries: merges are staged here
            @test length(dm) == i
            @test Avro.admit!(dm, "d$(lpad(1, 7, '0'))") === Symbol("d0000001")      # oldest (deep run)
            @test Avro.admit!(dm, "d$(lpad(i, 7, '0'))") === Symbol("d$(lpad(i, 7, '0'))")  # newest
            @test Avro.admit!(dm, "d$(lpad(i ÷ 2, 7, '0'))") === Symbol("d$(lpad(i ÷ 2, 7, '0'))")
            @test length(dm) == i
        end
    end
    @test length(dm) == limit
    while dm.merge !== nothing                            # drain: every source string survives the swaps
        Avro.admit!(dm, "d$(lpad(1, 7, '0'))")
    end
    @test length(dm) == limit && sum(length, dm.runs; init=0) + length(dm.recent) == limit

    # one million incremental admissions, latency recorded (plan §4.4 gate)
    mm = Avro.SymbolAdmission(max_names=1 << 21, max_bytes=64 << 20)
    t0 = time()
    for i in 1:1_000_000
        Avro.admit!(mm, "m$(i)")
    end
    elapsed = time() - t0
    @info "admission gate" million_admissions_seconds = round(elapsed; digits=2)
    @test length(mm) == 1_000_000
    @test Avro.admit!(mm, "m1") === :m1 && Avro.admit!(mm, "m999999") === :m999999
    @test elapsed < 60

    # capacity boundaries: exact at max_names and max_bytes
    cap = Avro.SymbolAdmission(max_names=Avro.RUN_BASE + 1, max_bytes=1 << 20)
    for i in 1:Avro.RUN_BASE + 1
        Avro.admit!(cap, "c$(lpad(i, 4, '0'))")
    end
    e3 = try Avro.admit!(cap, "over"); nothing catch err; err end
    @test e3 isa Avro.LimitError && e3.limit == :max_names && length(cap) == Avro.RUN_BASE + 1
    @test Avro.admit!(cap, "c0001") === :c0001            # existing names still admit after the failure

    # A maintenance cascade that cannot reserve its next merge must fail before it moves
    # a cursor, replaces a run, or commits the new name. The same retry must fail with the
    # same observation; a failed call cannot make its own retry admissible.
    transactional = Avro.SymbolAdmission(max_names=10_000, max_bytes=base + 103_000)
    function admissionname(i)
        return "x$(lpad(i, 4, '0'))"
    end
    for i in 1:4_096
        Avro.admit!(transactional, admissionname(i))
    end

    function admissionstate(a)
        merge = a.merge
        merge_state = merge === nothing ? nothing :
                      (objectid(merge), objectid(merge.out), merge.i, merge.j)
        return (merge_state, objectid.(a.runs), length.(a.runs), copy(a.recent),
                a.mergeat, a.count, a.bytes)
    end
    before = admissionstate(transactional)
    first_error = try
        Avro.admit!(transactional, admissionname(4_097))
        nothing
    catch err
        err
    end
    @test first_error isa Avro.LimitError && first_error.limit == :max_bytes
    @test admissionstate(transactional) == before
    retry_error = try
        Avro.admit!(transactional, admissionname(4_097))
        nothing
    catch err
        err
    end
    @test retry_error isa Avro.LimitError
    @test (retry_error.limit, retry_error.observed, retry_error.value) ==
          (first_error.limit, first_error.observed, first_error.value)
    @test admissionstate(transactional) == before
    @test !Avro.contains_unlocked(transactional, admissionname(4_097))

    # round-3 item 5: the recent buffer is prebuilt at exact RUN_BASE capacity, and a live merge's
    # complete output state is held in `bytes` (replacement overlap) until its sources are dropped
    ov = Avro.SymbolAdmission(max_names=1 << 16, max_bytes=1 << 24)
    VERSION >= v"1.11" && @test Avro.capacity(ov.recent) == Avro.RUN_BASE
    for i in 1:2 * Avro.RUN_BASE
        Avro.admit!(ov, "o$(lpad(i, 5, '0'))")
    end
    ovnames = sum(Avro.stringbytes(sizeof("o$(lpad(i, 5, '0'))")) for i in 1:2 * Avro.RUN_BASE)
    liveoverlap = 8 * 2 * Avro.RUN_BASE + Avro.STORAGE[].vector + Avro.shellbytes(Avro.RunMerge)
    @test ov.merge !== nothing && ov.bytes == base + ovnames + 2 * Avro.STORAGE[].vector + liveoverlap
    @test ov.bytes >= Base.summarysize(ov)
    Avro.admit!(ov, "o$(lpad(1, 5, '0'))")                                     # a repeat completes the merge
    @test ov.merge === nothing && ov.bytes == base + ovnames + Avro.STORAGE[].vector
    @test ov.bytes >= Base.summarysize(ov)
    VERSION >= v"1.11" && @test Avro.capacity(ov.recent) == Avro.RUN_BASE      # reused, never regrown

    # the admitting operation's lookup and merge-step work charges its own budget (§4.4, R07)
    ab = Avro.SymbolAdmission(max_names=1 << 16, max_bytes=1 << 24)
    bud = Avro.Budget(Avro.Limits())
    Avro.addinput!(bud, 1 << 20)
    before = bud.compare_bytes
    Avro.admit!(ab, "charged-name"; budget=bud)
    @test bud.compare_bytes > before
    @test Avro.admit!(:trusted, "any"; budget=bud) === :any
    Avro.close!(bud)

    # lookup work is charged from the table state protected by the admission lock
    raced = Avro.SymbolAdmission(max_names=1 << 16, max_bytes=1 << 24)
    racebudget = Avro.Budget(Avro.Limits(work_allowance=40, max_compare_bytes_per_byte=0))
    lock(raced.lock)
    local task
    try
        started = Channel{Nothing}(1)
        task = errormonitor(Threads.@spawn try
            put!(started, nothing)
            Avro.admit!(raced, "z"; budget=racebudget)
        catch err
            err
        end)
        take!(started)
        for _ in 1:100
            yield()
        end
        @test racebudget.compare_bytes == 0
        for i in 1:1000
            Avro.admit!(raced, "race-$(lpad(i, 4, '0'))")
        end
    finally
        unlock(raced.lock)
    end
    raceerror = fetch(task)
    @test raceerror isa Avro.LimitError && raceerror.limit === :max_compare_bytes_per_byte
    @test length(raced) == 1000
    Avro.close!(racebudget)

    # a rejected admission advances no maintenance state (round-2 D10)
    tr = Avro.SymbolAdmission(max_names=Avro.RUN_BASE * 4, max_bytes=1 << 24)
    for i in 1:(2 * Avro.RUN_BASE + 100)              # leave a merge in progress
        Avro.admit!(tr, "t$(lpad(i, 6, '0'))")
    end
    full = Avro.SymbolAdmission(max_names=1, max_bytes=1 << 20)
    Avro.admit!(full, "only")
    function mstate(x)
        return (x.merge === nothing ? (0, 0) : (x.merge.i, x.merge.j),
            length(x.recent), length(x.runs), x.count, x.bytes)
    end
    before = mstate(tr)
    @test_throws Avro.LimitError Avro.admit!(full, "rejected")       # rejected on its own table
    @test mstate(tr) == before                                        # unrelated table untouched (sanity)
    beforefull = mstate(full)
    @test_throws Avro.LimitError Avro.admit!(full, "rejected2")
    @test mstate(full) == beforefull                                  # the rejected admission mutated nothing
    tb = mstate(tr)
    e = try
        Avro.admit!(tr, "x"^(1 << 25))                                # over max_bytes with a live merge
        nothing
    catch err
        err
    end
    @test e isa Avro.LimitError && mstate(tr) == tb                   # merge state, runs and counters unchanged
end
