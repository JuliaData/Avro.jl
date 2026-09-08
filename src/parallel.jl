# Parallel container decode (plan §4.9): `Avro.Table` over byte and mapped sources pre-scans block
# headers into a block table, preallocates the final columns once at their exact storage, decodes the
# lowest uncommitted block directly into them under the sequential rule, and admits higher blocks
# strictly in block order into headroom with complete worst-case reservations (no eviction, no retry,
# every block decoded at most once). Commits are ordered and cumulative; the lowest failing block index
# wins regardless of failure kind. Parallelism only ever uses headroom: with no headroom the operation
# is the `ntasks = 1` direct path.

"One pre-scanned block: payload location, framing, and its prefix-sum row offset."
struct BlockEntry
    index::Int
    offset::Int          # first payload byte in the source buffer
    size::Int            # compressed payload size
    count::Int           # declared datum count
    rowstart::Int        # 1-based row offset of the block's first datum
end

"A logical view of an exact-capacity block-table allocation."
struct BlockTable <: AbstractVector{BlockEntry}
    storage::Vector{BlockEntry}
    count::Int
end

function Base.size(table::BlockTable)
    return (table.count,)
end

function Base.IndexStyle(::Type{BlockTable})
    return IndexLinear()
end

function Base.getindex(table::BlockTable, i::Int)
    @boundscheck checkbounds(table, i)
    return @inbounds table.storage[i]
end

function capacity(table::BlockTable)
    return capacity(table.storage)
end

"Fixed per-block decoder and coordinator state outside schema-derived builders and value storage."
const BLOCK_STATE_MAX = 8 * MiB

"The settled charged per-worker state allowance (plan §4.4; task stacks are runtime-owned, documented)."
const WORKER_STATE = 16 * 1024

struct PrescanResult
    entries::BlockTable
    totalrows::Int
    pending::Union{Nothing,Exception}   # a stage-1 structural failure, pending at index nblocks + 1
end

"Walk block headers without decompression (charged block table; a bad header becomes a pending failure)."
function prescanblocks(r::Reader)
    src = r.source::BytesSource
    tablecap = 64
    reserve!(r.budget, blocktablecharge(tablecap))     # the block table grows by reserved exact-capacity replacement (§4.4)
    entries = Vector{BlockEntry}(undef, tablecap)
    allocated!(r.budget, blocktablecharge(tablecap))
    nentries = 0
    rows = 0
    pending = nothing
    startpos = src.pos
    try
        while !sourceeof(src)
            count = sourcevarint(src)
            (0 <= count <= r.limits.max_block_count) ||
                (count < 0 ? throw(DataError("negative block count $count", position(src))) :
                 throw(LimitError(:max_block_count, Int(count), r.limits.max_block_count, :max_block_count, :decode)))
            size = sourcevarint(src)
            (0 <= size <= r.limits.max_block_bytes) ||
                (size < 0 ? throw(DataError("negative block size $size", position(src))) :
                 throw(LimitError(:max_block_bytes, Int(size), r.limits.max_block_bytes, :max_block_bytes, :decode)))
            nentries < r.limits.max_blocks ||
                throw(LimitError(:max_blocks, nentries + 1, r.limits.max_blocks, :max_blocks, :decode))
            off = src.pos
            Int(size) <= src.stop - off + 1 || throw(DataError("truncated file", off))
            src.pos = off + Int(size)
            for i in 1:16
                (sourceeof(src) ? throw(DataError("truncated file", src.pos)) : sourcebyte(src)) == r.sync[i] ||
                    throw(DataError("sync marker mismatch after block $(nentries + 1)", src.pos))
            end
            blockrows = Int(count)
            blockrows > typemax(Int) - rows &&
                throw(DataError("cumulative block row count exceeds the platform Int range", src.pos))
            newrows = rows + blockrows
            newrows <= r.limits.max_rows ||
                throw(LimitError(:max_rows, newrows, r.limits.max_rows, :max_rows, :decode))
            if nentries == tablecap
                newcap = checked_mul(2, tablecap)
                reserve!(r.budget, blocktablecharge(newcap))
                replacement = Vector{BlockEntry}(undef, newcap)
                allocated!(r.budget, blocktablecharge(newcap))
                copyto!(replacement, 1, entries, 1, nentries)
                entries = replacement                  # the old table is unreachable only after the rebind
                release!(r.budget, blocktablecharge(tablecap))
                tablecap = newcap
            end
            nentries += 1
            entries[nentries] = BlockEntry(nentries, off, Int(size), Int(count), rows + 1)
            rows = newrows
        end
    catch e
        (e isa DataError || e isa LimitError) || rethrow()
        pending = e
    end
    src.pos = startpos
    return PrescanResult(BlockTable(entries, nentries), rows, pending)
end

"""
The complete worst-case reservation `W` of one higher in-flight block (plan §4.9): compressed size, the
decompressed-buffer cap, the codec decoder requirement, its exact chunk shells and capacities, chunk
payload up to the block-output cap, and the scratch/state allowance.
"""
function blockworstcase(limits::Limits, e::BlockEntry, cols::Vector{Type},
                        builderpeak::Int)
    chunk = 0
    for E in cols
        chunk = checked_add(chunk, vectorbytes(E, e.count))
    end
    # `max_block_output_bytes` first covers the retained chunk payload. A second copy bounds
    # construction scratch: every live replacement predecessor is no larger than its replacement,
    # map-sort/keep scratch is no larger than the retained map state, and WTF-8 scratch is no larger
    # than the retained string. These terms can coexist across nested values, but their sum is still
    # bounded by the corresponding retained output. Builder/fused state is exact and schema-derived.
    output = limits.max_block_output_bytes
    scratch = checked_add(output, checked_add(builderpeak, BLOCK_STATE_MAX))
    return checked_add(checked_add(checked_add(e.size, limits.max_block_bytes), limits.max_codec_memory),
                       checked_add(checked_add(chunk, output), scratch))
end

"A per-block budget: the block's own caps and counters under a ceiling of exactly `W`."
function blockbudget(limits::Limits, W::Int)
    return Budget(limits, W, :decode, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  min(limits.max_total_values, limits.work_allowance), 0, 0)
end

mutable struct FailBox
    @atomic idx::Int
end

function recordfailure!(fail::FailBox, index::Int)
    while true
        cur = @atomic fail.idx
        index >= cur && return nothing
        (@atomicreplace fail.idx cur => index).success && return nothing
    end
end

mutable struct BlockJob
    const entry::BlockEntry
    const W::Int
    const budget::Budget
    const done::Threads.Event
    cols::Union{Nothing,Vector{ColumnBuilder}}
    outputbytes::Int
    err::Union{Nothing,Exception}
    @atomic state::Symbol            # :pending → :done | :failed | :abandoned
end

"Deterministic §4.9 counters of one parallel operation (also read by the peak-RSS gate)."
mutable struct ParallelStats
    inflight_highwater::Int          # blocks concurrently in flight (the direct head plus workers)
    speculative_decoded::Int         # higher blocks decoded past an authoritative failure
    assembly_bytes::Int              # bytes copied or moved out of chunked blocks
    committed_bytes::Int             # category-(a) slots plus category-(b) payload committed
    jobpeakmax::Int                  # the largest per-block budget peak observed
    peak_violations::Int             # commits whose job peak exceeded its W (gate: 0)
    nworkers::Int
    values::Int                      # the operation's final cumulative counters (equal to sequential)
    input_bytes::Int
    rows::Int
    blocks::Int
end

function ParallelStats()
    return ParallelStats(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
end

"The last operation's parallel counters (test and gate introspection only)."
const LAST_PARALLEL_STATS = Ref{Any}(nothing)

"Test-only schedule forcing: `PARALLEL_HOOK[] = (event, index) -> ...` (`:poolwrap`, `:admitted`, `:headdone`, `:workerstart`, `:workerdone`, `:commit`)."
const PARALLEL_HOOK = Ref{Any}(nothing)

function phook(event::Symbol, index::Int)
    return (h = PARALLEL_HOOK[]; h === nothing || h(event, index); nothing)
end

# ---- direct decode (the sequential rule, head task and ntasks = 1) -----------------------------------

function maketyped(plan::P, data::Vector{E}, base::Int, budget::Budget) where {P<:ReadPlan,E}
    charge = columnnodebytes(TypedColumn{E,P})
    reserve!(budget, charge)
    column = TypedColumn{E,P}(plan, data, base)
    allocated!(budget, charge)
    return column
end

"Builders that decode a block directly into the final columns at its prefix-sum offset (no chunks)."
function directbuilders(p::RecordPlan, projection::Union{Nothing,Projection},
                        finals::Vector{AbstractVector}, keptidx::Vector{Int}, base::Int,
                        budget::Budget)
    checkpoint = budgetcheckpoint(budget)
    cols = nothing
    try
        outer = vectorbytes(ColumnBuilder, length(p.fields))
        reserve!(budget, outer)
        cols = Vector{ColumnBuilder}(undef, length(p.fields))
        allocated!(budget, outer)
        for i in eachindex(p.fields)
            position = projection === nothing ? i : projection.slots[i]
            if position == 0
                setskipcolumn!(cols, i, p.fields[i], budget)
            else
                cols[i] = maketyped(p.fields[i], finals[position], base, budget)
            end
        end
        return cols
    catch
        cols = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function directbuilders(p::ResolvedRecordPlan, projection::Union{Nothing,Projection},
                        finals::Vector{AbstractVector}, keptidx::Vector{Int}, base::Int,
                        budget::Budget)
    n = length(p.schema.fields)
    checkpoint = budgetcheckpoint(budget)
    plans = nothing
    cols = nothing
    try
        planbytes = vectorbytes(Union{Nothing,ReadPlan}, n)
        reserve!(budget, planbytes)
        plans = Vector{Union{Nothing,ReadPlan}}(nothing, n)
        allocated!(budget, planbytes)
        for (slot, plan) in p.steps
            slot == 0 || (plans[slot] = plan)
        end
        for (slot, dp) in p.defaults
            plans[slot] = dp
        end
        outer = vectorbytes(ColumnBuilder, n)
        reserve!(budget, outer)
        cols = Vector{ColumnBuilder}(undef, n)
        allocated!(budget, outer)
        for i in 1:n
            plan = plans[i]::ReadPlan
            position = projection === nothing ? i : projection.slots[i]
            if position == 0
                setskipcolumn!(cols, i, plan, budget)
            else
                cols[i] = maketyped(plan, finals[position], base, budget)
            end
        end
        plans = nothing
        release!(budget, planbytes)
        return cols
    catch
        plans = nothing
        cols = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"Decode one pre-scanned block under the sequential rule (the main budget, actual-size reservations)."
function decodeblockrows!(spanplan::S, d::Decoder, plan, cells,
                          n::Int, b::Budget,
                          projection::Union{Nothing,Projection}, resolved::Bool,
                          outputbase::Int, slotrow::Int, cap::Int) where {S<:SpanPlan}
    staticvalues = resolved ? -1 : staticdatumvalues(spanplan, b.limits)
    blockout = 0
    if staticvalues >= 0
        for done in 1:n
            datumwork = beginstaticdatum!(d, staticvalues)
            try
                countvalues!(b)
                decoderow!(cells, d, plan)
                finishstaticdatum!(d, datumwork)
            catch
                abortdatum!(d, datumwork)
                rethrow()
            end
            blockout = checkblockoutput(b, outputbase, done, slotrow, cap)
        end
        return blockout
    end
    for done in 1:n
        datumspan!(spanplan, d, d.pos, d.budget.values,
                   d.budget.input_bytes)
        span = decoderspan(d)
        planneddatumspan!(plan, d, span, resolved, projection, true,
                          d.budget.values)
        span = decoderspan(d)
        datumwork = begindatum!(d, span)
        try
            countvalues!(b)
            decoderow!(cells, d, plan)
        catch
            abortdatum!(d, datumwork)
            rethrow()
        end
        finishdatum!(d, datumwork)
        blockout = checkblockoutput(b, outputbase, done, slotrow, cap)
    end
    return blockout
end

function decodedirect!(r::Reader, e::BlockEntry, plan, builders::Vector{ColumnBuilder},
                       slotrow::Int, projection::Union{Nothing,Projection}, resolved::Bool)
    b = r.budget
    work = BlockWork(b.values, b.input_bytes)
    src = r.source::BytesSource
    addblocks!(b)
    payload = view(src.buf, e.offset:e.offset + e.size - 1)
    addinput!(b, varintlength(e.count) + varintlength(e.size) + 16)
    out = decompressblock(r.codecname, r.codec, payload, r.limits, b)
    n = e.count
    r.validate === :strict && r.legacy === :avrojl1 &&
        (out = exactlegacyblock(out, n, r))
    checkblocklower!(b, work, n, spanvalues(r.span))
    d = setblockscope!(Decoder(out, b; validate=r.validate), work)
    cells = plan isa RecordPlan ? fuseskips(builders, b) : builders
    try
        outputbase = b.reserved
        cap = r.limits.max_block_output_bytes
        decodeblockrows!(r.span, d, plan, cells, n, b, projection, resolved,
                         outputbase, slotrow, cap)
    finally
        releasefused!(cells, builders, b)
    end
    if d.pos != length(out) + 1
        if r.legacy === :avrojl1 && r.codecname === :null
            r.warned || (@warn "accepting trailing bytes after $(n) datums in a null-codec block (legacy=:avrojl1; Avro.jl ≤ 1.1.2 sizing cushion)" source = 1; r.warned = true)
        else
            throw(DataError("block datums did not consume the block exactly", d.pos))
        end
    end
    addrows!(b, n)
    finishblockwork!(b, work)
    release!(b, bytesbytes(length(out)))
    return nothing
end

# ---- worker decode ----------------------------------------------------------------------------------

"Decode one admitted higher block into per-block chunk columns under its own full reservation."
function decodejob!(job::BlockJob, r::Reader, plan,
                    projection::Union{Nothing,Projection}, fail::FailBox, slotrow::Int,
                    resolved::Bool)
    e = job.entry
    if (@atomic fail.idx) < e.index
        @atomic job.state = :abandoned
        notify(job.done)
        return nothing
    end
    b = job.budget
    try
        phook(:workerstart, e.index)
        src = r.source::BytesSource
        payload = view(src.buf, e.offset:e.offset + e.size - 1)
        cname, codec = readercodec(r.codecname, r.limits, r.legacy)
        work = BlockWork(b.values, b.input_bytes)
        addinput!(b, varintlength(e.count) + varintlength(e.size) + 16)
        out = decompressblock(cname, codec, payload, r.limits, b)
        n = e.count
        checkblocklower!(b, work, n, spanvalues(r.span))
        cols = columnbuilders(plan, projection, n, b)
        cells = plan isa RecordPlan ? fuseskips(cols, b) : cols
        d = setblockscope!(Decoder(out, b; validate=r.validate), work)
        blockout = 0
        try
            outputbase = b.reserved
            cap = r.limits.max_block_output_bytes
            blockout = decodeblockrows!(r.span, d, plan, cells, n, b,
                                        projection, resolved, outputbase,
                                        slotrow, cap)
        finally
            releasefused!(cells, cols, b)
        end
        d.pos == length(out) + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
        finishblockwork!(b, work)
        job.outputbytes = blockout
        release!(b, bytesbytes(length(out)))
        job.cols = cols
        @atomic job.state = :done
        phook(:workerdone, e.index)
    catch err
        job.err = err
        @atomic job.state = :failed
        recordfailure!(fail, e.index)
    finally
        notify(job.done)
    end
    return nothing
end

function workerloop(ch::Channel{BlockJob}, r::Reader, plan,
                    projection::Union{Nothing,Projection}, fail::FailBox, slotrow::Int,
                    resolved::Bool)
    for job in ch
        decodejob!(job, r, plan, projection, fail, slotrow, resolved)
    end
    return nothing
end

"Close one worker channel and wait until every successfully started worker has stopped."
function settleworkers!(ch::Channel{BlockJob}, workers::Vector{Task}, nstarted::Int=length(workers))
    close(ch)
    for i in 1:nstarted
        wait(workers[i])
    end
    return nothing
end

"Release reservations owned by jobs that never committed."
function abandonjobs!(jobs::Vector{Union{Nothing,BlockJob}}, b::Budget, stats::ParallelStats)
    for job in jobs
        job === nothing && continue
        st = @atomic job.state
        st === :done && (stats.speculative_decoded += 1)
        unreserve!(b, job.W)                           # worst-case headroom: reserved, never resident
        close!(job.budget)
    end
    return nothing
end

# ---- the coordinator --------------------------------------------------------------------------------

"Commit one finished worker block in index order: cumulative checks, chunk copy, reservation swap."
function commitjob!(r::Reader, job::BlockJob, finals::Vector{AbstractVector}, keptidx::Vector{Int}, stats::ParallelStats)
    b = r.budget
    e = job.entry
    retained = job.budget.reserved
    worstowned = true
    retainedowned = false
    committed = false
    try
        phook(:commit, e.index)
        unreserve!(b, job.W)                         # worst-case headroom: reserved, never resident
        worstowned = false
        reserve!(b, retained)                        # the block's retained chunks and payload
        allocated!(b, retained)                      # resident under the job budget; the charge transfers here
        retainedowned = true
        addblocks!(b)
        addinput!(b, job.budget.input_bytes)
        countvalues!(b, job.budget.values)
        b.allowance_used = max(b.allowance_used, job.budget.allowance_used)
        checkoperationwork!(b)
        addcompare!(b, job.budget.compare_bytes)
        b.members = checked_add(b.members, job.budget.members)
        addrows!(b, e.count)
        job.outputbytes <= r.limits.max_block_output_bytes ||
            throw(LimitError(:max_block_output_bytes, job.outputbytes, r.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
        chunkcap = 0
        cols = job.cols::Vector{ColumnBuilder}
        builderstate = columnbuildersstate(cols)
        for (pos, i) in enumerate(keptidx)
            c = cols[i]::TypedColumn
            copyto!(finals[pos], e.rowstart, c.data, 1, e.count)
            moved = vectorbytes(eltype(c.data), e.count)
            chunkcap = checked_add(chunkcap, moved)
            stats.assembly_bytes = checked_add(stats.assembly_bytes, moved)
        end
        stats.committed_bytes = checked_add(stats.committed_bytes, retained - builderstate)
        stats.jobpeakmax = max(stats.jobpeakmax, job.budget.peak)
        job.budget.peak <= job.W || (stats.peak_violations += 1)
        release!(b, checked_add(chunkcap, builderstate))  # references moved; payload transfers, counted once
        close!(job.budget)
        committed = true
        return nothing
    finally
        if !committed
            worstowned && unreserve!(b, job.W)       # worst-case headroom: reserved, never resident
            retainedowned && release!(b, retained)
            close!(job.budget)
        end
    end
end

function finalcounters!(stats::ParallelStats, b::Budget)
    stats.values = b.values
    stats.input_bytes = b.input_bytes
    stats.rows = b.rows
    stats.blocks = b.blocks
    return nothing
end

"The pool plan: per-row slot bytes, in-flight cap, the worker count the ceiling admits, and the pool-state charge."
function poolplan(r::Reader, cols::Vector{Type}, entries::BlockTable, ntasks::Int,
                  builderpeak::Int)
    slotrow = 0
    for E in cols
        slotrow = checked_add(slotrow, slotbytes(E))
    end
    b = r.budget
    inflightcap = r.limits.max_inflight_blocks == 0 ? ntasks - 1 : min(r.limits.max_inflight_blocks, ntasks - 1)
    nworkers = min(ntasks - 1, Threads.nthreads() - 1, inflightcap, max(length(entries) - 1, 0))
    poolstate = 0
    if nworkers > 0 && r.legacy === nothing          # legacy tolerance mutates reader state: direct path only
        Whead = blockworstcase(r.limits, entries[1], cols, builderpeak)
        W2 = blockworstcase(r.limits, entries[2], cols, builderpeak)
        workersstate = checked_mul(WORKER_STATE, nworkers)
        jobsstate = vectorbytes(Union{Nothing,BlockJob}, length(entries))
        channelstate = vectorbytes(BlockJob, inflightcap)
        poolstate = checked_add(workersstate, checked_add(jobsstate, channelstate))
        checked_add(checked_add(b.reserved, Whead), checked_add(W2, poolstate)) <= b.ceiling || (nworkers = 0)
    else
        nworkers = 0
    end
    return (slotrow, inflightcap, nworkers, poolstate)
end

"The live parallel pool: its reserved state charge, its objects, and liveness (round-4 item 6)."
mutable struct BlockPool
    fail::Union{Nothing,FailBox}
    jobs::Union{Nothing,Vector{Union{Nothing,BlockJob}}}
    ch::Union{Nothing,Channel{BlockJob}}
    workers::Union{Nothing,Vector{Task}}
    const poolstate::Int
    alive::Bool
end

"""
Reserve the complete pool before its first package-owned object, then start the workers. A partial
startup drains started workers and returns the never-resident reservation through `unreserve!`.
Returns a live `BlockPool` with the pool-state charge settled resident.
"""
function startpool(r::Reader, plan, projection::Union{Nothing,Projection}, slotrow::Int, nblocks::Int,
                   inflightcap::Int, nworkers::Int, poolstate::Int, resolved::Bool)
    b = r.budget
    reserve!(b, poolstate)
    ch = nothing
    workers = nothing
    nstarted = 0
    try
        fail = FailBox(typemax(Int))
        jobs = Vector{Union{Nothing,BlockJob}}(nothing, nblocks)
        ch = Channel{BlockJob}(inflightcap)
        workers = Vector{Task}(undef, nworkers)
        for i in 1:nworkers
            workers[i] = Threads.@spawn workerloop(ch::Channel{BlockJob}, r, plan, projection,
                                                    fail::FailBox, slotrow, resolved)
            errormonitor(workers[i])
            nstarted = i
        end
        phook(:poolwrap, 0)
        pool = BlockPool(fail, jobs, ch, workers, poolstate, true)
        allocated!(b, poolstate)                      # publish only after the complete pool exists
        return pool
    catch
        if ch !== nothing
            workers === nothing ? close(ch) : settleworkers!(ch, workers, nstarted)
        end
        ch = nothing
        workers = nothing
        unreserve!(b, poolstate)                     # never settled: the pool objects die with this frame
        rethrow()
    end
end

"""
Retire a live pool: settle the workers, drop every pool object so nothing outlives the reservation,
then release the settled pool-state charge. All in-flight jobs have committed (the ordered commit
wave drains every admitted job before the loop re-enters), so nothing is abandoned here.
"""
function retirepool!(pool::BlockPool, b::Budget)
    settleworkers!(pool.ch::Channel{BlockJob}, pool.workers::Vector{Task})
    pool.fail = nothing
    pool.jobs = nothing
    pool.ch = nothing
    pool.workers = nothing
    release!(b, pool.poolstate)
    pool.alive = false
    return nothing
end

"Tear the pool down on every exit: settle workers, abandon queued jobs, release the state charge."
function teardownpool!(pool::BlockPool, b::Budget, stats::ParallelStats)
    if pool.alive
        settleworkers!(pool.ch::Channel{BlockJob}, pool.workers::Vector{Task})
    end
    pool.jobs === nothing || abandonjobs!(pool.jobs::Vector{Union{Nothing,BlockJob}}, b, stats)
    pool.fail = nothing
    pool.jobs = nothing
    pool.ch = nothing
    pool.workers = nothing
    if pool.alive
        release!(b, pool.poolstate)
        pool.alive = false
    end
    return nothing
end

"The `ntasks = 1` sequential path: every block decoded directly into the finals in order."
function directpath!(r::Reader, plan, projection::Union{Nothing,Projection}, finals::Vector{AbstractVector},
                     keptidx::Vector{Int}, slotrow::Int, pre::PrescanResult,
    stats::ParallelStats, resolved::Bool)
    for e in pre.entries
        builders = directbuilders(plan, projection, finals, keptidx, e.rowstart - 1,
                                  r.budget)
        try
            decodedirect!(r, e, plan, builders, slotrow, projection, resolved)
        finally
            releasecolumnbuilders!(builders, r.budget)
        end
    end
    pre.pending === nothing || throw(pre.pending)
    finalcounters!(stats, r.budget)
    return stats
end

"""
The coordinator loop over a live pool: retire it when the head's worst case no longer fits beside
it, admit higher blocks into headroom, decode the head under the sequential rule (its failure is the
lowest and poisons queued blocks), then commit completed jobs in order.
"""
function runpool!(r::Reader, plan, projection::Union{Nothing,Projection}, finals::Vector{AbstractVector},
                  keptidx::Vector{Int}, cols::Vector{Type}, entries::BlockTable,
                  stats::ParallelStats, pool::BlockPool, inflightcap::Int, slotrow::Int,
                  builderpeak::Int, resolved::Bool)
    b = r.budget
    inflight = 0
    next = 1
    tocommit = 1
    while tocommit <= length(entries)
        head = entries[next]
        headidx = next
        next += 1
        Whead = blockworstcase(r.limits, head, cols, builderpeak)
        pool.alive && checked_add(b.reserved, Whead) > b.ceiling && retirepool!(pool, b)
        if pool.alive
            next, inflight = admitjobs!(r, entries, cols, pool.jobs::Vector{Union{Nothing,BlockJob}},
                                        pool.ch::Channel{BlockJob}, stats, Whead, builderpeak,
                                        next, inflight, inflightcap)
        end
        builders = directbuilders(plan, projection, finals, keptidx, head.rowstart - 1,
                                  r.budget)
        try
            decodedirect!(r, head, plan, builders, slotrow, projection, resolved)
        catch
            pool.alive && recordfailure!(pool.fail::FailBox, headidx)    # queued higher blocks abandon
            rethrow()
        finally
            releasecolumnbuilders!(builders, r.budget)
        end
        tocommit = headidx + 1
        phook(:headdone, headidx)
        if pool.alive
            tocommit, inflight = commitwave!(r, pool.jobs::Vector{Union{Nothing,BlockJob}}, finals,
                                             keptidx, stats, pool.fail::FailBox, tocommit, inflight)
        end
        next = tocommit
    end
    return nothing
end

"Admit higher blocks in order into ceiling headroom with complete worst-case reservations."
function admitjobs!(r::Reader, entries::BlockTable, cols::Vector{Type},
                    jobs::Vector{Union{Nothing,BlockJob}}, ch::Channel{BlockJob}, stats::ParallelStats,
                    Whead::Int, builderpeak::Int, next::Int, inflight::Int,
                    inflightcap::Int)
    b = r.budget
    while next <= length(entries) && inflight < inflightcap
        e2 = entries[next]
        Wi = blockworstcase(r.limits, e2, cols, builderpeak)
        checked_add(checked_add(b.reserved, Whead), Wi) <= b.ceiling || break
        reserve!(b, Wi)
        job = BlockJob(e2, Wi, blockbudget(r.limits, Wi), Threads.Event(), nothing, 0, nothing, :pending)
        jobs[next] = job
        put!(ch, job)
        inflight += 1
        stats.inflight_highwater = max(stats.inflight_highwater, inflight + 1)
        phook(:admitted, e2.index)
        next += 1
    end
    return (next, inflight)
end

"Commit completed jobs in block order from `tocommit`; the lowest failing index propagates."
function commitwave!(r::Reader, jobs::Vector{Union{Nothing,BlockJob}}, finals::Vector{AbstractVector},
                     keptidx::Vector{Int}, stats::ParallelStats, fail::FailBox, tocommit::Int, inflight::Int)
    while tocommit <= length(jobs) && jobs[tocommit] !== nothing
        job = jobs[tocommit]::BlockJob
        wait(job.done)
        inflight -= 1
        st = @atomic job.state
        st === :done || throw(job.err::Exception)              # the lowest uncommitted block's own failure
        jobs[tocommit] = nothing
        try
            commitjob!(r, job, finals, keptidx, stats)
        catch
            recordfailure!(fail, job.entry.index)
            rethrow()
        end
        tocommit += 1
    end
    return (tocommit, inflight)
end

"""
Decode a pre-scanned byte source into the preallocated finals: the direct head under the sequential
rule, higher blocks in order into headroom, ordered cumulative commits, lowest failing index wins.
"""
function decodeblocks!(r::Reader, plan, projection::Union{Nothing,Projection}, finals::Vector{AbstractVector},
                       keptidx::Vector{Int}, cols::Vector{Type}, pre::PrescanResult, ntasks::Int,
                       resolved::Bool=false)
    entries = pre.entries
    stats = ParallelStats()
    LAST_PARALLEL_STATS[] = stats
    builderpeak = columnbuilderpeakstate(plan, projection)
    slotrow, inflightcap, nworkers, poolstate = poolplan(r, cols, entries, ntasks,
                                                        builderpeak)
    stats.nworkers = nworkers
    nworkers == 0 && return directpath!(r, plan, projection, finals, keptidx, slotrow,
                                       pre, stats, resolved)
    pool = startpool(r, plan, projection, slotrow, length(entries), inflightcap,
                     nworkers, poolstate, resolved)
    try
        runpool!(r, plan, projection, finals, keptidx, cols, entries, stats, pool,
                 inflightcap, slotrow, builderpeak, resolved)
        pre.pending === nothing || throw(pre.pending)
        finalcounters!(stats, r.budget)
        return stats
    catch
        pool.alive && recordfailure!(pool.fail::FailBox, length(entries) + 1)   # abandon everything still queued
        rethrow()
    finally
        teardownpool!(pool, r.budget, stats)
    end
end
