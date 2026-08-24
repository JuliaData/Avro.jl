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

"Per-block worst-case scratch-and-state allowance of `W` (plan §4.9; job peaks are gated against `W`)."
const SCRATCH_STATE_MAX = 8 * MiB

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
            newrows = checked_add(rows, Int(count))
            newrows <= r.limits.max_rows ||
                throw(LimitError(:max_rows, newrows, r.limits.max_rows, :max_rows, :decode))
            if nentries == tablecap
                newcap = checked_mul(2, tablecap)
                reserve!(r.budget, blocktablecharge(newcap))
                replacement = Vector{BlockEntry}(undef, newcap)
                copyto!(replacement, 1, entries, 1, nentries)
                release!(r.budget, blocktablecharge(tablecap))
                entries = replacement
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
function blockworstcase(limits::Limits, e::BlockEntry, cols::Vector{Type})
    chunk = 0
    for E in cols
        chunk = checked_add(chunk, vectorbytes(E, e.count))
    end
    return checked_add(checked_add(checked_add(e.size, limits.max_block_bytes), limits.max_codec_memory),
                       checked_add(checked_add(chunk, limits.max_block_output_bytes), SCRATCH_STATE_MAX))
end

"A per-block budget: the block's own caps and counters under a ceiling of exactly `W`."
function blockbudget(limits::Limits, W::Int)
    return Budget(limits, W, :decode, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  min(limits.max_total_values, limits.work_allowance))
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

ParallelStats() = ParallelStats(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

"The last operation's parallel counters (test and gate introspection only)."
const LAST_PARALLEL_STATS = Ref{Any}(nothing)

"Test-only schedule forcing: `PARALLEL_HOOK[] = (event, index) -> ...` (`:admitted`, `:headdone`, `:workerstart`, `:workerdone`, `:commit`)."
const PARALLEL_HOOK = Ref{Any}(nothing)

phook(event::Symbol, index::Int) = (h = PARALLEL_HOOK[]; h === nothing || h(event, index); nothing)

# ---- direct decode (the sequential rule, head task and ntasks = 1) -----------------------------------

maketyped(plan::P, data::Vector{E}, base::Int) where {P<:ReadPlan,E} = TypedColumn{E,P}(plan, data, base)

"Builders that decode a block directly into the final columns at its prefix-sum offset (no chunks)."
function directbuilders(p::RecordPlan, sel::Union{Nothing,Vector{Int}}, finals::Vector{AbstractVector}, keptidx::Vector{Int}, base::Int)
    cols = Vector{ColumnBuilder}(undef, length(p.fields))
    for i in eachindex(p.fields)
        pos = findfirst(==(i), keptidx)
        if pos === nothing
            cols[i] = SkipColumn(p.fields[i])
        else
            cols[i] = maketyped(p.fields[i], finals[pos], base)
        end
    end
    return cols
end

function directbuilders(p::ResolvedRecordPlan, sel::Union{Nothing,Vector{Int}}, finals::Vector{AbstractVector}, keptidx::Vector{Int}, base::Int)
    n = length(p.schema.fields)
    plans = Vector{Union{Nothing,ReadPlan}}(nothing, n)
    for (slot, plan) in p.steps
        slot == 0 || (plans[slot] = plan)
    end
    for (slot, dp) in p.defaults
        plans[slot] = dp
    end
    cols = Vector{ColumnBuilder}(undef, n)
    for i in 1:n
        pos = findfirst(==(i), keptidx)
        if pos === nothing
            cols[i] = SkipColumn(plans[i])
        else
            cols[i] = maketyped(plans[i], finals[pos], base)
        end
    end
    return cols
end

"Decode one pre-scanned block under the sequential rule (the main budget, actual-size reservations)."
function decodedirect!(r::Reader, e::BlockEntry, plan, builders::Vector{ColumnBuilder}, slotrow::Int)
    b = r.budget
    src = r.source::BytesSource
    addblocks!(b)
    payload = view(src.buf, e.offset:e.offset + e.size - 1)
    addinput!(b, varintlength(e.count) + varintlength(e.size) + 16)
    out = decompressblock(r.codecname, r.codec, payload, r.limits, b)
    n = e.count
    if r.validate === :strict && r.legacy === :avrojl1
        d0 = Decoder(out, b)
        for _ in 1:n
            skip(r.plan, d0)
        end
        if d0.pos != length(out) + 1
            if r.legacy === :avrojl1 && r.codecname === :null
                r.warned || (@warn "accepting trailing bytes after $(n) datums in a null-codec block (legacy=:avrojl1; Avro.jl ≤ 1.1.2 sizing cushion)" source = 1; r.warned = true)
                resize!(out, d0.pos - 1)
            else
                throw(DataError("block $(e.index) declares $n datums but they consume $(d0.pos - 1) of $(length(out)) bytes", d0.pos))
            end
        end
    end
    d = Decoder(out, b; validate=r.validate)
    cells = plan isa RecordPlan ? fuseskips(builders) : builders
    outputbase = b.reserved
    cap = r.limits.max_block_output_bytes
    for done in 1:n
        countvalues!(b)
        decoderow!(cells, d, plan)
        checkblockoutput(b, outputbase, done, slotrow, cap)
    end
    d.pos == length(out) + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
    addrows!(b, n)
    release!(b, bytesbytes(length(out)))
    return nothing
end

# ---- worker decode ----------------------------------------------------------------------------------

"Decode one admitted higher block into per-block chunk columns under its own full reservation."
function decodejob!(job::BlockJob, r::Reader, plan, sel::Union{Nothing,Vector{Int}}, fail::FailBox, slotrow::Int)
    e = job.entry
    if (@atomic fail.idx) < e.index
        @atomic job.state = :abandoned
        notify(job.done)
        return nothing
    end
    phook(:workerstart, e.index)
    b = job.budget
    try
        src = r.source::BytesSource
        payload = view(src.buf, e.offset:e.offset + e.size - 1)
        cname, codec = readercodec(String(r.codecname), r.limits, r.legacy)
        addinput!(b, varintlength(e.count) + varintlength(e.size) + 16)
        out = decompressblock(cname, codec, payload, r.limits, b)
        n = e.count
        if r.validate === :strict && r.legacy === :avrojl1
            d0 = Decoder(out, b)
            for _ in 1:n
                skip(r.plan, d0)
            end
            d0.pos == length(out) + 1 ||
                throw(DataError("block $(e.index) declares $n datums but they consume $(d0.pos - 1) of $(length(out)) bytes", d0.pos))
        end
        cols = columnbuilders(plan, sel, n, b)
        cells = plan isa RecordPlan ? fuseskips(cols) : cols
        d = Decoder(out, b; validate=r.validate)
        outputbase = b.reserved
        cap = r.limits.max_block_output_bytes
        blockout = 0
        for done in 1:n
            countvalues!(b)
            decoderow!(cells, d, plan)
            blockout = checkblockoutput(b, outputbase, done, slotrow, cap)
        end
        d.pos == length(out) + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
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

function workerloop(ch::Channel{BlockJob}, r::Reader, plan, sel::Union{Nothing,Vector{Int}}, fail::FailBox, slotrow::Int)
    for job in ch
        decodejob!(job, r, plan, sel, fail, slotrow)
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
        release!(b, job.W)
        worstowned = false
        reserve!(b, retained)                        # the block's retained chunks and payload
        retainedowned = true
        addblocks!(b)
        addinput!(b, job.budget.input_bytes)
        countvalues!(b, job.budget.values)
        addcompare!(b, job.budget.compare_bytes)
        b.members = checked_add(b.members, job.budget.members)
        addrows!(b, e.count)
        job.outputbytes <= r.limits.max_block_output_bytes ||
            throw(LimitError(:max_block_output_bytes, job.outputbytes, r.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
        chunkcap = 0
        cols = job.cols::Vector{ColumnBuilder}
        for (pos, i) in enumerate(keptidx)
            c = cols[i]::TypedColumn
            copyto!(finals[pos], e.rowstart, c.data, 1, e.count)
            moved = vectorbytes(eltype(c.data), e.count)
            chunkcap = checked_add(chunkcap, moved)
            stats.assembly_bytes = checked_add(stats.assembly_bytes, moved)
        end
        stats.committed_bytes = checked_add(stats.committed_bytes, retained)
        stats.jobpeakmax = max(stats.jobpeakmax, job.budget.peak)
        job.budget.peak <= job.W || (stats.peak_violations += 1)
        release!(b, chunkcap)                        # references moved; payload transfers, counted once
        close!(job.budget)
        committed = true
        return nothing
    finally
        if !committed
            worstowned && release!(b, job.W)
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

"""
Decode a pre-scanned byte source into the preallocated finals: the direct head under the sequential
rule, higher blocks in order into headroom, ordered cumulative commits, lowest failing index wins.
"""
function decodeblocks!(r::Reader, plan, sel::Union{Nothing,Vector{Int}}, finals::Vector{AbstractVector},
                       keptidx::Vector{Int}, cols::Vector{Type}, pre::PrescanResult, ntasks::Int)
    entries = pre.entries
    nblocks = length(entries)
    stats = ParallelStats()
    LAST_PARALLEL_STATS[] = stats
    slotrow = 0
    for E in cols
        slotrow = checked_add(slotrow, slotbytes(E))
    end
    b = r.budget
    inflightcap = r.limits.max_inflight_blocks == 0 ? ntasks - 1 : min(r.limits.max_inflight_blocks, ntasks - 1)
    nworkers = min(ntasks - 1, Threads.nthreads() - 1, inflightcap, max(nblocks - 1, 0))
    poolstate = 0
    if nworkers > 0 && r.legacy === nothing          # legacy tolerance mutates reader state: direct path only
        Whead = blockworstcase(r.limits, entries[1], cols)
        W2 = blockworstcase(r.limits, entries[2], cols)
        poolstate = WORKER_STATE * nworkers          # admission arithmetic only: acceptance stays sequential-identical
        checked_add(checked_add(b.reserved, Whead), checked_add(W2, poolstate)) <= b.ceiling || (nworkers = 0)
    else
        nworkers = 0
    end
    stats.nworkers = nworkers
    if nworkers == 0
        for e in entries                             # the ntasks = 1 direct path
            builders = directbuilders(plan, sel, finals, keptidx, e.rowstart - 1)
            decodedirect!(r, e, plan, builders, slotrow)
        end
        pre.pending === nothing || throw(pre.pending)
        finalcounters!(stats, b)
        return stats
    end
    fail = FailBox(typemax(Int))
    poolstate = checked_add(poolstate, STORAGE[].vector + 8 * nblocks)   # the jobs vector is pool state
    reserve!(b, poolstate)                             # the pool is charged once, before it exists, for
    poolalive = true                                   # its whole lifetime (§4.4 (d), round-2 D05)
    jobs = Vector{Union{Nothing,BlockJob}}(nothing, nblocks)
    ch = Channel{BlockJob}(nblocks)
    workers = Task[Threads.@spawn workerloop(ch, r, plan, sel, fail, slotrow) for _ in 1:nworkers]
    foreach(errormonitor, workers)
    retirepool! = function ()                          # the barrier drains every wave, so in-flight is
        poolalive || return nothing                    # always zero here: the pool can retire without
        close(ch)                                      # reordering commits, and the sequential tail runs
        for t in workers                               # with the pool's memory genuinely released
            wait(t)
        end
        release!(b, poolstate)
        poolalive = false
        return nothing
    end
    inflight = 0
    next = 1
    tocommit = 1
    try
        while tocommit <= nblocks
            head = entries[next]
            headidx = next
            next += 1
            Whead = blockworstcase(r.limits, head, cols)
            poolalive && checked_add(b.reserved, Whead) > b.ceiling && retirepool!()   # never crowd the head
            while poolalive && next <= nblocks && inflight < inflightcap
                e2 = entries[next]
                Wi = blockworstcase(r.limits, e2, cols)
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
            builders = directbuilders(plan, sel, finals, keptidx, head.rowstart - 1)
            try
                decodedirect!(r, head, plan, builders, slotrow)
            catch
                recordfailure!(fail, headidx)                          # queued higher blocks abandon
                rethrow()
            end
            tocommit = headidx + 1
            phook(:headdone, headidx)
            while tocommit <= nblocks && jobs[tocommit] !== nothing    # the admission-wave barrier
                job = jobs[tocommit]::BlockJob
                wait(job.done)
                inflight -= 1
                st = @atomic job.state
                st === :done || throw(job.err::Exception)              # the lowest uncommitted block's own failure
                jobs[tocommit] = nothing                               # commitjob! owns every reservation from here
                try
                    commitjob!(r, job, finals, keptidx, stats)
                catch
                    recordfailure!(fail, job.entry.index)
                    rethrow()
                end
                tocommit += 1
            end
            next = tocommit
        end
        pre.pending === nothing || throw(pre.pending)
        finalcounters!(stats, b)
        return stats
    catch err
        recordfailure!(fail, nblocks + 1)                              # abandon everything still queued
        rethrow()
    finally
        if poolalive
            close(ch)
            for t in workers
                wait(t)
            end
        end
        for job in jobs                                                # uncommitted reservations
            job === nothing && continue
            st = @atomic job.state
            st === :done && (stats.speculative_decoded += 1)
            release!(b, job.W)
            close!(job.budget)
        end
        if poolalive
            release!(b, poolstate)
            poolalive = false
        end
    end
end
