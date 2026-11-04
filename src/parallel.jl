# Parallel container decode for `Avro.Table` over byte and mapped sources: block headers are pre-scanned
# into a block table, the final columns are preallocated at the claimed row count, and every block
# decodes directly at its row offset. Workers take block indices in order; the lowest failing index's
# error is the one raised, so results, acceptance and errors are identical at every `ntasks`. Workers stop
# taking blocks once what they decompressed exceeds what the whole file may inflate to, so a read never
# decompresses more than that bound plus one block per worker.

"One pre-scanned block: payload location, framing, and its prefix-sum row offset."
struct BlockEntry
    index::Int
    offset::Int          # first payload byte in the source buffer
    size::Int            # compressed payload size
    count::Int           # declared datum count
    rowstart::Int        # 1-based row offset of the block's first datum
end

struct PrescanResult
    entries::Vector{BlockEntry}
    totalrows::Int
    pending::Union{Nothing,Exception}   # a header fault met while scanning; raised after the complete blocks decode
end

"Walk the block headers without decompressing (a bad header becomes the pending failure)."
function prescanblocks(r::Reader)
    src = r.source::BytesSource
    entries = BlockEntry[]
    rows = 0
    pending = nothing
    startpos = src.pos
    try
        while !sourceeof(src)
            count = sourcevarint(src)
            count >= 0 || throw(DataError("negative block count $count", position(src)))
            size = sourcevarint(src)
            size >= 0 || throw(DataError("negative block size $size", position(src)))
            size <= r.limits.max_bytes || throw(LimitError(:max_bytes, size, r.limits.max_bytes))
            off = src.pos
            Int(size) <= src.stop - off + 1 || throw(DataError("truncated file", off))
            src.pos = off + Int(size)
            checksync(src, r.sync, length(entries) + 1)
            blockrows = Int(count)
            blockrows > typemax(Int) - rows &&
                throw(DataError("cumulative block row count exceeds the platform Int range", src.pos))
            push!(entries, BlockEntry(length(entries) + 1, off, Int(size), blockrows, rows + 1))
            rows += blockrows
        end
    catch e
        (e isa DataError || e isa LimitError) || rethrow()
        pending = e
    end
    src.pos = startpos
    return PrescanResult(entries, rows, pending)
end

"What the workers of one parallel decode share."
mutable struct WorkerState
    @atomic taken::Int       # the last block index handed out
    @atomic failed::Int      # the lowest block index that failed so far (`typemax(Int)` while none has)
    @atomic inflated::Int    # the read's decompressed bytes so far
end

function recordfailure!(state::WorkerState, index::Int)
    while true
        cur = @atomic state.failed
        index >= cur && return nothing
        (@atomicreplace state.failed cur => index).success && return nothing
    end
end

# The cells a row of `plan` decodes into: a record plan's fused builders, otherwise the builders themselves.
function rowcells(plan, cols::Vector{ColumnBuilder})
    return plan isa RecordPlan ? fuseskips(cols) : cols
end

"Decode `count` records of `plan` into `rowcells` of its builders and require the block to be consumed exactly."
function decoderows!(plan, cells::Vector{ColumnBuilder}, d::Decoder, count::Int)
    if plan isa RecordPlan
        for _ in 1:count
            decoderow!(cells, d)
        end
    else
        for _ in 1:count
            decoderow!(cells, d, plan)
        end
    end
    return checkconsumed(d)
end

"""
A decoder over one pre-scanned block, its decompressed size recorded in `sizes`: an uncompressed block in a
`Vector` is read in place, since decoded values never alias their input.
"""
function blockdecoder(r::Reader, e::BlockEntry, sizes::Vector{Int})
    src = r.source::BytesSource
    stop = e.offset + e.size - 1
    if r.codecname === :null && r.legacy === nothing && src.buf isa Vector{UInt8}
        sizes[e.index] = e.size
        return blockcap!(Decoder(src.buf, r.limits; pos=e.offset, stop=stop))
    end
    decoded = decompressblock(r.codecname, view(src.buf, e.offset:stop), r.limits)
    sizes[e.index] = length(decoded)
    return blockcap!(Decoder(exactblock(r, decoded, e.count, e.index), r.limits))
end

"""
Charge blocks `1:upto` to the read's inflation budget in file order, stopping at the first block that was not
decompressed. Workers decode blocks in any order, so the budget is charged afterwards, in order, which keeps
the outcome independent of `ntasks`. When the workers stopped on the whole-file bound, the blocks they took
are a prefix of the file, and the charge fails within it unless one of its blocks failed first.
"""
function inflateinorder!(r::Reader, entries::Vector{BlockEntry}, sizes::Vector{Int}, upto::Int)
    for i in 1:min(upto, length(entries))
        sizes[i] < 0 && break
        inflate!(r, entries[i].size, sizes[i])
    end
    return nothing
end

"Decode one pre-scanned block from its decoder `d` straight into the finals at its row offset."
function decodeentry!(r::Reader, plan, cols::Vector{ColumnBuilder}, cells::Vector{ColumnBuilder}, e::BlockEntry,
                      d::Decoder)
    e.count == 0 && return checkconsumed(d)
    for c in cols
        c isa TypedColumn && (c.len = e.rowstart - 1)
    end
    return decoderows!(plan, cells, d, e.count)
end

# Every block a worker takes, it decompresses and decodes, so the blocks handed out stay a decoded prefix.
# Past `cap`, the most the whole file may inflate to, no further block is handed out.
function decodeworker!(state::WorkerState, r::Reader, plan, projection::Union{Nothing,Projection},
                       finals::Vector{AbstractVector}, entries::Vector{BlockEntry},
                       errors::Vector{Union{Nothing,Exception}}, sizes::Vector{Int}, cap::Int)
    cols = directbuilders(plan, projection, finals)           # builders are per worker
    cells = rowcells(plan, cols)
    n = length(entries)
    while true
        i = @atomic state.taken += 1
        i > n && break
        (@atomic state.failed) < i && break                    # a lower block already failed; every later index is higher
        try
            e = entries[i]
            d = blockdecoder(r, e, sizes)
            (@atomic state.inflated += sizes[i]) > cap && (@atomic state.taken = n)
            decodeentry!(r, plan, cols, cells, e, d)
        catch err
            errors[i] = err
            recordfailure!(state, i)
            break
        end
    end
    return nothing
end

function decodeparallel!(r::Reader, plan, projection::Union{Nothing,Projection}, finals::Vector{AbstractVector},
                         entries::Vector{BlockEntry}, nworkers::Int, sizes::Vector{Int})
    state = WorkerState(0, typemax(Int), r.decompressed)
    errors = Vector{Union{Nothing,Exception}}(nothing, length(entries))
    compressed = r.compressed
    for e in entries
        compressed = satadd(compressed, e.size)
    end
    cap = inflationcap(r.limits, compressed)
    workers = Task[errormonitor(Threads.@spawn decodeworker!(state, r, plan, projection, finals, entries, errors, sizes, cap))
                   for _ in 1:nworkers]
    try
        for t in workers
            wait(t)
        end
    catch
        recordfailure!(state, 0)                               # interrupted: no worker may outlive the source
        for t in workers
            istaskdone(t) || try wait(t) catch end
        end
        rethrow()
    end
    index = @atomic state.failed
    inflateinorder!(r, entries, sizes, index)
    index <= length(entries) && throw(errors[index]::Exception)
    return nothing
end

"""
Decode a pre-scanned byte source into the preallocated finals, in parallel when `ntasks` and the thread
count allow (legacy tolerance mutates reader state, so it decodes sequentially).
"""
function decodeblocks!(r::Reader, plan, projection::Union{Nothing,Projection}, finals::Vector{AbstractVector},
                       pre::PrescanResult, ntasks::Int)
    entries = pre.entries
    sizes = fill(-1, length(entries))
    nworkers = min(ntasks, Threads.nthreads(), length(entries))
    if nworkers > 1 && r.legacy === nothing
        decodeparallel!(r, plan, projection, finals, entries, nworkers, sizes)
    else
        cols = directbuilders(plan, projection, finals)
        cells = rowcells(plan, cols)
        for e in entries
            d = blockdecoder(r, e, sizes)
            inflate!(r, e.size, sizes[e.index])
            decodeentry!(r, plan, cols, cells, e, d)
        end
    end
    pre.pending === nothing || throw(pre.pending)
    return nothing
end
