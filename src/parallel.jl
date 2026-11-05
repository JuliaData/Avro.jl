# Parallel container decode for `Avro.Table` over byte and mapped sources: block headers are pre-scanned
# into a block table, the final columns are preallocated at the claimed row count, and every block
# decodes directly at its row offset. Workers take block indices in order; the lowest failing index's
# error is the one raised, so results, acceptance and errors are identical at every `ntasks`. Workers stop
# taking blocks once what they decompressed exceeds what the blocks being read may inflate to, so a read never
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

"""
Walk the block headers without decompressing, up to the block that holds file row `lastrow` (a bad header becomes
the pending failure).
"""
function prescanblocks(r::Reader, lastrow::Int=typemax(Int))
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
            lastrow < typemax(Int) && rows >= lastrow && break
        end
    catch e
        (e isa DataError || e isa LimitError) || rethrow()
        pending = e
    end
    src.pos = startpos
    return PrescanResult(entries, rows, pending)
end

"""
The pre-scanned blocks that hold file rows `firstrow:lastrow`, with row offsets from the first of them, and the file
rows before that block. A header fault is met only by a window that reaches past the complete blocks.
"""
function windowblocks(pre::PrescanResult, firstrow::Int, lastrow::Int)
    pending = lastrow > pre.totalrows ? pre.pending : nothing
    firstrow == 1 && lastrow >= pre.totalrows && return (PrescanResult(pre.entries, pre.totalrows, pending), 0)
    entries = BlockEntry[]
    base = pre.totalrows
    for e in pre.entries
        e.rowstart + e.count - 1 < firstrow && continue
        e.rowstart > lastrow && break
        isempty(entries) && (base = e.rowstart - 1)
        push!(entries, BlockEntry(e.index, e.offset, e.size, e.count, e.rowstart - base))
    end
    rows = isempty(entries) ? 0 : entries[end].rowstart + entries[end].count - 1
    return (PrescanResult(entries, rows, pending), base)
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
A decoder over one pre-scanned block, the `i`th the read decodes, its decompressed size recorded in `sizes[i]`: an
uncompressed block in a `Vector` is read in place, since decoded values never alias their input.
"""
function blockdecoder(r::Reader, e::BlockEntry, sizes::Vector{Int}, i::Int)
    src = r.source::BytesSource
    stop = e.offset + e.size - 1
    if r.codecname === :null && r.legacy === nothing && src.buf isa Vector{UInt8}
        sizes[i] = e.size
        return blockcap!(Decoder(src.buf, r.limits; pos=e.offset, stop=stop))
    end
    decoded = decompressblock(r.codecname, view(src.buf, e.offset:stop), r.limits)
    sizes[i] = length(decoded)
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

# A generic table's block decoder: builders over the final columns. Each worker decodes with its own builders.
struct BuilderBlocks{P}
    plan::P
    projection::Union{Nothing,Projection}
    finals::Vector{Any}                      # each column's `finalstorage`
    cols::Vector{ColumnBuilder}
    cells::Vector{ColumnBuilder}
end

function BuilderBlocks(plan, projection::Union{Nothing,Projection}, finals::Vector{Any})
    cols = directbuilders(plan, projection, finals)
    return BuilderBlocks(plan, projection, finals, cols, rowcells(plan, cols))
end

# The block decoder a worker uses, given the one the read was set up with.
function workerblocks(b::BuilderBlocks)
    return BuilderBlocks(b.plan, b.projection, b.finals)
end

"Decode one pre-scanned block from its decoder `d` straight into the final columns at its row offset."
function decodeblock!(b::BuilderBlocks, e::BlockEntry, d::Decoder)
    e.count == 0 && return checkconsumed(d)
    for c in b.cols
        c isa TypedColumn && (c.len = e.rowstart - 1)
    end
    return decoderows!(b.plan, b.cells, d, e.count)
end

# A typed table's block decoder, which workers share: each row decodes into `T` and its fields go to the columns.
struct TypedBlocks{T,C<:Tuple}
    node::TypedNode
    columns::C
end

function TypedBlocks{T}(node::TypedNode, columns::C) where {T,C<:Tuple}
    return TypedBlocks{T,C}(node, columns)
end

function workerblocks(b::TypedBlocks)
    return b
end

function decodeblock!(b::TypedBlocks{T}, e::BlockEntry, d::Decoder) where {T}
    for row in e.rowstart:(e.rowstart + e.count - 1)
        setrow!(b.columns, row, decodenode(T, b.node, d))
    end
    return checkconsumed(d)
end

# One worker of a parallel decode, decoding with its `workerblocks` of `blocks`. Every block it takes, it
# decompresses and decodes, so the blocks handed out stay a decoded prefix. Past `cap`, the most the blocks being
# read may inflate to, no further block is handed out.
struct BlockWorker{B}
    state::WorkerState
    r::Reader
    entries::Vector{BlockEntry}
    errors::Vector{Union{Nothing,Exception}}
    sizes::Vector{Int}
    cap::Int
    blocks::B
end

@noinline function (w::BlockWorker)()
    blocks = workerblocks(w.blocks)
    n = length(w.entries)
    while true
        i = @atomic w.state.taken += 1
        i > n && break
        (@atomic w.state.failed) < i && break                  # a lower block already failed; every later index is higher
        try
            e = w.entries[i]
            d = blockdecoder(w.r, e, w.sizes, i)
            (@atomic w.state.inflated += w.sizes[i]) > w.cap && (@atomic w.state.taken = n)
            decodeblock!(blocks, e, d)
        catch err
            w.errors[i] = err::Exception
            recordfailure!(w.state, i)
            break
        end
    end
    return nothing
end

function decodeparallel!(r::Reader, blocks, entries::Vector{BlockEntry}, nworkers::Int, sizes::Vector{Int})
    state = WorkerState(0, typemax(Int), r.decompressed)
    errors = Vector{Union{Nothing,Exception}}(nothing, length(entries))
    compressed = r.compressed
    for e in entries
        compressed = satadd(compressed, e.size)
    end
    cap = inflationcap(r.limits, compressed)
    worker = BlockWorker(state, r, entries, errors, sizes, cap, blocks)
    workers = Task[spawntask(worker) for _ in 1:nworkers]
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
Decode a pre-scanned byte source into preallocated final columns with the block decoder `blocks`, in parallel
when `ntasks` and the thread count allow (legacy tolerance mutates reader state, so it decodes sequentially).
"""
function decodeblocks!(r::Reader, blocks, pre::PrescanResult, ntasks::Int)
    entries = pre.entries
    sizes = fill(-1, length(entries))
    nworkers = min(ntasks, Threads.nthreads(), length(entries))
    if nworkers > 1 && r.legacy === nothing
        decodeparallel!(r, blocks, entries, nworkers, sizes)
    else
        for (i, e) in enumerate(entries)
            d = blockdecoder(r, e, sizes, i)
            inflate!(r, e.size, sizes[i])
            decodeblock!(blocks, e, d)
        end
    end
    pre.pending === nothing || throw(pre.pending)
    return nothing
end
