# Limits, budgets and the memory ceiling.
#
# One fixed-default, portable per-operation ceiling bounds package-owned memory through reservations made
# before the corresponding allocation; per-block and per-value limits keep that memory proportional to
# the input, and work is bounded as a function of input bytes (the value rule). Every `LimitError` names
# the limit, the observed value and the keyword that raises it.

const KiB = 1 << 10
const MiB = 1 << 20
const GiB = 1 << 30

"""
    Avro.Limits(; kwargs...)

Fixed-default, portable resource limits. Every field is an `Int ≥ 0`; the constructor validates the
cross-field relations and raises `ArgumentError` otherwise. The same limits must be raised
on every side that processes the data (writers enforce every limit readers enforce).

Fields (defaults): `max_depth` (1024), `max_bytes` (64 MiB), `max_datum_bytes` (64 MiB),
`max_json_depth` (1024), `max_block_bytes` (16 MiB), `max_block_count` (2^24),
`max_block_output_bytes` (64 MiB), `max_blocks` (unlimited), `max_codec_memory` (32 MiB),
`max_total_bytes` (16 GiB), `max_total_values` (unlimited), `max_rows` (unlimited), `max_values_per_byte` (16),
`work_allowance` (65,536), `max_resolution_work` (1,000,000),
`max_schema_bytes` (16 MiB), `max_schema_depth` (256), `max_schema_nodes` (1,000,000), `max_fields`
(65,535), `max_union_branches` (1,024), `max_enum_symbols` (65,535), `max_name_bytes` (1,024),
`max_named_types` (10,000), `max_metadata_bytes` (16 MiB), `max_metadata_entries` (10,000),
`max_inflight_blocks` (0 = `ntasks − 1`).
"""
struct Limits
    max_depth::Int
    max_bytes::Int
    max_datum_bytes::Int
    max_json_depth::Int
    max_block_bytes::Int
    max_block_count::Int
    max_block_output_bytes::Int
    max_blocks::Int
    max_codec_memory::Int
    max_total_bytes::Int
    max_total_values::Int
    max_rows::Int
    max_values_per_byte::Int
    work_allowance::Int
    max_resolution_work::Int
    max_schema_bytes::Int
    max_schema_depth::Int
    max_schema_nodes::Int
    max_fields::Int
    max_union_branches::Int
    max_enum_symbols::Int
    max_name_bytes::Int
    max_named_types::Int
    max_metadata_bytes::Int
    max_metadata_entries::Int
    max_inflight_blocks::Int
end

const LIMIT_DEFAULTS = (
    max_depth = 1024,
    max_bytes = 64 * MiB,
    max_datum_bytes = 64 * MiB,
    max_json_depth = 1024,
    max_block_bytes = 16 * MiB,
    max_block_count = 1 << 24,
    max_block_output_bytes = 64 * MiB,
    max_blocks = typemax(Int),
    max_codec_memory = 32 * MiB,
    max_total_bytes = 16 * GiB,
    max_total_values = typemax(Int),
    max_rows = typemax(Int),
    max_values_per_byte = 16,
    work_allowance = 65_536,
    max_resolution_work = 1_000_000,
    max_schema_bytes = 16 * MiB,
    max_schema_depth = 256,
    max_schema_nodes = 1_000_000,
    max_fields = 65_535,
    max_union_branches = 1_024,
    max_enum_symbols = 65_535,
    max_name_bytes = 1_024,
    max_named_types = 10_000,
    max_metadata_bytes = 16 * MiB,
    max_metadata_entries = 10_000,
    max_inflight_blocks = 0,
)

const MIN_CODEC_MEMORY = 16 * MiB   # bzip2 (≤ 3.7 MiB), xz preset 6 (8.06 MiB) and zstd ≤ level 19 frames all decode

const INFINITE = typemax(Int)

"Add two non-negative sizes and saturate at the largest representable Int."
function satadd(a::Int, b::Int)
    a >= 0 && b >= 0 || throw(ArgumentError("saturating sizes must be non-negative"))
    (a == INFINITE || b == INFINITE || a > INFINITE - b) && return INFINITE
    return a + b
end

function limitvalue(name::Symbol, value)
    try
        return Int(value)
    catch err
        err isa Union{InexactError,OverflowError,TypeError,MethodError} || rethrow()
        throw(ArgumentError("Avro.Limits: `$name` must be an Int-compatible integer"))
    end
end

function Limits(; kwargs...)
    isempty(kwargs) && return DEFAULT_LIMITS          # every one-shot call defaults to this
    for k in keys(kwargs)
        haskey(LIMIT_DEFAULTS, k) || throw(ArgumentError("unknown Avro.Limits keyword `$k`"))
    end
    names = keys(LIMIT_DEFAULTS)
    vals = ntuple(i -> limitvalue(names[i], get(kwargs, names[i], LIMIT_DEFAULTS[i])),
                  length(LIMIT_DEFAULTS))
    limits = Limits(vals...)
    validate(limits)
    return limits
end

function validate(l::Limits)
    for (i, name) in enumerate(fieldnames(Limits))
        v = getfield(l, i)
        v >= 0 || throw(ArgumentError("Avro.Limits: `$name` must be ≥ 0, got $v"))
    end
    l.max_codec_memory >= MIN_CODEC_MEMORY ||
        throw(ArgumentError("Avro.Limits: `max_codec_memory` must be ≥ 16 MiB (got $(l.max_codec_memory)): bzip2, xz preset 6 and default zstandard frames need that much decoder memory"))
    datumcap = satadd(l.max_bytes, MiB)
    l.max_datum_bytes <= datumcap ||
        throw(ArgumentError("Avro.Limits: `max_datum_bytes` ($(l.max_datum_bytes)) must be ≤ `max_bytes` + 1 MiB ($datumcap)"))
    quarter = l.max_total_bytes ÷ 4
    half = l.max_total_bytes ÷ 2
    l.max_block_bytes <= quarter ||
        throw(ArgumentError("Avro.Limits: `max_block_bytes` ($(l.max_block_bytes)) must be ≤ `max_total_bytes` ÷ 4 ($quarter)"))
    codeccap = satadd(l.max_codec_memory, 4 * MiB)
    codeccap <= quarter ||
        throw(ArgumentError("Avro.Limits: `max_codec_memory` + 4 MiB ($codeccap) must be ≤ `max_total_bytes` ÷ 4 ($quarter)"))
    l.max_block_output_bytes <= half ||
        throw(ArgumentError("Avro.Limits: `max_block_output_bytes` ($(l.max_block_output_bytes)) must be ≤ `max_total_bytes` ÷ 2 ($half)"))
    metadatacap = satadd(l.max_metadata_bytes, l.max_schema_bytes)
    metadatacap <= half ||
        throw(ArgumentError("Avro.Limits: `max_metadata_bytes` + `max_schema_bytes` ($metadatacap) must be ≤ `max_total_bytes` ÷ 2 ($half)"))
    first = first_unit_bytes(l)
    first <= l.max_total_bytes ||
        throw(ArgumentError("Avro.Limits: two `max_block_bytes` + `max_codec_memory` + 4 MiB + `max_datum_bytes` ($first) must be ≤ `max_total_bytes` ($(l.max_total_bytes)): one block and one datum must fit"))
    return l
end

function checked_add(a::Int, b::Int)
    return Base.Checked.checked_add(a, b)
end

function checked_mul(a::Int, b::Int)
    return Base.Checked.checked_mul(a, b)
end

"Compute `multiplier * input + allowance`, saturating non-negative caps at `typemax(Int)`."
function muladdcap(multiplier::Int, input::Int, allowance::Int)
    multiplier >= 0 || throw(ArgumentError("cap multiplier must be non-negative"))
    input >= 0 || throw(ArgumentError("cap input must be non-negative"))
    allowance >= 0 || throw(ArgumentError("cap allowance must be non-negative"))
    (multiplier == 0 || input == 0) && return allowance
    available = typemax(Int) - allowance
    input > available ÷ multiplier && return typemax(Int)
    return multiplier * input + allowance
end

function Base.show(io::IO, l::Limits)
    return print(io, "Avro.Limits(max_total_bytes=", l.max_total_bytes, ", …)")
end

function Base.show(io::IO, ::MIME"text/plain", l::Limits)
    println(io, "Avro.Limits:")
    for name in fieldnames(Limits)
        println(io, "  ", name, " = ", getfield(l, name))
    end
    return nothing
end

"""
    first_unit_bytes(limits)

The memory the first unit of progress of any operation may need: one compressed block, its decompressed
buffer, the codec workspace plus 4 MiB of fixed overhead, and one datum.
"""
function first_unit_bytes(l::Limits)
    return satadd(satadd(satadd(l.max_block_bytes, l.max_block_bytes),
                         satadd(l.max_codec_memory, 4 * MiB)),
                  l.max_datum_bytes)
end

const DEFAULT_LIMITS = validate(Limits(values(LIMIT_DEFAULTS)...))

# ---- per-operation budget -------------------------------------------------------------------------

"""
    Budget(limits; direction=:decode)

The accumulator of one operation: live reservations against the `max_total_bytes` ceiling, the value
work counter, rows, blocks, codec members, resolution work and the shared allowance.
"""
mutable struct Budget
    const limits::Limits
    const ceiling::Int
    const direction::Symbol
    reserved::Int
    peak::Int
    values::Int
    input_bytes::Int
    rows::Int
    blocks::Int
    members::Int
    resolution_work::Int
    allowance_used::Int     # the largest draw on work_allowance so far (maintained when input arrives)
    pending::Int            # reserved bytes not yet allocated
    workcap::Int            # min(max_total_values, max_values_per_byte × input_bytes + work_allowance)
    workdeferred::Int       # root encode transactions whose exact byte denominator is not known yet
    default_position::Int   # reusable cursor into a resolved default's immutable branch tape
end

function Budget(limits::Limits; direction::Symbol=:decode)
    direction in (:decode, :encode) || throw(ArgumentError("direction must be :decode or :encode"))
    return Budget(limits, limits.max_total_bytes, direction, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  min(limits.max_total_values, limits.work_allowance), 0, 0)
end

"A persistent table ledger whose explicit byte ceiling is independent of operation-limit relations."
function ledgerbudget(ceiling::Int)
    ceiling >= 0 || throw(ArgumentError("ledger ceiling must be non-negative"))
    limits = Limits()
    return Budget(limits, ceiling, :decode, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  min(limits.max_total_values, limits.work_allowance), 0, 0)
end

function limiterror(b::Budget, limit::Symbol, observed::Int, value::Int)
    return LimitError(limit, observed, value, limit, b.direction)
end

"Enforce the shared byte bound for one bytes, string, or fixed value."
function checkvaluebytes(b::Budget, n::Int)
    n <= b.limits.max_bytes ||
        throw(limiterror(b, :max_bytes, n, b.limits.max_bytes))
    return nothing
end

"""
    reserve!(budget, n)

Reserve `n` bytes of package-owned memory before allocating them. Throws `LimitError(:max_total_bytes)`
when the reservation would exceed the operation's ceiling.
"""
function reserve!(b::Budget, n::Int)
    n >= 0 || throw(ArgumentError("reservation must be non-negative"))
    n == 0 && return b
    total = satadd(b.reserved, n)
    total == typemax(Int) && b.reserved > typemax(Int) - n &&
        throw(limiterror(b, :max_total_bytes, total, b.ceiling))
    total <= b.ceiling || throw(limiterror(b, :max_total_bytes, total, b.ceiling))
    b.reserved = total
    b.peak = max(b.peak, total)
    b.pending += n                                     # pending ≤ reserved ≤ ceiling
    return b
end

# Drop `n` bytes from the budget's pending counter. Internal: the public entry points are `allocated!`,
# `unreserve!` and `close!`, which validate their side of the contract.
function settlepending!(b::Budget, n::Int)
    n <= 0 && return b
    b.pending -= n
    return b
end

"""
    allocated!(budget, n)

Record that `n` previously reserved bytes are now resident. Settling more bytes than are pending is a
settlement mismatch and throws instead of clamping.
"""
function allocated!(b::Budget, n::Int)
    n >= 0 || throw(ArgumentError("settlement must be non-negative"))
    n == 0 && return b
    n <= b.pending || throw(ArgumentError("settlement of $n bytes exceeds the pending reservation $(b.pending)"))
    return settlepending!(b, n)
end

"""
    retain!(budget, n)

Charge `n` bytes of storage that already exists and is being retained beyond the structure that
allocated it — tree references a schema node keeps alive, or slot shifts inside prebuilt capacity.
Nothing is allocated at the call site, so the reservation settles immediately: the storage is
already resident. Storage allocated at the call site must use `reserve!` before the allocation and
`allocated!` after it instead.
"""
function retain!(b::Budget, n::Int)
    reserve!(b, n)
    allocated!(b, n)
    return b
end

"""
    release!(budget, n)

Release `n` reserved **resident** bytes (after the corresponding storage became unreachable or was
handed to the caller). Reservations that never became resident are returned with [`unreserve!`](@ref);
asking to release more than the resident portion is a settlement mismatch and throws.
"""
function release!(b::Budget, n::Int)
    n >= 0 || throw(ArgumentError("release must be non-negative"))
    n == 0 && return b
    n <= b.reserved || throw(ArgumentError("release of $n bytes exceeds the live reservation $(b.reserved)"))
    resident = b.reserved - b.pending
    n <= resident || throw(ArgumentError("release of $n bytes exceeds the resident portion $resident ($(b.pending) of $(b.reserved) reserved bytes are pending; unfulfilled reservations return through unreserve!)"))
    b.reserved -= n
    return b
end

"""
    unreserve!(budget, n)

Return `n` reserved bytes that never became resident (worst-case headroom, or the unwind of a failed
allocation) to the budget.
"""
function unreserve!(b::Budget, n::Int)
    n >= 0 || throw(ArgumentError("unreserve must be non-negative"))
    n == 0 && return b
    n <= b.pending || throw(ArgumentError("unreserve of $n bytes exceeds the pending reservation $(b.pending)"))
    settlepending!(b, n)
    b.reserved -= n
    return b
end

"""
    close!(budget)

Settle every pending reservation (called in the `finally` of every operation).
"""
function close!(b::Budget)
    settlepending!(b, b.pending)
    return b
end

"Capture the counters `rollbackreservations!` needs to unwind an operation exactly."
function budgetcheckpoint(b::Budget)
    return (b.reserved, b.pending)
end

"""
Unwind every reservation acquired after `checkpoint` (a `budgetcheckpoint`) on a failed ownership
transfer. The pending delta since the checkpoint is this operation's in-flight remainder and returns
through `unreserve!`; the settled rest — storage that dies with the failed operation — is released.
The budget may carry unrelated pending headroom, so the split must come from the checkpoint.
"""
function rollbackreservations!(budget::Budget, checkpoint::NTuple{2,Int})
    reserved0, pending0 = checkpoint
    delta = budget.reserved - reserved0
    delta <= 0 && return nothing
    pend = budget.pending - pending0
    pend > 0 && unreserve!(budget, pend)
    resident = delta - max(pend, 0)
    resident > 0 && release!(budget, resident)
    return nothing
end

"""
Construction-side exact-replacement growth: the backing vector starts at a reserved exact
capacity, grows only by reserving the doubled replacement before allocating it, and shrinks to its
exact final length in `finishbuild!`. `budget === nothing` builds without accounting (unbudgeted
public constructors account elsewhere).
"""
mutable struct BuildBuf{T}
    data::Vector{T}
    len::Int
end

function BuildBuf{T}(budget::Union{Nothing,Budget}, cap::Int) where {T}
    cap = max(cap, 0)
    checkpoint = budget === nothing ? nothing : budgetcheckpoint(budget)
    data = nothing
    try
        budget === nothing || reserve!(budget, vectorbytes(T, cap))
        data = Vector{T}(undef, cap)
        budget === nothing || allocated!(budget, vectorbytes(T, cap))
        shell = shellbytes(BuildBuf{T})
        budget === nothing || reserve!(budget, shell)
        bb = BuildBuf{T}(data, 0)
        budget === nothing || allocated!(budget, shell)
        return bb
    catch
        data = nothing
        budget === nothing || rollbackreservations!(budget, checkpoint::NTuple{2,Int})
        rethrow()
    end
end

function Base.push!(bb::BuildBuf{T}, budget::Union{Nothing,Budget}, x) where {T}
    if bb.len == length(bb.data)
        newcap = max(2 * length(bb.data), 4)
        budget === nothing || reserve!(budget, vectorbytes(T, newcap))
        nd = Vector{T}(undef, newcap)
        budget === nothing || allocated!(budget, vectorbytes(T, newcap))
        copyto!(nd, 1, bb.data, 1, bb.len)
        oldbytes = vectorbytes(T, length(bb.data))
        bb.data = nd                                   # the old storage is unreachable only after the rebind
        budget === nothing || release!(budget, oldbytes)
    end
    bb.len += 1
    @inbounds bb.data[bb.len] = x
    return bb
end

"Shrink to the exact final length by replacement and hand the backing vector to the caller."
function finishbuild!(bb::BuildBuf{T}, budget::Union{Nothing,Budget}) where {T}
    out = bb.data
    if bb.len != length(out)
        budget === nothing || reserve!(budget, vectorbytes(T, bb.len))
        out = Vector{T}(undef, bb.len)
        budget === nothing || allocated!(budget, vectorbytes(T, bb.len))
        copyto!(out, 1, bb.data, 1, bb.len)
        oldbytes = vectorbytes(T, length(bb.data))
        bb.data = out
        budget === nothing || release!(budget, oldbytes)
    end
    budget === nothing || release!(budget, shellbytes(BuildBuf{T}))
    return out
end

"Push with exact-replacement growth when the prebuilt capacity is exhausted."
function budgetedpush!(v::FrozenVector{T}, x, budget::Union{Nothing,Budget}) where {T}
    v.frozen && throw(FrozenError("vector"))
    if length(v.data) == v.cap
        newcap = max(2 * v.cap, 4)
        budget === nothing || reserve!(budget, vectorbytes(T, newcap))
        nd = Vector{T}(undef, newcap)
        budget === nothing || allocated!(budget, vectorbytes(T, newcap))
        resize!(nd, length(v.data))
        copyto!(nd, 1, v.data, 1, length(v.data))
        oldbytes = vectorbytes(T, v.cap)
        v.data = nd                                    # the old storage is unreachable only after the rebind
        v.cap = newcap
        budget === nothing || release!(budget, oldbytes)
    end
    push!(v.data, x)
    return v
end

"Sorted insert (or overwrite) with exact-replacement growth when the capacity is exhausted."
function budgetedinsert!(d::FrozenDict{K,V}, k, v, budget::Union{Nothing,Budget}) where {K,V}
    d.frozen && throw(FrozenError("dict"))
    i = searchsortedfirst(d.keys, k)
    if i <= length(d.keys) && keymatches(d.keys[i], k)
        d.vals[i] = v
        return d
    end
    length(d.keys) == d.cap && growdict!(d, budget)
    insert!(d.keys, i, convert(K, k))
    insert!(d.vals, i, convert(V, v))
    return d
end

function growdict!(d::FrozenDict{K,V}, budget::Union{Nothing,Budget}) where {K,V}
    newcap = max(2 * d.cap, 4)
    growth = vectorbytes(K, newcap) + vectorbytes(V, newcap)
    budget === nothing || reserve!(budget, growth)
    nk = Vector{K}(undef, newcap)
    nv = Vector{V}(undef, newcap)
    budget === nothing || allocated!(budget, growth)
    resize!(nk, length(d.keys))
    resize!(nv, length(d.vals))
    copyto!(nk, 1, d.keys, 1, length(d.keys))
    copyto!(nv, 1, d.vals, 1, length(d.vals))
    oldbytes = vectorbytes(K, d.cap) + vectorbytes(V, d.cap)
    d.keys = nk                                        # the old storage is unreachable only after the rebind
    d.vals = nv
    d.cap = newcap
    budget === nothing || release!(budget, oldbytes)
    return d
end

"Append without ordering; `budgetedsort!` orders the complete dictionary once (sorted inserts move O(n²) entries)."
function budgetedappend!(d::FrozenDict{K,V}, k, v, budget::Union{Nothing,Budget}) where {K,V}
    d.frozen && throw(FrozenError("dict"))
    length(d.keys) == d.cap && growdict!(d, budget)
    push!(d.keys, convert(K, k))
    push!(d.vals, convert(V, v))
    return d
end

"""
    budgetedsort!(d::FrozenDict, budget) -> Int

Sort the entries appended with `budgetedappend!` by key, with one stable merge sort. Returns 0,
or the append position of the first key that repeats an earlier key (`d` then stays unsorted).
"""
function budgetedsort!(d::FrozenDict{K,V}, budget::Union{Nothing,Budget}) where {K,V}
    d.frozen && throw(FrozenError("dict"))
    n = length(d.keys)
    n < 2 && return 0
    scratch = vectorbytes(Int32, n) + vectorbytes(Int32, cld(n, 2))
    budget === nothing || reserve!(budget, scratch)
    perm = Vector{Int32}(undef, n)
    tmp = Vector{Int32}(undef, cld(n, 2))
    budget === nothing || allocated!(budget, scratch)
    try
        mergesort!(perm, tmp, d.keys)
        dup = 0
        for i in 2:n
            keymatches(d.keys[perm[i]], d.keys[perm[i - 1]]) || continue
            dup = dup == 0 ? Int(perm[i]) : min(dup, Int(perm[i]))   # stable: perm[i] is the later one
        end
        dup == 0 || return dup
        for start in 1:n                               # apply the permutation in place, one cycle at a time
            perm[start] > 0 || continue
            key, val = d.keys[start], d.vals[start]
            i = start
            while true
                j = Int(perm[i])
                perm[i] = -perm[i]                     # negative marks a placed entry
                if j == start
                    d.keys[i] = key
                    d.vals[i] = val
                    break
                end
                d.keys[i] = d.keys[j]
                d.vals[i] = d.vals[j]
                i = j
            end
        end
        return 0
    finally
        budget === nothing || release!(budget, scratch)
    end
end

# ---- sorted-run name index -------------------------------------------------------------------------

const NAME_INDEX_RUNS = 64   # a binary counter of an Int count has at most 63 set bits

"""
    NameIndex{V}(budget)

An index of `String` keys (fullnames, aliases, admitted names) that is built one insertion at a time
with lookups in between: sorted runs whose lengths are distinct powers of two, merged like a binary
counter, so `n` insertions move `O(n log n)` entries and a lookup binary-searches at most 64 runs. No
hashing of untrusted keys; `FullName` keys find equal `String` keys (`isless` and `keymatches` compare
them). Storage is reserved against `budget` (`nothing` builds without accounting).
"""
mutable struct NameIndex{V}
    const keys::Vector{Vector{String}}   # runs, longest first
    const vals::Vector{Vector{V}}
    count::Int
end

function nameindexshellbytes(::Type{V}) where {V}
    return vectorbytes(Vector{String}, NAME_INDEX_RUNS) + vectorbytes(Vector{V}, NAME_INDEX_RUNS) +
           shellbytes(NameIndex{V})
end

function runbytes(::Type{V}, n::Int) where {V}
    return vectorbytes(String, n) + vectorbytes(V, n)
end

function NameIndex{V}(budget::Union{Nothing,Budget}) where {V}
    charge = nameindexshellbytes(V)
    budget === nothing || reserve!(budget, charge)
    keys = Vector{Vector{String}}(undef, NAME_INDEX_RUNS)
    vals = Vector{Vector{V}}(undef, NAME_INDEX_RUNS)
    resize!(keys, 0)
    resize!(vals, 0)
    index = NameIndex{V}(keys, vals, 0)
    budget === nothing || allocated!(budget, charge)
    return index
end

function Base.length(index::NameIndex)
    return index.count
end

"The run and position of `key`, or `(0, 0)`."
function runposition(index::NameIndex, key)
    for r in eachindex(index.keys)
        run = index.keys[r]
        i = searchsortedfirst(run, key)
        i <= length(run) && keymatches(run[i], key) && return (r, i)
    end
    return (0, 0)
end

function Base.haskey(index::NameIndex, key)
    return first(runposition(index, key)) != 0
end

function Base.get(index::NameIndex, key, default)
    r, i = runposition(index, key)
    return r == 0 ? default : index.vals[r][i]
end

"Insert a key that is not present (callers check `haskey` first)."
function Base.insert!(index::NameIndex{V}, key::String, value, budget::Union{Nothing,Budget}) where {V}
    budget === nothing || reserve!(budget, runbytes(V, 1))
    ks = String[key]
    vs = V[value]
    budget === nothing || allocated!(budget, runbytes(V, 1))
    while !isempty(index.keys) && length(index.keys[end]) == length(ks)
        ks, vs = mergeruns(pop!(index.keys), pop!(index.vals), ks, vs, budget)
    end
    push!(index.keys, ks)                              # within the prebuilt NAME_INDEX_RUNS capacity
    push!(index.vals, vs)
    index.count += 1
    return index
end

function mergeruns(ak::Vector{String}, av::Vector{V}, bk::Vector{String}, bv::Vector{V},
                   budget::Union{Nothing,Budget}) where {V}
    n = length(ak) + length(bk)
    budget === nothing || reserve!(budget, runbytes(V, n))
    ks = Vector{String}(undef, n)
    vs = Vector{V}(undef, n)
    budget === nothing || allocated!(budget, runbytes(V, n))
    i = j = 1
    for k in 1:n
        if j > length(bk) || (i <= length(ak) && isless(ak[i], bk[j]))
            ks[k] = ak[i]
            vs[k] = av[i]
            i += 1
        else
            ks[k] = bk[j]
            vs[k] = bv[j]
            j += 1
        end
    end
    budget === nothing ||
        release!(budget, runbytes(V, length(ak)) + runbytes(V, length(bk)))   # both inputs die with this frame
    return (ks, vs)
end

"Release the index's own storage (its keys are owned and released by the caller)."
function releasenameindex!(index::NameIndex{V}, budget::Budget) where {V}
    total = nameindexshellbytes(V)
    for run in index.keys
        total = checked_add(total, runbytes(V, length(run)))
    end
    release!(budget, total)
    return nothing
end

"Binary search one sorted partner-id vector and charge every inspected entry."
function resolutionsearchsortedfirst(keys::Vector{Int32}, key::Int32, budget::Budget)
    lo = 1
    hi = length(keys) + 1
    while lo < hi
        mid = lo + ((hi - lo) >>> 1)
        addresolution!(budget)
        if keys[mid] < key
            lo = mid + 1
        else
            hi = mid
        end
    end
    return lo
end

function keymatches(a, b)
    return a == b
end

"Zero a budget for reuse by a prepared per-call path."
function resetbudget!(b::Budget)
    close!(b)
    b.reserved = 0
    b.peak = 0
    b.values = 0
    b.input_bytes = 0
    b.rows = 0
    b.blocks = 0
    b.members = 0
    b.resolution_work = 0
    b.allowance_used = 0
    b.pending = 0
    b.workcap = min(b.limits.max_total_values, b.limits.work_allowance)
    b.workdeferred = 0
    b.default_position = 0
    return b
end

"""
    addinput!(budget, n)

Credit `n` input bytes (decompressed block bytes plus framing, raw datum bytes, or JSON text) to the
work rule.
"""
function addinput!(b::Budget, n::Int)
    n <= 0 && return b
    l = b.limits
    b.workdeferred == 0 &&
        (b.allowance_used = max(b.allowance_used,
                                max(b.values - muladdcap(l.max_values_per_byte, b.input_bytes, 0), 0)))
    b.input_bytes = checked_add(b.input_bytes, n)
    b.workcap = min(l.max_total_values,
                    muladdcap(l.max_values_per_byte, b.input_bytes, l.work_allowance))
    return b
end

"Remove input bytes that a legacy repair proved were padding rather than encoded values."
function removeinput!(b::Budget, n::Int)
    n <= 0 && return b
    n <= b.input_bytes || throw(ArgumentError("input removal exceeds credited bytes"))
    b.input_bytes -= n
    limits = b.limits
    b.workcap = min(limits.max_total_values,
                    muladdcap(limits.max_values_per_byte, b.input_bytes,
                              limits.work_allowance))
    return b
end

"""
    countvalues!(budget, n=1)

Count `n` values (every value encountered, zero-size ones and codec members included) against
`max_total_values` and the work rule `values ≤ max_values_per_byte × input_bytes + work_allowance`
(the cap is cached by `addinput!`, so the hot path is one addition and one comparison).
"""
function checkedlimitadd(b::Budget, current::Int, additional::Int,
                         limit::Symbol, maximum::Int)
    current >= 0 && additional >= 0 ||
        throw(ArgumentError("limit counters must be non-negative"))
    additional <= typemax(Int) - current ||
        throw(limiterror(b, limit, typemax(Int), maximum))
    observed = current + additional
    observed <= maximum || throw(limiterror(b, limit, observed, maximum))
    return observed
end

function checkedvalueadd(b::Budget, current::Int, additional::Int)
    return checkedlimitadd(b, current, additional, :max_total_values,
                           b.limits.max_total_values)
end

@inline function countvalues!(b::Budget, n::Int=1)
    v = checkedvalueadd(b, b.values, n)
    b.values = v
    b.workdeferred == 0 && v > b.workcap && workexceeded(b)
    return b
end

@noinline function workexceeded(b::Budget)
    l = b.limits
    b.values <= l.max_total_values || throw(limiterror(b, :max_total_values, b.values, l.max_total_values))
    cap = muladdcap(l.max_values_per_byte, b.input_bytes, l.work_allowance)
    throw(limiterror(b, :max_values_per_byte, b.values, cap))
end

"The largest draw on `work_allowance` so far (values beyond `max_values_per_byte × input_bytes`)."
function allowanceused(b::Budget)
    base = muladdcap(b.limits.max_values_per_byte, b.input_bytes, 0)
    return max(b.allowance_used, b.values - base)
end

"The exact local work-rule deficit of one datum or container block."
function workdeficit(b::Budget, values::Int, input::Int)
    values >= 0 || throw(ArgumentError("work values must be non-negative"))
    input >= 0 || throw(ArgumentError("work input must be non-negative"))
    return max(values - muladdcap(b.limits.max_values_per_byte, input, 0), 0)
end

"Admit one exact or lower-bound work scope without committing its values to the operation counter."
function checkworkscope!(b::Budget, values::Int, input::Int; projected::Bool=false)
    deficit = workdeficit(b, values, input)
    values <= b.limits.max_total_values ||
        throw(limiterror(b, :max_total_values, values,
                         b.limits.max_total_values))
    cap = muladdcap(b.limits.max_values_per_byte, input, b.limits.work_allowance)
    values <= cap || throw(limiterror(b, :max_values_per_byte, values, cap))
    if projected
        total = checkedvalueadd(b, b.values, values)
        cumulative = max(total - muladdcap(b.limits.max_values_per_byte,
                                           b.input_bytes, 0), 0)
        cumulative <= b.limits.work_allowance ||
            throw(limiterror(b, :max_values_per_byte, total,
                             muladdcap(b.limits.max_values_per_byte, b.input_bytes,
                                       b.limits.work_allowance)))
        b.allowance_used = max(b.allowance_used, cumulative)
    end
    b.allowance_used = max(b.allowance_used, deficit)
    return deficit
end

"Validate the cumulative operation scope and retain its largest shared allowance deficit."
function checkoperationwork!(b::Budget)
    deficit = workdeficit(b, b.values, b.input_bytes)
    deficit <= b.limits.work_allowance ||
        throw(limiterror(b, :max_values_per_byte, b.values,
                         muladdcap(b.limits.max_values_per_byte, b.input_bytes,
                                   b.limits.work_allowance)))
    b.allowance_used = max(b.allowance_used, deficit)
    return deficit
end

"Check values that an already-credited input scope is certain to encounter later."
function checkprojectedvalues!(b::Budget, additional::Int)
    additional >= 0 || throw(ArgumentError("projected values must be non-negative"))
    total = checkedvalueadd(b, b.values, additional)
    cap = muladdcap(b.limits.max_values_per_byte, b.input_bytes,
                    b.limits.work_allowance)
    total <= cap || throw(limiterror(b, :max_values_per_byte, total, cap))
    b.allowance_used = max(b.allowance_used,
                           max(total - muladdcap(b.limits.max_values_per_byte,
                                                 b.input_bytes, 0), 0))
    return nothing
end

function beginworkdefer!(b::Budget)
    b.workdeferred = checked_add(b.workdeferred, 1)
    return nothing
end

function endworkdefer!(b::Budget)
    b.workdeferred > 0 || throw(ArgumentError("work transaction is not active"))
    b.workdeferred -= 1
    return nothing
end

function addrows!(b::Budget, n::Int)
    b.rows = checkedlimitadd(b, b.rows, n, :max_rows, b.limits.max_rows)
    return b
end

function addblocks!(b::Budget, n::Int=1)
    b.blocks = checkedlimitadd(b, b.blocks, n, :max_blocks,
                               b.limits.max_blocks)
    return b
end

function addmembers!(b::Budget, n::Int=1)
    members = checkedlimitadd(b, b.members, n, :max_total_values,
                              b.limits.max_total_values)
    b.members = members
    return countvalues!(b, n)
end

function addresolution!(b::Budget, n::Int=1)
    b.resolution_work = checkedlimitadd(b, b.resolution_work, n,
                                        :max_resolution_work,
                                        b.limits.max_resolution_work)
    return b
end

function checkdepth(b::Budget, depth::Int)
    depth <= b.limits.max_depth || throw(limiterror(b, :max_depth, depth, b.limits.max_depth))
    return nothing
end

"""
    withbudget(f, limits; direction=:decode)

Run `f(budget)` with a fresh operation budget, settling every pending reservation on every exit path.
"""
function withbudget(f, limits::Limits; direction::Symbol=:decode)
    b = Budget(limits; direction=direction)
    try
        return f(b)
    finally
        close!(b)
    end
end
