# Limits, budgets and the memory ceiling (plan §4.4).
#
# One fixed-default, portable per-operation ceiling bounds package-owned memory through reservations made
# before the corresponding allocation; work is bounded as a function of input bytes (the value rule), and
# key comparisons by the comparison rule. Every `LimitError` names the limit, the observed value and the
# keyword that raises it.

const KiB = 1 << 10
const MiB = 1 << 20

"""
    Avro.Limits(; kwargs...)

Fixed-default, portable resource limits. Every field is an `Int ≥ 0`; the constructor validates the
cross-field relations of plan §4.4 and raises `ArgumentError` otherwise. The same limits must be raised
on every side that processes the data (writers enforce every limit readers enforce).

Fields (defaults): `max_depth` (1024), `max_bytes` (64 MiB), `max_datum_bytes` (64 MiB),
`max_json_depth` (1024), `max_block_bytes` (16 MiB), `max_block_count` (2^24),
`max_block_output_bytes` (64 MiB), `max_blocks` (2^28), `max_codec_memory` (32 MiB),
`max_total_bytes` (256 MiB), `max_total_values` (3*2^26), `max_rows` (2^28), `max_values_per_byte` (16),
`work_allowance` (65,536), `max_compare_bytes_per_byte` (64), `max_resolution_work` (1,000,000),
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
    max_compare_bytes_per_byte::Int
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
    max_blocks = 1 << 28,
    max_codec_memory = 32 * MiB,
    max_total_bytes = 256 * MiB,
    max_total_values = 3 << 26,
    max_rows = 1 << 28,
    max_values_per_byte = 16,
    work_allowance = 65_536,
    max_compare_bytes_per_byte = 64,
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

# clamped addition for counters that must never throw (guard state)
function clamped_add(a::Int, b::Int)
    return (r = a + b; (b > 0 && r < a) ? typemax(Int) : ((b < 0 && r > a) ? typemin(Int) : r))
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
buffer, the codec workspace plus 4 MiB of fixed overhead, and one datum (plan §4.4).
"""
function first_unit_bytes(l::Limits)
    return satadd(satadd(satadd(l.max_block_bytes, l.max_block_bytes),
                         satadd(l.max_codec_memory, 4 * MiB)),
                  l.max_datum_bytes)
end

# ---- available-memory guard (best-effort; plan §4.4) ------------------------------------------------

mutable struct GuardState
    @atomic pending::Int   # bytes reserved by live operations but not yet allocated
end

const GUARD = GuardState(0)

function cgroup_remaining()
    Sys.islinux() || return typemax(Int)
    return something(cgroup_v2_remaining(), cgroup_v1_remaining(), typemax(Int))
end

function read_int_file(path::AbstractString)
    isfile(path) || return nothing
    txt = try
        strip(Base.read(path, String))
    catch
        return nothing
    end
    txt == "max" && return typemax(Int)
    return tryparse(Int, txt)
end

function cgroup_v2_remaining()
    mx = read_int_file("/sys/fs/cgroup/memory.max")
    cur = read_int_file("/sys/fs/cgroup/memory.current")
    (mx === nothing || cur === nothing) && return nothing
    mx == typemax(Int) && return typemax(Int)
    return max(mx - cur, 0)
end

function cgroup_v1_remaining()
    mx = read_int_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    cur = read_int_file("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    (mx === nothing || cur === nothing) && return nothing
    mx >= typemax(Int) ÷ 2 && return typemax(Int)   # "unlimited" is reported as a huge number
    return max(mx - cur, 0)
end

"""
    available_memory()

Best-effort estimate of the memory this process may still use: the minimum of the host's free memory,
the (cgroup-constrained) total memory and the cgroup's remaining memory, minus the package's pending
reservations. `Sys.free_memory()` is host-wide on Julia ≤ 1.12 (documented); on macOS it counts only
truly free pages, so the reclaimable inactive page cache is added (`host_free_memory`).
"""
function available_memory()
    free = host_free_memory()
    total = Int(min(Sys.total_memory(), typemax(Int) % UInt64))
    avail = min(free, total, cgroup_remaining())
    return max(clamped_add(avail, -(@atomic GUARD.pending)), 0)
end

# `Sys.free_memory()`, except on macOS where free + inactive pages (the `vm_stat` convention) is the
# reclaimable figure: free pages alone routinely fall to a few hundred MB on a busy host.
function host_free_memory()
    free = Int(min(Sys.free_memory(), typemax(Int) % UInt64))
    Sys.isapple() || return free
    return max(free, something(darwin_available_memory(), 0))
end

function darwin_available_memory()
    stats = zeros(UInt32, 38)   # vm_statistics64_data_t as HOST_VM_INFO64_COUNT integer_t
    count = Ref{UInt32}(length(stats))
    host = ccall(:mach_host_self, UInt32, ())
    kr = ccall(:host_statistics64, Cint, (UInt32, Cint, Ptr{UInt32}, Ptr{UInt32}), host, 4, stats, count)
    ccall(:mach_port_deallocate, Cint, (UInt32, UInt32), unsafe_load(cglobal(:mach_task_self_, UInt32)), host)
    (kr == 0 && count[] >= 3) || return nothing
    pagesize = Int(ccall(:getpagesize, Cint, ()))
    return clamped_add(Int(stats[1]), Int(stats[3])) * pagesize   # free + inactive pages
end

"""
    effective_ceiling(limits; available=available_memory())

`min(limits.max_total_bytes, available ÷ 2)` — the ceiling an operation actually runs under (plan §4.4).
"""
function effective_ceiling(l::Limits; available::Int=available_memory())
    return min(l.max_total_bytes, max(available, 0) ÷ 2)
end

# ---- per-operation budget -------------------------------------------------------------------------

"""
    Budget(limits; direction=:decode, available=available_memory())

The accumulator of one operation: live reservations against the effective ceiling, the value and
comparison work counters, rows, blocks, codec members, resolution work and the shared allowance
(plan §4.4). Constructing a budget fails with `LimitError(:available_memory)` when the effective
ceiling cannot admit the operation's first unit of progress.
"""
mutable struct Budget
    const limits::Limits
    const ceiling::Int
    const direction::Symbol
    reserved::Int
    peak::Int
    values::Int
    input_bytes::Int
    compare_bytes::Int
    rows::Int
    blocks::Int
    members::Int
    resolution_work::Int
    allowance_used::Int     # the largest draw on work_allowance so far (maintained when input arrives)
    pending::Int            # this operation's contribution to GUARD.pending
    published::Int          # the part of `pending` already pushed to GUARD (batched, best-effort)
    workcap::Int            # min(max_total_values, max_values_per_byte × input_bytes + work_allowance)
    workdeferred::Int       # root encode transactions whose exact byte denominator is not known yet
    default_position::Int   # reusable cursor into a resolved default's immutable branch tape
end

function Budget(limits::Limits; direction::Symbol=:decode, available::Int=available_memory())
    direction in (:decode, :encode) || throw(ArgumentError("direction must be :decode or :encode"))
    ceiling = effective_ceiling(limits; available=available)
    need = first_unit_bytes(limits)
    ceiling >= need || throw(LimitError(:available_memory, available, need, :max_total_bytes, direction))
    return Budget(limits, ceiling, direction, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  min(limits.max_total_values, limits.work_allowance), 0, 0)
end

"A persistent table ledger whose explicit byte ceiling is independent of operation-limit relations."
function ledgerbudget(ceiling::Int; direction::Symbol=:decode)
    ceiling >= 0 || throw(ArgumentError("ledger ceiling must be non-negative"))
    direction in (:decode, :encode) || throw(ArgumentError("direction must be :decode or :encode"))
    limits = Limits()
    return Budget(limits, ceiling, direction, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
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
when the reservation would exceed the operation's effective ceiling.
"""
const GUARD_CHUNK = 1 << 20   # guard publication batch: each budget's unpublished slack stays < 1 MiB

function updateguard!(delta::Int)
    while true
        old = @atomic GUARD.pending
        new = max(clamped_add(old, delta), 0)
        result = @atomicreplace GUARD.pending old => new
        result.success && return new
    end
end

function reserve!(b::Budget, n::Int)
    n >= 0 || throw(ArgumentError("reservation must be non-negative"))
    n == 0 && return b
    total = satadd(b.reserved, n)
    total == typemax(Int) && b.reserved > typemax(Int) - n &&
        throw(limiterror(b, :max_total_bytes, total, b.ceiling))
    total <= b.ceiling || throw(limiterror(b, :max_total_bytes, total, b.ceiling))
    b.reserved = total
    b.peak = max(b.peak, total)
    b.pending = clamped_add(b.pending, n)
    if b.pending - b.published >= GUARD_CHUNK          # batched: the global atomic is off the per-cell path
        delta = b.pending - b.published
        updateguard!(delta)
        b.published = b.pending
    end
    return b
end

# Drop `n` bytes from the budget's pending counter and (batched) from the guard. Internal: the public
# entry points are `allocated!`, `unreserve!` and `close!`, which validate their side of the contract.
function settlepending!(b::Budget, n::Int)
    n <= 0 && return b
    b.pending -= n
    if b.published - b.pending >= GUARD_CHUNK || (b.pending == 0 && b.published > 0)
        delta = b.published - b.pending
        updateguard!(-delta)
        b.published = b.pending
    end
    return b
end

"""
    allocated!(budget, n)

Record that `n` previously reserved bytes are now resident: they leave the guard's pending counter,
which must only carry reservations not yet backed by storage (plan §4.4 — the OS counters already see
resident pages, so leaving them pending would subtract them twice from `available_memory()`). Settling
more bytes than are pending is a settlement mismatch and throws instead of clamping.
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
`allocated!` after it instead (round-4 item 2).
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
allocation) to the guard and the budget.
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

Return every pending reservation to the guard (called in the `finally` of every operation).
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
Construction-side exact-replacement growth (plan §4.4): the backing vector starts at a reserved exact
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

"Push with §4.4 exact-replacement growth when the prebuilt capacity is exhausted."
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

"Sorted insert (or overwrite) with §4.4 exact-replacement growth when the capacity is exhausted."
function budgetedinsert!(d::FrozenDict{K,V}, k, v, budget::Union{Nothing,Budget}) where {K,V}
    d.frozen && throw(FrozenError("dict"))
    i = budgetedsearchsortedfirst(d.keys, k, budget)
    if i <= length(d.keys)
        budget === nothing || addcompare!(budget, keycomparisonwork(d.keys[i], k))
    end
    if i <= length(d.keys) && keymatches(d.keys[i], k)
        budget === nothing || addcompare!(budget, slotbytes(V))
        d.vals[i] = v
        return d
    end
    n = length(d.keys)
    moved = checked_mul(n - i + 2, checked_add(slotbytes(K), slotbytes(V)))
    if n == d.cap
        moved = checked_add(moved, checked_mul(n, checked_add(slotbytes(K), slotbytes(V))))
    end
    budget === nothing || addcompare!(budget, moved)   # all fallible work precedes allocation or mutation
    if length(d.keys) == d.cap
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
        d.keys = nk                                    # the old storage is unreachable only after the rebind
        d.vals = nv
        d.cap = newcap
        budget === nothing || release!(budget, oldbytes)
    end
    insert!(d.keys, i, convert(K, k))
    insert!(d.vals, i, convert(V, v))
    return d
end

function budgetedsearchsortedfirst(keys::Vector, key, budget::Union{Nothing,Budget})
    lo = 1
    hi = length(keys) + 1
    while lo < hi
        mid = lo + ((hi - lo) >>> 1)
        budget === nothing || addcompare!(budget, keycomparisonwork(keys[mid], key))
        if isless(keys[mid], key)
            lo = mid + 1
        else
            hi = mid
        end
    end
    return lo
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

function budgetedkeyindex(d::FrozenDict, key, budget::Union{Nothing,Budget})
    i = budgetedsearchsortedfirst(d.keys, key, budget)
    if i <= length(d.keys)
        budget === nothing || addcompare!(budget, keycomparisonwork(d.keys[i], key))
        keymatches(d.keys[i], key) && return i
    end
    return 0
end

function budgetedhaskey(d::FrozenDict, key, budget::Union{Nothing,Budget})
    return budgetedkeyindex(d, key, budget) != 0
end

function budgetedget(d::FrozenDict, key, default, budget::Union{Nothing,Budget})
    i = budgetedkeyindex(d, key, budget)
    i == 0 && return default
    return d.vals[i]
end

function budgetedgetindex(d::FrozenDict, key, budget::Union{Nothing,Budget})
    i = budgetedkeyindex(d, key, budget)
    i == 0 && throw(KeyError(key))
    return d.vals[i]
end

function keycomparisonwork(a::AbstractString, b::AbstractString)
    return min(sizeof(a), sizeof(b)) + 1
end

function keycomparisonwork(a, b)
    return max(slotbytes(typeof(a)), slotbytes(typeof(b)))
end

function keymatches(a, b)
    return a == b
end

"Zero a budget for reuse by a prepared per-call path (guard bookkeeping settled first)."
function resetbudget!(b::Budget)
    close!(b)
    b.reserved = 0
    b.peak = 0
    b.values = 0
    b.input_bytes = 0
    b.compare_bytes = 0
    b.rows = 0
    b.blocks = 0
    b.members = 0
    b.resolution_work = 0
    b.allowance_used = 0
    b.pending = 0
    b.published = 0
    b.workcap = min(b.limits.max_total_values, b.limits.work_allowance)
    b.workdeferred = 0
    b.default_position = 0
    return b
end

function reserve_replacement!(b::Budget, oldbytes::Int, newbytes::Int)
    reserve!(b, newbytes)   # old and new storage are both charged during the copy
    return b
end

"""
    addinput!(budget, n)

Credit `n` input bytes (decompressed block bytes plus framing, raw datum bytes, or JSON text) to the
work and comparison rules.
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

"""
    addcompare!(budget, n)

Charge `n` compared key bytes (or moved index entries, 4 or 8 bytes each) against the comparison rule
`compare_bytes ≤ max_compare_bytes_per_byte × input_bytes + work_allowance`.
"""
function addcompare!(b::Budget, n::Int)
    n <= 0 && return b
    l = b.limits
    cap = muladdcap(l.max_compare_bytes_per_byte, b.input_bytes,
                    l.work_allowance)
    n <= typemax(Int) - b.compare_bytes ||
        throw(limiterror(b, :max_compare_bytes_per_byte, typemax(Int), cap))
    b.compare_bytes += n
    if b.workdeferred == 0
        b.compare_bytes <= cap ||
            throw(limiterror(b, :max_compare_bytes_per_byte, b.compare_bytes, cap))
    end
    return b
end

"Validate exact comparison work against the input bytes of its complete operation scope."
function checkcomparisonwork!(b::Budget, compared::Int=b.compare_bytes,
                              input::Int=b.input_bytes)
    compared >= 0 || throw(ArgumentError("compared bytes must be non-negative"))
    input >= 0 || throw(ArgumentError("comparison input must be non-negative"))
    cap = muladdcap(b.limits.max_compare_bytes_per_byte, input,
                    b.limits.work_allowance)
    compared <= cap ||
        throw(limiterror(b, :max_compare_bytes_per_byte, compared, cap))
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

Run `f(budget)` with a fresh operation budget, returning every pending reservation to the guard on every
exit path.
"""
function withbudget(f, limits::Limits; direction::Symbol=:decode, available::Int=available_memory())
    b = Budget(limits; direction=direction, available=available)
    try
        return f(b)
    finally
        close!(b)
    end
end
