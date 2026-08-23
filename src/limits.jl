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
`max_total_bytes` (256 MiB), `max_total_values` (2^28), `max_rows` (2^28), `max_values_per_byte` (16),
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
    max_total_values = 1 << 28,
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

function Limits(; kwargs...)
    for k in keys(kwargs)
        haskey(LIMIT_DEFAULTS, k) || throw(ArgumentError("unknown Avro.Limits keyword `$k`"))
    end
    vals = ntuple(i -> Int(get(kwargs, keys(LIMIT_DEFAULTS)[i], LIMIT_DEFAULTS[i])), length(LIMIT_DEFAULTS))
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
    l.max_datum_bytes <= checked_add(l.max_bytes, MiB) ||
        throw(ArgumentError("Avro.Limits: `max_datum_bytes` ($(l.max_datum_bytes)) must be ≤ `max_bytes` + 1 MiB ($(checked_add(l.max_bytes, MiB)))"))
    quarter = l.max_total_bytes ÷ 4
    half = l.max_total_bytes ÷ 2
    l.max_block_bytes <= quarter ||
        throw(ArgumentError("Avro.Limits: `max_block_bytes` ($(l.max_block_bytes)) must be ≤ `max_total_bytes` ÷ 4 ($quarter)"))
    checked_add(l.max_codec_memory, 4 * MiB) <= quarter ||
        throw(ArgumentError("Avro.Limits: `max_codec_memory` + 4 MiB ($(checked_add(l.max_codec_memory, 4 * MiB))) must be ≤ `max_total_bytes` ÷ 4 ($quarter)"))
    l.max_block_output_bytes <= half ||
        throw(ArgumentError("Avro.Limits: `max_block_output_bytes` ($(l.max_block_output_bytes)) must be ≤ `max_total_bytes` ÷ 2 ($half)"))
    checked_add(l.max_metadata_bytes, l.max_schema_bytes) <= half ||
        throw(ArgumentError("Avro.Limits: `max_metadata_bytes` + `max_schema_bytes` ($(checked_add(l.max_metadata_bytes, l.max_schema_bytes))) must be ≤ `max_total_bytes` ÷ 2 ($half)"))
    return l
end

checked_add(a::Int, b::Int) = Base.Checked.checked_add(a, b)
checked_mul(a::Int, b::Int) = Base.Checked.checked_mul(a, b)

# clamped addition for counters that must never throw (guard state)
clamped_add(a::Int, b::Int) = (r = a + b; (b > 0 && r < a) ? typemax(Int) : ((b < 0 && r > a) ? typemin(Int) : r))

Base.show(io::IO, l::Limits) = print(io, "Avro.Limits(max_total_bytes=", l.max_total_bytes, ", …)")

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
first_unit_bytes(l::Limits) =
    checked_add(checked_add(checked_add(l.max_block_bytes, l.max_block_bytes), checked_add(l.max_codec_memory, 4 * MiB)), l.max_datum_bytes)

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
effective_ceiling(l::Limits; available::Int=available_memory()) = min(l.max_total_bytes, max(available, 0) ÷ 2)

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
end

function Budget(limits::Limits; direction::Symbol=:decode, available::Int=available_memory())
    direction in (:decode, :encode) || throw(ArgumentError("direction must be :decode or :encode"))
    ceiling = effective_ceiling(limits; available=available)
    need = first_unit_bytes(limits)
    ceiling >= need || throw(LimitError(:available_memory, available, need, :max_total_bytes, direction))
    return Budget(limits, ceiling, direction, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, min(limits.max_total_values, limits.work_allowance))
end

limiterror(b::Budget, limit::Symbol, observed::Int, value::Int) = LimitError(limit, observed, value, limit, b.direction)

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
    total = checked_add(b.reserved, n)
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

"""
    allocated!(budget, n)

Record that `n` previously reserved bytes are now resident (they leave the guard's pending counter).
"""
function allocated!(b::Budget, n::Int)
    n <= 0 && return b
    n = min(n, b.pending)
    b.pending -= n
    if b.published - b.pending >= GUARD_CHUNK || (b.pending == 0 && b.published > 0)
        delta = b.published - b.pending
        updateguard!(-delta)
        b.published = b.pending
    end
    return b
end

"""
    release!(budget, n)

Release `n` reserved bytes (after the corresponding storage became unreachable or was handed to the
caller).
"""
function release!(b::Budget, n::Int)
    n <= 0 && return b
    allocated!(b, n)
    b.reserved = max(b.reserved - n, 0)
    return b
end

"""
    close!(budget)

Return every pending reservation to the guard (called in the `finally` of every operation).
"""
function close!(b::Budget)
    allocated!(b, b.pending)
    return b
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
    b.allowance_used = max(b.allowance_used, b.values - checked_mul(l.max_values_per_byte, b.input_bytes))   # values only grow between inputs
    b.input_bytes = checked_add(b.input_bytes, n)
    b.workcap = min(l.max_total_values, checked_add(checked_mul(l.max_values_per_byte, b.input_bytes), l.work_allowance))
    return b
end

"""
    countvalues!(budget, n=1)

Count `n` values (every value encountered, zero-size ones and codec members included) against
`max_total_values` and the work rule `values ≤ max_values_per_byte × input_bytes + work_allowance`
(the cap is cached by `addinput!`, so the hot path is one addition and one comparison).
"""
@inline function countvalues!(b::Budget, n::Int=1)
    v = checked_add(b.values, n)
    b.values = v
    v <= b.workcap || workexceeded(b)
    return b
end

@noinline function workexceeded(b::Budget)
    l = b.limits
    b.values <= l.max_total_values || throw(limiterror(b, :max_total_values, b.values, l.max_total_values))
    cap = checked_add(checked_mul(l.max_values_per_byte, b.input_bytes), l.work_allowance)
    throw(limiterror(b, :max_values_per_byte, b.values, cap))
end

"The largest draw on `work_allowance` so far (values beyond `max_values_per_byte × input_bytes`)."
allowanceused(b::Budget) = max(b.allowance_used, b.values - checked_mul(b.limits.max_values_per_byte, b.input_bytes))

"""
    addcompare!(budget, n)

Charge `n` compared key bytes (or moved index entries, 4 or 8 bytes each) against the comparison rule
`compare_bytes ≤ max_compare_bytes_per_byte × input_bytes + work_allowance`.
"""
function addcompare!(b::Budget, n::Int)
    n <= 0 && return b
    l = b.limits
    b.compare_bytes = checked_add(b.compare_bytes, n)
    cap = checked_add(checked_mul(l.max_compare_bytes_per_byte, b.input_bytes), l.work_allowance)
    b.compare_bytes <= cap || throw(limiterror(b, :max_compare_bytes_per_byte, b.compare_bytes, cap))
    return b
end

function addrows!(b::Budget, n::Int)
    b.rows = checked_add(b.rows, n)
    b.rows <= b.limits.max_rows || throw(limiterror(b, :max_rows, b.rows, b.limits.max_rows))
    return b
end

function addblocks!(b::Budget, n::Int=1)
    b.blocks = checked_add(b.blocks, n)
    b.blocks <= b.limits.max_blocks || throw(limiterror(b, :max_blocks, b.blocks, b.limits.max_blocks))
    return b
end

function addmembers!(b::Budget, n::Int=1)
    b.members = checked_add(b.members, n)
    return countvalues!(b, n)
end

function addresolution!(b::Budget, n::Int=1)
    b.resolution_work = checked_add(b.resolution_work, n)
    b.resolution_work <= b.limits.max_resolution_work ||
        throw(limiterror(b, :max_resolution_work, b.resolution_work, b.limits.max_resolution_work))
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
