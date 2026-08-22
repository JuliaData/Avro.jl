# Symbol admission (plan §4.4/§6): Julia interns `Symbol`s permanently, so untrusted strings are
# admitted through a bounded, lock-protected table before interning. The table is a log-structured set
# of sorted runs (no hashing): a recent buffer scanned linearly, sorted into a run when full. Merges
# are deamortised: an in-progress merge advances by at most MERGE_STEP moved entries per admission,
# the source runs stay searchable until the completed output replaces them (a partially built run is
# never consulted), and maintenance runs before an admission mutates anything, so a failed admission
# leaves the table unchanged. `max_bytes` accounts the table's memory — string bytes plus an 8-byte
# index slot per name — and merge scratch is reserved against it before allocation.

const RUN_BASE = 1024
const MERGE_STEP = 2048

"An in-progress deamortised run merge: the sources stay searchable until the output replaces them."
mutable struct RunMerge
    const x::Vector{String}
    const y::Vector{String}
    const out::Vector{String}
    i::Int
    j::Int
end

"""
    Avro.SymbolAdmission(; max_names=1_000_000, max_bytes=64 << 20)

A caller-owned symbol-admission table: decides which untrusted strings may be interned as `Symbol`s
(Tables column names; typed `Symbol` values). Strings already admitted do not count twice. `max_bytes`
bounds the table's memory: the admitted strings' bytes plus an 8-byte index slot per name, with merge
scratch reserved against it. Exceeding `max_names` or `max_bytes` raises `LimitError` and leaves the
table unchanged. `Avro.DEFAULT_ADMISSION` is the process-wide default; pass `names=:trusted` to bypass
admission for trusted sources.
"""
mutable struct SymbolAdmission
    const lock::ReentrantLock
    const max_names::Int
    const max_bytes::Int
    const recent::Vector{String}          # unsorted, ≤ RUN_BASE entries
    const runs::Vector{Vector{String}}    # sorted runs; merged pairwise as sizes match
    merge::Union{Nothing,RunMerge}
    mergeat::Int                          # runs index of the active merge's x (y sits at mergeat + 1)
    count::Int
    bytes::Int                            # string bytes + 8 per admitted name
end

function SymbolAdmission(; max_names::Integer=1_000_000, max_bytes::Integer=64 << 20)
    max_names >= 0 || throw(ArgumentError("max_names must be ≥ 0"))
    max_bytes >= 0 || throw(ArgumentError("max_bytes must be ≥ 0"))
    return SymbolAdmission(ReentrantLock(), Int(max_names), Int(max_bytes), String[], Vector{String}[], nothing, 0, 0, 0)
end

const DEFAULT_ADMISSION = SymbolAdmission()

Base.length(a::SymbolAdmission) = lock(() -> a.count, a.lock)

function contains_unlocked(a::SymbolAdmission, s::String)
    for r in a.recent
        r == s && return true
    end
    for run in a.runs
        i = searchsortedfirst(run, s)
        i <= length(run) && run[i] == s && return true
    end
    return false
end

"Stage the next equal-size pair (scratch reserved against `max_bytes` before allocation)."
function schedule_unlocked!(a::SymbolAdmission)
    a.merge === nothing || return nothing
    for i in length(a.runs) - 1:-1:1
        length(a.runs[i]) == length(a.runs[i + 1]) || continue
        outlen = length(a.runs[i]) + length(a.runs[i + 1])
        need = checked_add(a.bytes, 8 * outlen)
        need <= a.max_bytes || throw(LimitError(:max_bytes, need, a.max_bytes, :max_bytes, :decode))
        a.merge = RunMerge(a.runs[i], a.runs[i + 1], Vector{String}(undef, outlen), 1, 1)
        a.mergeat = i
        return nothing
    end
    return nothing
end

"Advance the active merge by at most MERGE_STEP moves; completion swaps the output in and may cascade."
function step_unlocked!(a::SymbolAdmission)
    m = a.merge
    m === nothing && return nothing
    x, y, out = m.x, m.y, m.out
    i, j = m.i, m.j
    k = i + j - 1
    stop = min(k + MERGE_STEP - 1, length(out))
    @inbounds while k <= stop
        if i <= length(x) && (j > length(y) || x[i] <= y[j])
            out[k] = x[i]
            i += 1
        else
            out[k] = y[j]
            j += 1
        end
        k += 1
    end
    m.i = i
    m.j = j
    if k > length(out)
        a.runs[a.mergeat] = out
        deleteat!(a.runs, a.mergeat + 1)
        a.merge = nothing
        a.mergeat = 0
        schedule_unlocked!(a)             # a completed merge may enable the next equal-size pair
    end
    return nothing
end

function carry_unlocked!(a::SymbolAdmission)
    length(a.recent) < RUN_BASE && return nothing
    run = sort!(copy(a.recent))
    empty!(a.recent)
    push!(a.runs, run)
    schedule_unlocked!(a)
    return nothing
end

"""
    admit!(admission, s::AbstractString) -> Symbol

Admit `s` (if new, counting it against the table's budgets) and return `Symbol(s)`.
"""
function admit!(a::SymbolAdmission, s::AbstractString)
    str = String(s)
    lock(a.lock) do
        step_unlocked!(a)                 # maintenance first: a failure below leaves the table unchanged
        contains_unlocked(a, str) && return nothing
        ncount = a.count + 1
        ncount <= a.max_names || throw(LimitError(:max_names, ncount, a.max_names, :max_names, :decode))
        nbytes = checked_add(a.bytes, sizeof(str) + 8)
        nbytes <= a.max_bytes || throw(LimitError(:max_bytes, nbytes, a.max_bytes, :max_bytes, :decode))
        if length(a.recent) + 1 == RUN_BASE && a.merge === nothing &&
           !isempty(a.runs) && length(a.runs[end]) == RUN_BASE
            need = checked_add(nbytes, 8 * 2 * RUN_BASE)     # the carry would stage a (RUN_BASE, RUN_BASE) merge
            need <= a.max_bytes || throw(LimitError(:max_bytes, need, a.max_bytes, :max_bytes, :decode))
        end
        push!(a.recent, str)
        a.count = ncount
        a.bytes = nbytes
        carry_unlocked!(a)
        return nothing
    end
    return Symbol(str)
end

admit!(::Symbol, s::AbstractString) = Symbol(s)   # `:trusted` bypass (validated by callers)

"""
    admission(names) -> SymbolAdmission | Symbol

Normalise the `names=` keyword: a `SymbolAdmission` object, or `:trusted`.
"""
admission(a::SymbolAdmission) = a
function admission(s::Symbol)
    s === :trusted || throw(ArgumentError("`names` must be an Avro.SymbolAdmission or :trusted, got :$s"))
    return s
end
admission(x) = throw(ArgumentError("`names` must be an Avro.SymbolAdmission or :trusted"))
