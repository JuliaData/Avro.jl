# Symbol admission (plan §6): Julia interns `Symbol`s permanently, so untrusted strings are admitted
# through a bounded, lock-protected table before interning. The table is a log-structured set of sorted
# runs (no hashing; plan §4.4 "map structure"): a recent buffer scanned linearly, sorted and merged
# with runs of equal size when full, so lookups binary-search ≤ log2(N ÷ RUN_BASE) + 1 runs.

const RUN_BASE = 1024

"""
    Avro.SymbolAdmission(; max_names=1_000_000, max_bytes=64 << 20)

A caller-owned symbol-admission table: decides which untrusted strings may be interned as `Symbol`s
(Tables column names; typed `Symbol` values). Strings already admitted do not count twice. Exceeding
`max_names` or `max_bytes` raises `LimitError`. `Avro.DEFAULT_ADMISSION` is the process-wide default;
pass `names=:trusted` to bypass admission for trusted sources.
"""
mutable struct SymbolAdmission
    const lock::ReentrantLock
    const max_names::Int
    const max_bytes::Int
    const recent::Vector{String}          # unsorted, ≤ RUN_BASE entries
    const runs::Vector{Vector{String}}    # sorted runs of sizes RUN_BASE × 2^k, largest first
    count::Int
    bytes::Int
end

function SymbolAdmission(; max_names::Integer=1_000_000, max_bytes::Integer=64 << 20)
    max_names >= 0 || throw(ArgumentError("max_names must be ≥ 0"))
    max_bytes >= 0 || throw(ArgumentError("max_bytes must be ≥ 0"))
    return SymbolAdmission(ReentrantLock(), Int(max_names), Int(max_bytes), String[], Vector{String}[], 0, 0)
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

function carry!(a::SymbolAdmission)
    length(a.recent) < RUN_BASE && return nothing
    run = sort!(copy(a.recent))
    empty!(a.recent)
    while !isempty(a.runs) && length(a.runs[end]) == length(run)
        run = mergeruns(pop!(a.runs), run)
    end
    push!(a.runs, run)
    return nothing
end

function mergeruns(x::Vector{String}, y::Vector{String})
    out = Vector{String}(undef, length(x) + length(y))
    i = j = k = 1
    while i <= length(x) && j <= length(y)
        if x[i] <= y[j]
            out[k] = x[i]; i += 1
        else
            out[k] = y[j]; j += 1
        end
        k += 1
    end
    while i <= length(x)
        out[k] = x[i]; i += 1; k += 1
    end
    while j <= length(y)
        out[k] = y[j]; j += 1; k += 1
    end
    return out
end

"""
    admit!(admission, s::AbstractString) -> Symbol

Admit `s` (if new, counting it against the table's budgets) and return `Symbol(s)`.
"""
function admit!(a::SymbolAdmission, s::AbstractString)
    str = String(s)
    lock(a.lock) do
        contains_unlocked(a, str) && return nothing
        ncount = a.count + 1
        ncount <= a.max_names || throw(LimitError(:max_names, ncount, a.max_names, :max_names, :decode))
        nbytes = checked_add(a.bytes, sizeof(str))
        nbytes <= a.max_bytes || throw(LimitError(:max_bytes, nbytes, a.max_bytes, :max_bytes, :decode))
        push!(a.recent, str)
        a.count = ncount
        a.bytes = nbytes
        carry!(a)
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
