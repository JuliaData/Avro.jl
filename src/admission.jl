# Symbol admission (plan §4.4/§6): Julia interns `Symbol`s permanently, so untrusted strings are
# admitted through a bounded, lock-protected table before interning. The table is a log-structured set
# of sorted runs (no hashing): a recent buffer scanned linearly, sorted into a run when full. Merges
# are deamortised: an in-progress merge advances by at most MERGE_STEP moved entries per admission,
# the source runs stay searchable until the completed output replaces them (a partially built run is
# never consulted). `max_bytes` accounts the table's memory — string bytes plus an 8-byte index slot
# per name, plus the staged merge's output slots while the merge is live (replacement overlap) — and
# every allocation is exact and precedes any mutation (round-3 item 5): the recent buffer is prebuilt
# at RUN_BASE capacity, the carried run and the next merge's output are built before the admission
# publishes, so a failed admission leaves the table unchanged.

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
bounds the table's memory: the admitted strings' bytes plus an 8-byte index slot per name, plus the
staged merge's output slots while a merge is in progress. Exceeding `max_names` or `max_bytes` raises
`LimitError` and leaves the table unchanged. `Avro.DEFAULT_ADMISSION` is the process-wide default;
pass `names=:trusted` to bypass admission for trusted sources.
"""
mutable struct SymbolAdmission
    const lock::ReentrantLock
    const max_names::Int
    const max_bytes::Int
    const recent::Vector{String}          # unsorted, ≤ RUN_BASE entries; prebuilt at RUN_BASE capacity
    const runs::Vector{Vector{String}}    # sorted runs; merged pairwise as sizes match
    merge::Union{Nothing,RunMerge}
    mergeat::Int                          # runs index of the active merge's x (y sits at mergeat + 1)
    count::Int
    bytes::Int                            # string bytes + 8 per admitted name + the live merge's output slots
end

function SymbolAdmission(; max_names::Integer=1_000_000, max_bytes::Integer=64 << 20)
    max_names >= 0 || throw(ArgumentError("max_names must be ≥ 0"))
    max_bytes >= 0 || throw(ArgumentError("max_bytes must be ≥ 0"))
    recent = Vector{String}(undef, RUN_BASE)           # exact capacity once, never grown (§4.4 growth rule)
    resize!(recent, 0)
    return SymbolAdmission(ReentrantLock(), Int(max_names), Int(max_bytes), recent, Vector{String}[], nothing, 0, 0, 0)
end

const DEFAULT_ADMISSION = SymbolAdmission()

function Base.length(a::SymbolAdmission)
    return lock(() -> a.count, a.lock)
end

function contains_unlocked(a::SymbolAdmission, s::AbstractString)
    for r in a.recent
        r == s && return true
    end
    for run in a.runs
        i = searchsortedfirst(run, s)
        i <= length(run) && run[i] == s && return true
    end
    return false
end

"""
Stage the next equal-size pair. The output slots are held in `a.bytes` while the merge is live
(replacement overlap, round-3 item 5); `prebuilt` is the output vector built before the admission
mutated anything (an exact fresh allocation covers a prediction mismatch, which never happens under
the lock).
"""
function schedule_unlocked!(a::SymbolAdmission, prebuilt::Union{Nothing,Vector{String}}=nothing)
    a.merge === nothing || return nothing
    for i in length(a.runs) - 1:-1:1
        length(a.runs[i]) == length(a.runs[i + 1]) || continue
        outlen = length(a.runs[i]) + length(a.runs[i + 1])
        need = checked_add(a.bytes, 8 * outlen)
        need <= a.max_bytes || throw(LimitError(:max_bytes, need, a.max_bytes, :max_bytes, :decode))
        out = prebuilt !== nothing && length(prebuilt) == outlen ? prebuilt : Vector{String}(undef, outlen)
        a.merge = RunMerge(a.runs[i], a.runs[i + 1], out, 1, 1)
        a.mergeat = i
        a.bytes = need                                 # the overlap is held until the sources are dropped
        return nothing
    end
    return nothing
end

"Advance the active merge by at most MERGE_STEP moves; completion swaps the output in and may cascade."
function step_unlocked!(a::SymbolAdmission, prebuilt::Union{Nothing,Vector{String}}=nothing)
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
        a.bytes -= 8 * length(out)        # the sources die: the held overlap returns
        schedule_unlocked!(a, prebuilt)   # a completed merge may enable the next equal-size pair
    end
    return nothing
end

"Publish the prebuilt carried run (built and sorted before the admission mutated anything)."
function carry_unlocked!(a::SymbolAdmission, run::Union{Nothing,Vector{String}},
                         prebuilt::Union{Nothing,Vector{String}})
    run === nothing && return nothing
    resize!(a.recent, 0)                  # capacity RUN_BASE is retained; the buffer is never grown
    push!(a.runs, run)
    schedule_unlocked!(a, prebuilt)
    return nothing
end

"Build the carried run before any mutation: an exact-capacity sorted copy of the full recent buffer."
function buildcarry(a::SymbolAdmission)
    run = Vector{String}(undef, RUN_BASE)
    copyto!(run, 1, a.recent, 1, RUN_BASE)
    return sort!(run)
end

"The run length at `i` after the active merge completes, without mutating the run table."
function completedrunlength(a::SymbolAdmission, i::Int, at::Int, merged::Int)
    if i < at
        return length(a.runs[i])
    elseif i == at
        return merged
    end
    return length(a.runs[i + 1])
end

"""
The staged-merge plan after one maintenance step and an optional carry, without mutating anything:
`(nextlen, released)` — the output length of the merge the step will stage (0 when none) and the held
overlap a completion this step returns. The admission preflights `bytes + 8nextlen - released` and
prebuilds the `nextlen` output before it mutates the table.
"""
function nextmergeplan(a::SymbolAdmission, carry::Bool)
    active = a.merge
    completed = false
    at = 0
    merged = 0
    released = 0
    nruns = length(a.runs)
    if active !== nothing
        k = active.i + active.j - 1
        remaining = length(active.out) - k + 1
        remaining > MERGE_STEP && return (0, 0)        # the current merge still owns the scheduler
        completed = true
        at = a.mergeat
        merged = checked_add(length(active.x), length(active.y))
        released = 8 * length(active.out)
        nruns -= 1
    end
    for i in nruns - 1:-1:1
        left = completed ? completedrunlength(a, i, at, merged) : length(a.runs[i])
        right = completed ? completedrunlength(a, i + 1, at, merged) : length(a.runs[i + 1])
        left == right || continue
        return (checked_add(left, right), released)
    end
    if carry && nruns > 0
        right = completed ? completedrunlength(a, nruns, at, merged) : length(a.runs[nruns])
        right == RUN_BASE && return (2 * RUN_BASE, released)
    end
    return (0, released)
end

"""
    admit!(admission, s::AbstractString) -> Symbol

Admit `s` (if new, counting it against the table's budgets) and return `Symbol(s)`.
"""
function admit!(a::SymbolAdmission, s::AbstractString; budget::Union{Nothing,Budget}=nothing)
    nbytes0 = sizeof(s)                                # charged before any copy is made (round-2 D10)
    lock(a.lock) do
        if budget !== nothing                          # charge the state that this locked lookup will search (§4.4, R07)
            addcompare!(budget, checked_mul(max(nbytes0, 1), length(a.recent) + 34 * (length(a.runs) + 1)))
            a.merge === nothing || addcompare!(budget, 8 * MERGE_STEP)
        end
        if contains_unlocked(a, s)
            # A repeat may advance maintenance, but it must remain admissible after the table reaches
            # its byte ceiling. Defer a cascade that has no scratch headroom; source runs remain live
            # and searchable until a later admission can complete it.
            nextlen, released = nextmergeplan(a, false)
            if checked_add(a.bytes, 8 * nextlen - released) <= a.max_bytes
                scratch = nextlen > 0 && released > 0 ? Vector{String}(undef, nextlen) : nothing
                step_unlocked!(a, scratch)
            end
            return nothing
        end
        ncount = a.count + 1                           # every admission check precedes any mutation —
        ncount <= a.max_names || throw(LimitError(:max_names, ncount, a.max_names, :max_names, :decode))
        nbytes = checked_add(a.bytes, nbytes0 + 8)     # a rejected admission advances no maintenance
        nbytes <= a.max_bytes || throw(LimitError(:max_bytes, nbytes, a.max_bytes, :max_bytes, :decode))
        carry = length(a.recent) + 1 == RUN_BASE
        nextlen, released = nextmergeplan(a, carry)
        need = checked_add(nbytes, 8 * nextlen - released)
        need <= a.max_bytes || throw(LimitError(:max_bytes, need, a.max_bytes, :max_bytes, :decode))
        # Every allocation precedes any mutation (round-3 item 5): the retained copy, the carried run's
        # buffer and the next merge's output exist before the table changes, so a failure here leaves
        # the table unchanged.
        retained = String(s)
        carrybuf = carry ? Vector{String}(undef, RUN_BASE) : nothing
        willstage = nextlen > 0 && (released > 0 || (carry && a.merge === nothing))
        scratch = willstage ? Vector{String}(undef, nextlen) : nothing
        step_unlocked!(a, scratch)                     # maintenance advances only for accepted admissions
        push!(a.recent, retained)                      # into the prebuilt RUN_BASE capacity: never grows
        a.count = ncount
        a.bytes += nbytes0 + 8                         # increment: a completion in the step above released its overlap
        if carrybuf !== nothing
            copyto!(carrybuf, 1, a.recent, 1, RUN_BASE)
            sort!(carrybuf)
            carry_unlocked!(a, carrybuf, scratch)
        end
        return nothing
    end
    return Symbol(s)
end

function admit!(::Symbol, s::AbstractString; budget::Union{Nothing,Budget}=nothing)
    return Symbol(s)                                      # `:trusted` bypass (validated by callers)
end

"""
    admission(names) -> SymbolAdmission | Symbol

Normalise the `names=` keyword: a `SymbolAdmission` object, or `:trusted`.
"""
function admission(a::SymbolAdmission)
    return a
end

function admission(s::Symbol)
    s === :trusted || throw(ArgumentError("`names` must be an Avro.SymbolAdmission or :trusted, got :$s"))
    return s
end

function admission(x)
    throw(ArgumentError("`names` must be an Avro.SymbolAdmission or :trusted"))
end
