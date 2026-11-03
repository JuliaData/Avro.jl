# Test types shared by several test files.

# An iterable whose declared length disagrees with the items it yields.
struct ArrayLengthMismatch
    declared::Int
    actual::Int
end

function Base.IteratorSize(::Type{ArrayLengthMismatch})
    return Base.HasLength()
end

function Base.length(x::ArrayLengthMismatch)
    return x.declared
end

function Base.iterate(x::ArrayLengthMismatch, state::Int=1)
    state <= x.actual || return nothing
    return (Int64(state), state + 1)
end
