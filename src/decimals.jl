# Decimals.jl integration (plan §4.8, the `decimal` logical type).
#
# `Decimals.Decimal{P,S,T}` is an isbits fixed-scale decimal whose precision and scale are type
# parameters, so it is an exact target for a `decimal` schema: the unscaled coefficient is read straight
# out of the big-endian two's complement payload into `T` and written back the same way — no `BigInt`,
# no per-value allocation, on either side. The generic value model keeps `Avro.Decimal`/`Avro.WideDecimal`
# (plan §4.6 is a closed set); this file is the typed route beside it.

"The storage integer of a `Decimals.Decimal` target."
function decimalstorage(::Type{Decimals.Decimal{P,S,T}}) where {P,S,T}
    return T
end

"""
    fulldecimal(D) -> Decimals.Decimal{P,S,T}

`D` with its storage integer filled in when the target spells only `Decimal{P,S}`; the tier comes from
Decimals.jl's own construction, so Avro never restates that policy.
"""
function fulldecimal(::Type{D}) where {D<:Decimals.Decimal}
    return typeof(D(0))
end

"""
    decimalbound(T, precision) -> T

`10^precision − 1` as a `T`: the largest unscaled magnitude a decimal of `precision` digits may carry.
Never overflows — every caller has already checked that `precision` fits the target's own precision.
"""
function decimalbound(::Type{T}, precision::Int) where {T<:Integer}
    v = one(T)
    for _ in 1:precision
        v *= T(10)
    end
    return v - one(T)
end

"The `i`th payload byte counted from the most significant end (`little` reverses the payload, §4.9)."
@inline function payloadbyte(buf, p::DecimalPlan, start::Int, n::Int, i::Int)
    return @inbounds buf[p.little ? start + n - 1 - i : start + i]
end

"""
    decimalinteger(T, d, p, start, n) -> T

The `n` big-endian two's complement bytes at `start` as a sign-extended `T`. A payload wider than `T`
is accepted only when its leading bytes are pure sign extension; anything else is corrupt for this
schema and raises a `DataError`.
"""
function decimalinteger(::Type{T}, d::Decoder, p::DecimalPlan, start::Int, n::Int) where {T<:Integer}
    buf = d.buf
    w = sizeof(T)
    n > w && checksignbytes(T, d, buf, p, start, n)
    v = zero(T)
    @inbounds for i in (n > w ? n - w : 0):n - 1
        v = (v << 8) | T(payloadbyte(buf, p, start, n, i))
    end
    if n < w
        shift = 8 * (w - n)
        v = (v << shift) >> shift
    end
    return v
end

"The leading `n − sizeof(T)` bytes of an over-wide payload must repeat the sign of the byte below them."
function checksignbytes(::Type{T}, d::Decoder, buf, p::DecimalPlan, start::Int, n::Int) where {T<:Integer}
    w = sizeof(T)
    sign = payloadbyte(buf, p, start, n, 0) >= 0x80 ? 0xff : 0x00
    for i in 0:n - w - 1
        payloadbyte(buf, p, start, n, i) == sign || dataerror(d, "decimal of $n bytes does not fit $T")
    end
    (payloadbyte(buf, p, start, n, n - w) >= 0x80) == (sign == 0xff) ||
        dataerror(d, "decimal of $n bytes does not fit $T")
    return nothing
end

# ---- typed reads --------------------------------------------------------------------------------------

"A `decimal` decoded straight into `D = Decimals.Decimal{P,S,T}`; `maxmag` is the schema's precision bound."
struct DecimalTarget{D,T} <: TypedPlan
    plan::DecimalPlan
    maxmag::T
end

"""
    decimaltarget(D, p, memo) -> DecimalTarget or nothing

Admit `D = Decimals.Decimal{P,S,T}` for the decimal plan `p`. The scale must match the schema exactly —
a differing scale is a different number, never a silent rescale — and `P` must cover the schema's
precision, so every value the schema admits is representable in `D` (the same "the target loses nothing"
rule as `float → Float64`). Anything else falls back to the semantic route.
"""
function decimaltarget(::Type{D0}, p::DecimalPlan, memo::TypedMemo) where {D0<:Decimals.Decimal}
    Decimals.scale(D0) == p.scale || return nothing
    Base.precision(D0) >= p.precision || return nothing
    D = fulldecimal(D0)
    T = decimalstorage(D)
    reservenode!(memo, DecimalTarget{D,T})
    out = DecimalTarget{D,T}(p, decimalbound(T, p.precision))
    settlenode!(memo, DecimalTarget{D,T})
    return out
end

function typedvalue(p::DecimalTarget{D,T}, d::Decoder, names) where {D,T}
    start, n = decimalspan(p.plan, d)
    u = decimalinteger(T, d, p.plan, start, n)
    -p.maxmag <= u <= p.maxmag || dataerror(d, "decimal exceeds precision $(p.plan.precision)")
    return reinterpret(D, u)
end

"""
    StructUtils.structlike(::AvroStyle, ::Type{<:Decimals.Decimal})

A decimal is one number, not a one-field struct: the semantic route must `lift` it from the generic
decimal rather than feed a coefficient to `Decimal{P,S,T}(x)` (whose argument is a *value*, not an
unscaled coefficient — decomposing it would silently rescale).
"""
function StructUtils.structlike(::AvroStyle, ::Type{<:Decimals.Decimal})
    return false
end

"""
    StructUtils.lift(D, x)

The semantic route into a `Decimals.Decimal`: whatever produced an `Avro.Decimal`/`Avro.WideDecimal`
(a promoted `string → bytes` decimal, a JSON datum, a struct outside the fast route) converts under the
same exactness rule the typed route admits by.
"""
function StructUtils.lift(::Type{D0}, x::Union{Decimal,WideDecimal}) where {D0<:Decimals.Decimal}
    D = fulldecimal(D0)
    x.scale == Decimals.scale(D) ||
        throw(ConversionError("decimal scale $(x.scale) does not equal the scale of $D"))
    u = Decimals.unscaled(typemax(D))
    -u <= x.unscaled <= u || throw(ConversionError("decimal $(x.unscaled)e-$(x.scale) does not fit $D"))
    return reinterpret(D, decimalstorage(D)(x.unscaled))
end

# ---- writes -------------------------------------------------------------------------------------------

"`u` as a byte index from the least significant end."
@inline function unscaledbyte(u::T, i::Int) where {T<:Integer}
    return UInt8((u >> (8 * i)) & T(0xff))
end

"""
    twoscomplementlength(u) -> Int

The minimal number of bytes of the big-endian two's complement of `u`: the leading bytes of `u`'s
storage are dropped while they only repeat the sign of the byte below them.
"""
function twoscomplementlength(u::T) where {T<:Integer}
    n = sizeof(T)
    while n > 1
        top = unscaledbyte(u, n - 1)
        below = unscaledbyte(u, n - 2)
        ((top == 0x00 && below < 0x80) || (top == 0xff && below >= 0x80)) || break
        n -= 1
    end
    return n
end

"The minimal big-endian two's complement of `u` as a fresh vector (the JSON encoding's byte string)."
function twoscomplementbytes(u::T) where {T<:Integer}
    n = twoscomplementlength(u)
    out = Vector{UInt8}(undef, n)
    @inbounds for i in 1:n
        out[i] = unscaledbyte(u, n - i)
    end
    return out
end

"Write the minimal big-endian two's complement of `u` as a length-prefixed `bytes` payload."
function writedecimalbytes!(e::Encoder, u::T) where {T<:Integer}
    n = twoscomplementlength(u)
    n <= e.budget.limits.max_bytes || throw(LimitError(:max_bytes, n, e.budget.limits.max_bytes, :max_bytes, :encode))
    writelong!(e, Int64(n))
    ensureroom!(e, n)
    p = e.pos
    @inbounds for i in 1:n
        e.buf[p + i] = unscaledbyte(u, n - i)
    end
    e.pos = p + n
    return nothing
end

"Write `u` sign-extended to exactly `size` bytes."
function writedecimalfixed!(e::Encoder, u::T, size::Int) where {T<:Integer}
    ensureroom!(e, size)
    pad = u < zero(T) ? 0xff : 0x00
    p = e.pos
    @inbounds for i in 1:size
        k = size - i
        e.buf[p + i] = k < sizeof(T) ? unscaledbyte(u, k) : pad
    end
    e.pos = p + size
    return nothing
end

function encodevalue(p::WDecimal, e::Encoder, x::Decimals.Decimal)
    Decimals.scale(x) == p.scale ||
        encodeerror("decimal scale $(Decimals.scale(x)) does not equal the schema scale $(p.scale) (rescale first)", x)
    u = Decimals.unscaled(x)
    Base.precision(typeof(x)) <= p.precision || checkdecimalprecision(p, x, u)
    p.fixedsize == 0 && return writedecimalbytes!(e, u)
    twoscomplementlength(u) <= p.fixedsize ||
        encodeerror("decimal does not fit a fixed of $(p.fixedsize) bytes", x)
    return writedecimalfixed!(e, u, p.fixedsize)
end

"A value whose own precision exceeds the schema's is checked against the schema's bound (plan §4.8)."
function checkdecimalprecision(p::WDecimal, x, u::T) where {T<:Integer}
    bound = decimalbound(T, p.precision)
    -bound <= u <= bound || encodeerror("decimal exceeds precision $(p.precision)", x)
    return nothing
end

function accepts(p::WDecimal, x::Decimals.Decimal)
    return Decimals.scale(x) == p.scale
end

function printkind(out::JSONOut, ::BytesSchema, v::Decimals.Decimal, depth, budget)
    return printbytestring(out.io, twoscomplementbytes(Decimals.unscaled(v)))
end

function printkind(out::JSONOut, s::FixedSchema, v::Decimals.Decimal, depth, budget)
    return printbytestring(out.io, padtwoscomplement(twoscomplementbytes(Decimals.unscaled(v)), s.size))
end

# ---- schema derivation --------------------------------------------------------------------------------

"""
Derive the schema of a `Decimals.Decimal{P,S,T}`: `bytes` annotated `decimal(P, S)`. `bytes` — not
`fixed(n)` — because `fixed` is a *named* type: deriving one would have to synthesise an Avro name per
(precision, scale), which collides across storage types, pollutes the namespace of every enclosing
record, and makes `Avro.inspect` report the 1.x fixed-decimal misframing warning for schemas Avro.jl
itself produced. `bytes` is unnamed, is what Java/fastavro/avro-python derive, and its minimal-length
payload is canonical per value. Writing into an explicit `fixed(n)` decimal schema is fully supported.
"""
function derivedecimal(::Type{D}) where {D<:Decimals.Decimal}
    l = DecimalLogical(Base.precision(D), Decimals.scale(D))
    return BytesSchema(l, makeprops((;), SCHEMA_GRAMMAR[:bytes], l), NodeMeta())
end
