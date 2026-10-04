# bzip2 codec adapter (plan §4.9): multi-stream payloads decoded to exact exhaustion; the decoder's
# fixed requirement (≤ 3.7 MiB for the 900 KiB block format) is reserved per stream.
module AvroCodecBzip2Ext

using Avro, CodecBzip2

const BZIP2_DECODER_BYTES = 3_700_000
const BZIP2_ENCODER_BYTES = 7_600_000

function Avro.compresscapacity(::Val{:bzip2}, n::Int)
    return Avro.checked_add(Avro.checked_add(n, cld(n, 100)), 600)
end

struct Bzip2Reader end

function Avro.decompressblock(name::Symbol, ::Bzip2Reader, payload::AbstractVector{UInt8}, limits::Avro.Limits, budget::Avro.Budget)
    checkpoint = Avro.budgetcheckpoint(budget)
    try
        total = length(payload)
        total > 0 || throw(Avro.CodecError(:bzip2, :decompress, "bzip2 payload has no stream"))
        out = Avro.initialoutput(budget, total, limits.max_block_bytes)
        outlen = 0
        pos = 1
        while pos <= total
            Avro.addmembers!(budget)
            Avro.reserve!(budget, BZIP2_DECODER_BYTES)        # settled inside transcodemember! once initialize runs
            before = outlen
            consumed, out, outlen = Avro.transcodemember!(:bzip2, Bzip2Decompressor(), payload, pos, total, out, outlen, limits.max_block_bytes, budget, BZIP2_DECODER_BYTES)
            Avro.release!(budget, BZIP2_DECODER_BYTES)
            consumed == 0 && throw(Avro.CodecError(:bzip2, :decompress, "invalid bzip2 stream at payload byte $pos"))
            Avro.addinput!(budget, outlen - before)
            pos += consumed
        end
        return Avro.shrinkexact(budget, out, outlen)
    catch
        Avro.rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function bzip2writer(level, limits::Avro.Limits)
    lvl = level === nothing ? 9 : Int(level)
    1 <= lvl <= 9 || throw(ArgumentError("the bzip2 level must be 1–9, got $lvl"))
    return Avro.WriterCodec(:bzip2, () -> Bzip2Compressor(blocksize100k=lvl), BZIP2_ENCODER_BYTES, 0)
end

function __init__()
    Avro.registercodec!(:bzip2, (reader=limits -> Bzip2Reader(), writer=bzip2writer))
    return nothing
end

end # module
