# xz codec adapter (plan §4.9): multi-stream payloads with the format's four-byte zero stream padding;
# liblzma enforces `max_codec_memory` as its decoder `memlimit` (the complete per-block requirement) but
# cannot report a block's requirement before allocating, so the workspace reservation is the cap itself.
# The writer's workspace and the emitted streams' decoder requirement come from the liblzma estimators.
module AvroCodecXzExt

using Avro, CodecXz, XZ_jll

struct XzReader
    memlimit::UInt64
end

function lzma_encoder_memusage(preset::Integer)
    return ccall((:lzma_easy_encoder_memusage, XZ_jll.liblzma), UInt64, (UInt32,), preset)
end

function lzma_decoder_memusage(preset::Integer)
    return ccall((:lzma_easy_decoder_memusage, XZ_jll.liblzma), UInt64, (UInt32,), preset)
end

function Avro.compresscapacity(::Val{:xz}, n::Int)
    bound = ccall((:lzma_stream_buffer_bound, XZ_jll.liblzma), Csize_t, (Csize_t,), n)
    return Avro.boundint(:xz, bound)
end

function Avro.decompressblock(name::Symbol, r::XzReader, payload::AbstractVector{UInt8}, limits::Avro.Limits, budget::Avro.Budget)
    checkpoint = Avro.budgetcheckpoint(budget)
    try
        total = length(payload)
        total > 0 || throw(Avro.CodecError(:xz, :decompress, "xz payload has no stream"))
        out = Avro.initialoutput(budget, total, limits.max_block_bytes)
        outlen = 0
        pos = 1
        sawstream = false
        while pos <= total
            if payload[pos] == 0x00
                sawstream || throw(Avro.CodecError(:xz, :decompress, "xz stream padding precedes the first stream"))
                run = 0
                while pos + run <= total && payload[pos + run] == 0x00
                    run += 1
                end
                (run % 4 == 0 && pos + run > total) || throw(Avro.CodecError(:xz, :decompress, "invalid xz stream padding at payload byte $pos"))
                break                                                  # trailing padding in multiples of four ends the payload
            end
            Avro.addmembers!(budget)
            Avro.reserve!(budget, limits.max_codec_memory)        # settled inside transcodemember! once initialize runs
            before = outlen
            consumed, out, outlen = Avro.transcodemember!(:xz, XzDecompressor(memlimit=r.memlimit, flags=UInt32(0)), payload, pos, total, out, outlen, limits.max_block_bytes, budget, limits.max_codec_memory)
            Avro.release!(budget, limits.max_codec_memory)
            consumed == 0 && throw(Avro.CodecError(:xz, :decompress, "invalid xz stream at payload byte $pos"))
            Avro.addinput!(budget, outlen - before)
            sawstream = true
            pos += consumed
            # inter-stream padding: maximal zero run in a multiple of four
            run = 0
            while pos + run <= total && payload[pos + run] == 0x00
                run += 1
            end
            if run > 0
                run % 4 == 0 || throw(Avro.CodecError(:xz, :decompress, "invalid xz stream padding at payload byte $pos"))
                pos += run
            end
        end
        return Avro.shrinkexact(budget, out, outlen)
    catch
        Avro.rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function xzwriter(level, limits::Avro.Limits)
    preset = level === nothing ? 6 : Int(level)
    0 <= preset <= 9 || throw(ArgumentError("the xz preset must be 0–9, got $preset"))
    workspace = Int(min(lzma_encoder_memusage(preset), UInt64(typemax(Int))))
    required = Int(min(lzma_decoder_memusage(preset), UInt64(typemax(Int))))
    required <= limits.max_codec_memory || throw(Avro.LimitError(:max_codec_memory, required, limits.max_codec_memory, :max_codec_memory, :encode))
    return Avro.WriterCodec(:xz, () -> XzCompressor(level=preset), workspace, 0)
end

function __init__()
    Avro.registercodec!(:xz, (reader=limits -> XzReader(UInt64(limits.max_codec_memory)), writer=xzwriter))
    return nothing
end

end # module
