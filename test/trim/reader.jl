# A minimal Avro reader program for the `juliac --trim` smoke: no eval, no runtime Symbol lookups.
module TrimReader

using Avro

function (@main)(args)
    length(args) == 1 || (println("usage: reader <file.avro>"); return 1)
    n = Avro.Reader(args[1]) do r
        count = 0
        for (c, _) in Avro.eachblock(r)
            count += c
        end
        count
    end
    println(n)
    return 0
end

end
