using Avro
using Documenter

DocMeta.setdocmeta!(Avro, :DocTestSetup, :(using Avro); recursive=true)

makedocs(;
    modules=[Avro],
    authors="Jacob Quinn <quinn.jacobd@gmail.com> and contributors",
    repo="https://github.com/quinnj/Avro.jl/blob/{commit}{path}#{line}",
    sitename="Avro.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://quinnj.github.io/Avro.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/quinnj/Avro.jl",
)
