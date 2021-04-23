using Avro
using Documenter

DocMeta.setdocmeta!(Avro, :DocTestSetup, :(using Avro); recursive=true)

makedocs(;
    modules=[Avro],
    authors="Jacob Quinn <quinn.jacobd@gmail.com> and contributors",
    repo="https://github.com/JuliaData/Avro.jl/blob/{commit}{path}#{line}",
    sitename="Avro.jl",
    format=Documenter.HTML(;
        edit_link="main",
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://JuliaData.github.io/Avro.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/JuliaData/Avro.jl",
    devbranch = "main",
)
