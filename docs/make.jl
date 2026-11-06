using Documenter, Avro

makedocs(;
    modules=[Avro],
    sitename="Avro.jl",
    format=Documenter.HTML(; size_threshold=2_000_000, size_threshold_warn=1_000_000),
    authors="Jacob Quinn and contributors",
    checkdocs=:public,
    warnonly=false,
    pages=[
        "Home" => "index.md",
        "Manual" => [
            "manual/schemas.md",
            "manual/encoding.md",
            "manual/container.md",
            "manual/tables.md",
            "manual/evolution.md",
            "manual/logicaltypes.md",
            "manual/singleobject.md",
            "manual/sortorder.md",
            "manual/limits-and-security.md",
            "manual/performance.md",
        ],
        "Migration from 1.x" => "migration.md",
        "Benchmarks" => "benchmarks.md",
        "Reference" => "reference.md",
    ],
)

deploydocs(; repo="github.com/JuliaData/Avro.jl", devbranch="main", push_preview=true)
