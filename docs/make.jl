using PerformanceAnalytics
using Documenter

DocMeta.setdocmeta!(PerformanceAnalytics, :DocTestSetup, :(using PerformanceAnalytics); recursive=true)

makedocs(;
    modules=[PerformanceAnalytics],
    authors="georgeg <georgegi86@gmail.com> and contributors",
    sitename="PerformanceAnalytics.jl",
    format=Documenter.HTML(;
        canonical="https://georgegee23.github.io/PerformanceAnalytics.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/georgegee23/PerformanceAnalytics.jl",
    devbranch="master",
)
