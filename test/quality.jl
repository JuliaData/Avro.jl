# Quality gates (plan §9.14): run with AVRO_QUALITY_GATES=true.
using Aqua
using JET

@testset "Aqua" begin
    Aqua.test_all(Avro; ambiguities=false, project_extras=true, deps_compat=true, stale_deps=true,
        piracies=true, unbound_args=true, undefined_exports=true, persistent_tasks=false)
    Aqua.test_ambiguities(Avro)
end

@testset "JET" begin
    rep = JET.report_package(Avro; target_modules=(Avro,))
    @test isempty(JET.get_reports(rep))
end
