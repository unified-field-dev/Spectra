use anyhow::{bail, Context, Result};
use serde_json::json;
use spectra_testkit::{
    DriverKind, MatrixSpec, ScenarioRunner, ScenarioSpec, StorageAdapter, Topology,
};

use crate::capacity::{self, CapacityRunArgs};
use crate::cli::CliMatrix;
use crate::experiments::{ExperimentTrack, REGISTRY};
use crate::stats::summarize_timings;
use crate::sweep::{SweepCli, SweepParams};

pub struct RunArgs {
    pub experiment: String,
    pub matrix: CliMatrix,
    pub sweep: SweepCli,
    pub report: Option<std::path::PathBuf>,
}

pub async fn run_experiment(args: RunArgs) -> Result<()> {
    let meta = crate::experiments::resolve_experiment(&args.experiment)
        .with_context(|| format!("unknown experiment {}", args.experiment))?;

    match meta.track {
        ExperimentTrack::Scenario => run_scenario(args, meta.id, meta.summary).await,
        ExperimentTrack::Write | ExperimentTrack::Query => {
            let sweep = SweepParams::resolve(&args.sweep);
            capacity::run_capacity(CapacityRunArgs {
                meta,
                matrix: args.matrix,
                sweep,
                report: args.report,
            })
            .await
        }
    }
}

async fn run_scenario(args: RunArgs, id: &str, summary: &str) -> Result<()> {
    let mut matrix = args.matrix.to_matrix_spec();
    let spec = scenario_for_experiment(id, &mut matrix)?;

    let result = ScenarioRunner::run(matrix.clone(), &spec, DriverKind::Benchmark).await?;

    let report = json!({
        "experiment": id,
        "summary": summary,
        "matrix": matrix.slug(),
        "scenario_id": result.scenario_id,
        "step_timings": result.step_timings.iter().map(|t| json!({
            "step_index": t.step_index,
            "op": t.op,
            "elapsed_ms": t.elapsed_ms,
        })).collect::<Vec<_>>(),
        "stats": summarize_timings(&result.step_timings),
        "error": result.error,
    });

    let pretty = serde_json::to_string_pretty(&report)?;
    println!("{pretty}");

    if let Some(path) = args.report {
        capacity::write_report(&path, &pretty)?;
    }

    Ok(())
}

fn scenario_for_experiment(id: &str, matrix: &mut MatrixSpec) -> Result<ScenarioSpec> {
    match id {
        "bm-s0" => {
            if matrix.storage != StorageAdapter::Mem || matrix.topology != Topology::Embedded {
                bail!("bm-s0 requires --storage mem --topology embedded");
            }
            Ok(ScenarioSpec::emit_only_bench())
        }
        "bm-s1" => {
            matrix.storage = StorageAdapter::Mem;
            matrix.topology = Topology::Embedded;
            Ok(ScenarioSpec::persist_roundtrip_bench())
        }
        "bm-s2" => {
            matrix.storage = StorageAdapter::Sqlite;
            matrix.topology = Topology::Embedded;
            Ok(ScenarioSpec::persist_roundtrip_bench())
        }
        "bm-s3" => {
            matrix.storage = StorageAdapter::Mem;
            matrix.topology = Topology::Embedded;
            Ok(ScenarioSpec::query_range_bench(10))
        }
        _ => bail!("unmapped scenario experiment {id}"),
    }
}

pub fn list_experiments() {
    for meta in REGISTRY {
        let track = match meta.track {
            ExperimentTrack::Scenario => "smoke",
            ExperimentTrack::Write => "write",
            ExperimentTrack::Query => "query",
        };
        println!("{}  [{track}]  {}", meta.id, meta.summary);
    }
    println!("Full catalog: docs/bench/PERFORMANCE.md (highlights); lab EXPERIMENTS for registry");
}
