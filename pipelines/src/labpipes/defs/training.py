"""S2: training_set, trained_model, eval_report, beats_champion gate and challenger asset.

- training_set: UNLOAD training window (e.g. 2024-01..03) and holdout month (2024-04) from
  staging.trip_features to the artifact bucket. The distance lookup is computed on the training window.
- trained_model: fit tripmodel.LightGBMModel (v2), log it to MLflow as a pyfunc with model_signature()
  and bundled tripmodel code, register a version under 'trip-duration', and tag it.
- eval_report: evaluate the new candidate against champion (or v1 baseline if no champion yet) on holdout.
- beats_champion: blocking check ensuring candidate beats champion MAE within latency budget.
- challenger: sets the 'challenger' alias in MLflow. Pipeline NEVER sets 'champion'.
"""

import os
from pathlib import Path

import dagster as dg
import mlflow
import mlflow.pyfunc
from mlflow.exceptions import MlflowException

import tripmodel
from tripmodel import BaselineModel, LightGBMModel, evaluate, model_signature

from ..contracts import features_contract, mlflow_contract
from ..partitions import check_month
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData


def _get_window_and_holdout(redshift: RedshiftData) -> tuple[list[str], str, str]:
    """Returns (train_months, holdout_month, window_label)."""
    rows = redshift.query("select distinct trip_month from staging.trip_features order by trip_month")
    months = [r["trip_month"] for r in rows] if rows else ["2024-01", "2024-02", "2024-03", "2024-04"]
    if len(months) < 2:
        return months, months[0], f"{months[0]}..{months[0]}"
    train_months = months[:-1]
    holdout_month = months[-1]
    window_label = f"{train_months[0]}..{train_months[-1]}"
    return train_months, holdout_month, window_label


@dg.asset(
    deps=["trip_features"],
    group_name="model",
    kinds={"redshift", "s3"},
    pool="redshift",
)
def training_set(
    redshift: RedshiftData,
    artifacts: ArtifactBucket,
) -> dg.MaterializeResult:
    """UNLOAD train window, holdout month, and zone-pair distances to the artifact bucket."""
    train_months, holdout_month, window = _get_window_and_holdout(redshift)
    for m in [*train_months, holdout_month]:
        check_month(m)  # formatted into SQL below
    # Inside UNLOAD ('...') the query is itself a string literal, so its quotes are doubled ('');
    # a plain query uses single quotes. Mixing the two is a Redshift syntax error.
    train_in = ", ".join(f"''{m}''" for m in train_months)
    train_in_plain = ", ".join(f"'{m}'" for m in train_months)

    bucket = artifacts.bucket
    # UNLOAD statements into artifact bucket
    redshift.execute(
        # 1. Training set trips
        f"unload ('select pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min "
        f"from staging.trip_features where trip_month in ({train_in})') "
        f"to 's3://{bucket}/training/{window}/train/' iam_role default format as parquet allowoverwrite",
        # 2. Holdout month trips
        f"unload ('select pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min "
        f"from staging.trip_features where trip_month in (''{holdout_month}'')') "
        f"to 's3://{bucket}/training/{window}/holdout/' iam_role default format as parquet allowoverwrite",
        # 3. Zone-pair distance medians over the training window
        f"unload ('select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles "
        f"from staging.stg_trips where trip_month in ({train_in}) "
        f"group by pickup_zone_id, dropoff_zone_id') "
        f"to 's3://{bucket}/training/{window}/distances/' iam_role default format as parquet allowoverwrite",
    )

    counts = redshift.query(
        f"select count(case when trip_month in ({train_in_plain}) then 1 end) as train, "
        f"count(case when trip_month = '{holdout_month}' then 1 end) as holdout "
        "from staging.trip_features"
    )
    train_count = int(counts[0]["train"]) if counts else 0
    holdout_count = int(counts[0]["holdout"]) if counts else 0

    return dg.MaterializeResult(
        metadata={
            "training_window": window,
            "holdout_month": holdout_month,
            "train_rows": dg.MetadataValue.int(train_count),
            "holdout_rows": dg.MetadataValue.int(holdout_count),
        }
    )


@dg.asset(
    deps=[training_set],
    group_name="model",
    kinds={"mlflow"},
)
def trained_model(
    redshift: RedshiftData,
    artifacts: ArtifactBucket,
    tracking: MlflowTracking,
) -> dg.MaterializeResult:
    """Fit LightGBM model, log to MLflow, and register a new version."""
    train_months, holdout_month, window = _get_window_and_holdout(redshift)
    train_df = artifacts.read_parquet(f"training/{window}/train")
    dist_df = artifacts.read_parquet(f"training/{window}/distances")

    model = LightGBMModel().fit(train_df, dist_df, num_rounds=300)

    client = tracking.client()
    contract = mlflow_contract()
    exp_name = contract["experiment"]
    model_name = contract["registered_model"]

    git_sha = os.environ.get("GIT_SHA", "dev")

    mlflow.set_experiment(exp_name)
    with mlflow.start_run(run_name=f"train-{window}") as run:
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=model,
            signature=model_signature(),
            code_paths=[str(Path(tripmodel.__file__).parent)],
            registered_model_name=model_name,
        )

    version = info.registered_model_version
    client.set_model_version_tag(model_name, version, "training_window", window)
    client.set_model_version_tag(model_name, version, "holdout_month", holdout_month)
    client.set_model_version_tag(model_name, version, "features_version", str(features_contract()["version"]))
    client.set_model_version_tag(model_name, version, "git_sha", git_sha)

    # Persist the version number for downstream eval_report
    artifacts.write_json(f"training/{window}/latest_version.json", {"version": version, "run_id": run.info.run_id})

    return dg.MaterializeResult(
        metadata={
            "model_version": version,
            "run_id": run.info.run_id,
            "training_window": window,
        }
    )


@dg.asset(
    deps=[trained_model, "taxi_zones"],  # boroughs for the airport/manhattan/outer slices
    group_name="model",
    kinds={"mlflow"},
)
def eval_report(
    redshift: RedshiftData,
    artifacts: ArtifactBucket,
    tracking: MlflowTracking,
) -> dg.MaterializeResult:
    """Evaluate candidate model and compare against champion (or v1 baseline)."""
    train_months, holdout_month, window = _get_window_and_holdout(redshift)
    holdout_df = artifacts.read_parquet(f"training/{window}/holdout")
    dist_df = artifacts.read_parquet(f"training/{window}/distances")
    train_df = artifacts.read_parquet(f"training/{window}/train")

    meta = artifacts.read_json(f"training/{window}/latest_version.json")
    version = str(meta["version"])

    zones_rows = redshift.query("select location_id, borough from ingest.taxi_zones")
    boroughs = {int(r["location_id"]): str(r["borough"]) for r in zones_rows} if zones_rows else {}

    client = tracking.client()
    contract = mlflow_contract()
    model_name = contract["registered_model"]

    # 1. Evaluate candidate model. A loaded pyfunc's predict takes (data); the (None, df) form is
    #    only for a raw PythonModel object (see tripmodel.evaluate's docstring).
    candidate_model = mlflow.pyfunc.load_model(f"models:/{model_name}/{version}")
    metrics = evaluate(candidate_model.predict, holdout_df, boroughs)

    # 2. Evaluate champion on the same holdout, or the v1 baseline before there is a champion.
    #    Only a missing alias falls back; any other MLflow error fails the run.
    try:
        client.get_model_version_by_alias(model_name, "champion")
        has_champion = True
    except MlflowException as e:
        # MLflow reports a missing alias as INVALID_PARAMETER_VALUE "alias champion not found".
        if "alias champion not found" not in str(e).lower():
            raise
        has_champion = False

    if has_champion:
        champ_model = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")
        champion_mae = evaluate(champ_model.predict, holdout_df, boroughs)["mae_min"]
        compared_with = "champion"
    else:
        base_model = BaselineModel().fit(train_df, dist_df)
        champion_mae = evaluate(lambda df: base_model.predict(None, df), holdout_df, boroughs)["mae_min"]
        compared_with = "v1_baseline"

    # 3. Log every contract metric on the training run, and copy the tagged ones to the version.
    run_metrics = {**metrics, "champion_mae_min": champion_mae}
    with mlflow.start_run(run_id=meta["run_id"]):
        mlflow.log_metrics({k: v for k, v in run_metrics.items() if k in contract["metrics"]})
        mlflow.set_tag("compared_with", compared_with)
    client.set_model_version_tag(model_name, version, "mae_min", f"{metrics['mae_min']:.4f}")
    client.set_model_version_tag(model_name, version, "p95_latency_ms", f"{metrics['p95_latency_ms']:.2f}")
    client.set_model_version_tag(model_name, version, "champion_mae_min", f"{champion_mae:.4f}")

    # Persist eval summary for beats_champion check and challenger asset
    eval_summary = {
        "version": version,
        "mae_min": metrics["mae_min"],
        "champion_mae_min": champion_mae,
        "p95_latency_ms": metrics["p95_latency_ms"],
        "beats": metrics["mae_min"] < champion_mae,
    }
    artifacts.write_json(f"training/{window}/eval_summary.json", eval_summary)

    return dg.MaterializeResult(
        metadata={
            "mae_min": dg.MetadataValue.float(metrics["mae_min"]),
            "champion_mae_min": dg.MetadataValue.float(champion_mae),
            "p95_latency_ms": dg.MetadataValue.float(metrics["p95_latency_ms"]),
            "beats_champion": dg.MetadataValue.bool(metrics["mae_min"] < champion_mae),
            "compared_with": compared_with,
            **{k: dg.MetadataValue.float(v) for k, v in metrics.items() if k.startswith("mae_min.")},
        }
    )


@dg.asset_check(
    asset=eval_report,
    blocking=True,
    description="Blocking gate: Candidate beats champion MAE and stays within p95 latency budget (<50ms).",
)
def beats_champion(
    redshift: RedshiftData,
    artifacts: ArtifactBucket,
) -> dg.AssetCheckResult:
    _, _, window = _get_window_and_holdout(redshift)
    summary = artifacts.read_json(f"training/{window}/eval_summary.json")

    mae = summary["mae_min"]
    champ_mae = summary["champion_mae_min"]
    p95 = summary["p95_latency_ms"]

    beats = mae < champ_mae
    within_latency = p95 <= 50.0  # 50ms latency budget
    passed = beats and within_latency

    desc = f"Candidate MAE {mae:.4f} vs champion {champ_mae:.4f}, p95 {p95:.2f}ms"
    if not beats:
        desc = f"Candidate MAE {mae:.4f} does not beat champion {champ_mae:.4f}"
    elif not within_latency:
        desc = f"Candidate latency {p95:.2f}ms exceeds 50ms budget"

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=desc,
        metadata={
            "candidate_mae": dg.MetadataValue.float(mae),
            "champion_mae": dg.MetadataValue.float(champ_mae),
            "p95_latency_ms": dg.MetadataValue.float(p95),
        },
    )


@dg.asset(
    deps=[eval_report],
    group_name="model",
    kinds={"mlflow"},
)
def challenger(
    redshift: RedshiftData,
    artifacts: ArtifactBucket,
    tracking: MlflowTracking,
) -> dg.MaterializeResult:
    """Sets the 'challenger' alias on the verified model version. Never sets champion."""
    _, _, window = _get_window_and_holdout(redshift)
    summary = artifacts.read_json(f"training/{window}/eval_summary.json")
    version = str(summary["version"])

    client = tracking.client()
    contract = mlflow_contract()
    model_name = contract["registered_model"]

    client.set_registered_model_alias(name=model_name, alias="challenger", version=version)

    return dg.MaterializeResult(
        metadata={
            "registered_model": model_name,
            "alias": "challenger",
            "version": version,
        }
    )


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets(
        training_set, trained_model, eval_report, challenger
    ) | dg.AssetSelection.checks_for_assets(eval_report),
)
