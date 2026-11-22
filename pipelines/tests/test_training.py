"""S2: the model assets end to end, on synthetic trips, a local artifact dir and a SQLite MLflow."""

import dagster as dg
from mlflow import MlflowClient

from labpipes.contracts import mlflow_contract
from labpipes.defs import training
from labpipes.resources import MlflowTracking
from fakes import LocalArtifacts, unloading_redshift


def _run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # MLflow writes SQLite-backed artifacts under ./mlruns
    (tmp_path / "art").mkdir(exist_ok=True)
    uri = f"sqlite:///{tmp_path}/mlflow.db"
    resources = {
        "redshift": unloading_redshift(str(tmp_path)),
        "artifacts": LocalArtifacts(bucket="art", root=str(tmp_path)),
        "tracking": MlflowTracking(tracking_uri=uri),
    }
    result = dg.materialize(
        [dg.AssetSpec("trip_features"), training.training_set, training.trained_model, training.eval_report,
         training.challenger, training.beats_champion],
        selection=dg.AssetSelection.assets("training_set", "trained_model", "eval_report", "challenger"),
        resources=resources,
        raise_on_error=False,
    )
    return result, MlflowClient(uri)


def test_train_registers_tags_and_sets_only_challenger(tmp_path, monkeypatch):
    result, client = _run(tmp_path, monkeypatch)
    assert result.success
    gate = result.get_asset_check_evaluations()
    assert [e.check_name for e in gate] == ["beats_champion"] and gate[0].passed
    contract = mlflow_contract()
    name = contract["registered_model"]

    challenger = client.get_model_version_by_alias(name, "challenger")
    missing = set(contract["version_tags"]) - set(challenger.tags)
    assert not missing, missing
    assert challenger.tags["training_window"] == "2024-01..2024-03"
    assert challenger.tags["holdout_month"] == "2024-04"
    assert "champion" not in client.get_registered_model(name).aliases

    run_metrics = client.get_run(challenger.run_id).data.metrics
    assert {"mae_min", "p95_latency_ms", "champion_mae_min"} <= set(run_metrics)
    assert run_metrics["mae_min"] < run_metrics["champion_mae_min"]
