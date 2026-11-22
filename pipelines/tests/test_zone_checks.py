"""S1 decision 006: zone IDs block outside 1..265; the 264/265 share only warns."""

from types import SimpleNamespace

import dagster as dg

from fakes import FakeRedshift
from labpipes.defs.quality import unknown_location_share, zone_ids_in_range

# 2024-01 as it is in staging.stg_trips.
JAN = {"rows": 2_964_624, "nonpositive_duration": 0, "over_3h": 0, "null_passengers": 0,
       "invalid_pickup": 0, "invalid_dropoff": 0, "unknown_pickup": 12_018, "unknown_dropoff": 28_083}


class StatsRedshift(FakeRedshift):
    stats: dict

    def query(self, sql: str) -> list[dict]:
        return [self.stats]


def _evaluate(**overrides) -> dict:
    """Call each check's function directly; it only needs the partition key and the resource."""
    redshift = StatsRedshift(cluster_id="lab", database="lab", db_user="x", stats={**JAN, **overrides})
    context = SimpleNamespace(partition_key="2024-01")
    return {c.check_key.name: c.node_def.compute_fn.decorated_fn(context, redshift)
            for c in (zone_ids_in_range, unknown_location_share)}


def test_january_passes_both():
    ev = _evaluate()
    assert ev["zone_ids_in_range"].passed
    assert ev["unknown_location_share"].passed


def test_an_impossible_dropoff_blocks():
    ev = _evaluate(invalid_dropoff=1)
    assert not ev["zone_ids_in_range"].passed
    assert ev["zone_ids_in_range"].severity == dg.AssetCheckSeverity.ERROR


def test_a_gps_outage_only_warns():
    ev = _evaluate(unknown_pickup=60_000)  # about 2% of pickups
    assert ev["zone_ids_in_range"].passed
    assert not ev["unknown_location_share"].passed
    assert ev["unknown_location_share"].severity == dg.AssetCheckSeverity.WARN
