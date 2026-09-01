import csv
import random
from pathlib import Path

from services.common.traffic_rules import congestion_level
from services.iot_producer.scenario_data import CONFIG, ROADS, SCENARIO_FACTORS, make_event


ROOT = Path(__file__).resolve().parents[1]


def test_demo_config_matches_report_scenarios() -> None:
    assert len(ROADS) == 6
    assert SCENARIO_FACTORS == {
        "normal": (0.90, 0.33),
        "rush_hour": (0.55, 0.68),
        "rain": (0.68, 0.57),
        "incident": (0.22, 0.91),
    }
    assert CONFIG["incident_duration_seconds"] == 30


def test_generated_event_has_iot_demo_schema() -> None:
    event = make_event(ROADS[0], "normal", 7, None, random.Random(2026))
    required = {
        "event_id",
        "source",
        "sensor_id",
        "road_id",
        "road_name",
        "latitude",
        "longitude",
        "timestamp",
        "speed_kmh",
        "density_pct",
        "vehicle_count",
        "occupancy_pct",
        "scenario",
        "cycle",
        "quality",
    }
    assert required <= set(event)
    assert event["source"] == "iot"
    assert event["scenario"] == "normal"
    assert event["cycle"] == 7


def test_incident_only_changes_selected_road() -> None:
    selected = ROADS[0]
    unaffected = ROADS[1]

    incident_event = make_event(selected, "incident", 10, selected["road_id"], random.Random(11))
    selected_normal = make_event(selected, "normal", 10, None, random.Random(11))
    assert incident_event["speed_kmh"] < selected_normal["speed_kmh"]
    assert incident_event["density_pct"] > selected_normal["density_pct"]

    unaffected_incident = make_event(unaffected, "incident", 10, selected["road_id"], random.Random(22))
    unaffected_normal = make_event(unaffected, "normal", 10, None, random.Random(22))
    assert unaffected_incident["speed_kmh"] == unaffected_normal["speed_kmh"]
    assert unaffected_incident["density_pct"] == unaffected_normal["density_pct"]


def test_reference_csv_contains_6_roads_times_4_scenarios() -> None:
    with (ROOT / "data" / "iot" / "demo_scenarios.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 24
    assert {row["scenario"] for row in rows} == {"normal", "rush_hour", "rain", "incident"}
    assert all(
        congestion_level(float(row["speed_kmh"]), float(row["density_pct"])) == row["expected_state"]
        for row in rows
    )
