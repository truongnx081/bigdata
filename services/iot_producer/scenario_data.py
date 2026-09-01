"""Load the IoT demo dataset and generate events for the configured scenarios."""

from __future__ import annotations

import json
import math
import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_SCENARIOS = {"normal", "rush_hour", "rain", "incident"}
DEFAULT_DATA_FILE = Path(__file__).resolve().parents[2] / "data" / "iot" / "scenario_config.json"
DATA_FILE = Path(os.getenv("IOT_DATA_FILE", str(DEFAULT_DATA_FILE)))


def load_config(path: Path = DATA_FILE) -> dict[str, Any]:
    config = json.loads(path.read_text(encoding="utf-8"))
    roads = config.get("roads")
    scenarios = config.get("scenarios")
    if not isinstance(roads, list) or not roads:
        raise ValueError("IoT scenario config must contain at least one road")
    if not isinstance(scenarios, dict) or set(scenarios) != REQUIRED_SCENARIOS:
        raise ValueError("IoT scenario config must define normal, rush_hour, rain and incident")

    road_ids = [str(road.get("road_id", "")) for road in roads]
    if any(not road_id for road_id in road_ids) or len(road_ids) != len(set(road_ids)):
        raise ValueError("Every IoT road must have a unique road_id")

    required_road_fields = {
        "road_id",
        "road_name",
        "latitude",
        "longitude",
        "free_speed_kmh",
        "capacity",
    }
    for road in roads:
        missing = required_road_fields - set(road)
        if missing:
            raise ValueError(f"Road {road.get('road_id', '?')} is missing: {sorted(missing)}")

    for name, scenario in scenarios.items():
        speed_factor = float(scenario["speed_factor"])
        density_factor = float(scenario["density_factor"])
        if not 0 < speed_factor <= 1 or not 0 < density_factor <= 1:
            raise ValueError(f"Invalid factors for scenario {name}")
    return config


CONFIG = load_config()
ROADS: list[dict[str, Any]] = CONFIG["roads"]
ROAD_INDEX = {road["road_id"]: index for index, road in enumerate(ROADS)}
SCENARIO_FACTORS = {
    name: (float(value["speed_factor"]), float(value["density_factor"]))
    for name, value in CONFIG["scenarios"].items()
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(
    road: dict[str, Any],
    scenario: str,
    cycle: int,
    incident_road: str | None,
    rng: random.Random,
) -> dict[str, Any]:
    speed_factor, density_factor = SCENARIO_FACTORS[scenario]
    affected = scenario != "incident" or road["road_id"] == incident_road
    if scenario == "incident" and not affected:
        speed_factor, density_factor = SCENARIO_FACTORS["normal"]

    wave = 0.06 * math.sin(cycle / 8 + ROAD_INDEX[road["road_id"]])
    density = max(4.0, min(98.0, (density_factor + wave + rng.gauss(0, 0.035)) * 100))
    speed = max(
        4.0,
        min(
            float(road["free_speed_kmh"]) + 8,
            float(road["free_speed_kmh"]) * speed_factor - density * 0.055 + rng.gauss(0, 2.2),
        ),
    )
    vehicle_count = max(1, round(float(road["capacity"]) * density / 100 + rng.gauss(0, 2)))
    return {
        "event_id": str(uuid.uuid4()),
        "source": "iot",
        "sensor_id": f"iot-{road['road_id']}",
        "road_id": road["road_id"],
        "road_name": road["road_name"],
        "latitude": float(road["latitude"]) + rng.uniform(-0.00035, 0.00035),
        "longitude": float(road["longitude"]) + rng.uniform(-0.00035, 0.00035),
        "timestamp": utc_now(),
        "speed_kmh": round(speed, 2),
        "density_pct": round(density, 2),
        "vehicle_count": vehicle_count,
        "occupancy_pct": round(density * rng.uniform(0.75, 0.98), 2),
        "scenario": scenario,
        "cycle": cycle,
        "quality": round(rng.uniform(0.96, 1.0), 3),
    }
