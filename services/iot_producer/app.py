"""Ứng dụng nguồn IoT giả lập độc lập, có web điều khiển và phát dữ liệu vào Kafka."""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from services.iot_producer.scenario_data import CONFIG, ROADS, SCENARIO_FACTORS, make_event, utc_now


logging.basicConfig(level=logging.INFO, format="%(asctime)s | IoT | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("iot-source")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = "traffic.iot.raw"
PROCESSED_TOPIC = "traffic.iot.processed"
STATUS_TOPIC = "traffic.source.status"
INTERVAL_SECONDS = max(0.2, float(os.getenv("IOT_INTERVAL_SECONDS", "1")))
AI_RESULT_STALE_SECONDS = max(4.0, float(os.getenv("AI_RESULT_STALE_SECONDS", "12")))
RANDOM_SEED = int(os.getenv("IOT_RANDOM_SEED", str(CONFIG["random_seed"])))
INCIDENT_DURATION_SECONDS = int(os.getenv("IOT_INCIDENT_DURATION_SECONDS", str(CONFIG["incident_duration_seconds"])))
STATIC_DIR = Path(__file__).resolve().parent / "static"

LEVEL_RANK = {"smooth": 0, "moderate": 1, "heavy": 2, "critical": 3}

def encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


@dataclass
class SourceState:
    active: bool = True
    scenario: str = "normal"
    temporary_until: float | None = None
    previous_scenario: str = "normal"
    incident_road: str | None = None

    def set_scenario(self, scenario: str, incident_road: str | None, duration: float | None = None) -> None:
        if scenario not in SCENARIO_FACTORS:
            return
        self.previous_scenario = self.scenario if self.scenario != "incident" else "normal"
        self.scenario = scenario
        self.incident_road = incident_road if scenario == "incident" else None
        self.temporary_until = time.monotonic() + duration if duration else None

    def tick(self) -> None:
        if self.temporary_until and time.monotonic() >= self.temporary_until:
            self.scenario = self.previous_scenario
            self.temporary_until = None
            self.incident_road = None


class IoTRuntime:
    def __init__(self) -> None:
        self.rng = random.Random(RANDOM_SEED)
        scenario = os.getenv("IOT_SCENARIO", "normal")
        scenario = scenario if scenario in SCENARIO_FACTORS else "normal"
        self.state = SourceState()
        incident_road = self.rng.choice(ROADS)["road_id"] if scenario == "incident" else None
        self.state.set_scenario(scenario, incident_road)
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.kafka_connected = False
        self.total_events = 0
        self.cycles = 0
        self.last_publish_at: str | None = None
        self.last_error: str | None = None
        self.latest_events: list[dict[str, Any]] = []
        self.processed_roads: dict[str, tuple[float, dict[str, Any]]] = {}

    def start(self) -> None:
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, name="iot-kafka-worker", daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=6)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            remaining = None
            if self.state.temporary_until:
                remaining = max(0, round(self.state.temporary_until - time.monotonic()))
            analysis = self._analysis_snapshot()
            return {
                "source": "iot",
                "port": 8101,
                "active": self.state.active,
                "scenario": self.state.scenario,
                "temporary_seconds": remaining,
                "incident_road": self.state.incident_road,
                "kafka_connected": self.kafka_connected,
                "total_events": self.total_events,
                "cycles": self.cycles,
                "events_per_cycle": len(ROADS),
                "interval_seconds": INTERVAL_SECONDS,
                "last_publish_at": self.last_publish_at,
                "last_error": self.last_error,
                "roads": list(self.latest_events),
                "analysis": analysis,
            }

    def _analysis_snapshot(self) -> dict[str, Any]:
        if not self.state.active:
            return {"status": "stopped", "summary": None, "alerts": [], "roads": []}

        now = time.monotonic()
        expired = [road_id for road_id, (received_at, _) in self.processed_roads.items() if now - received_at > AI_RESULT_STALE_SECONDS]
        for road_id in expired:
            self.processed_roads.pop(road_id, None)
        roads = [dict(payload) for _, payload in self.processed_roads.values()]
        if not roads:
            return {"status": "waiting", "summary": None, "alerts": [], "roads": []}

        roads.sort(key=lambda item: str(item.get("road_name", "")))
        worst = max(roads, key=lambda item: (LEVEL_RANK.get(str(item.get("congestion_level")), 0), float(item.get("risk_score") or 0)))
        alerts = [
            dict(item)
            for item in roads
            if str(item.get("congestion_level")) in {"heavy", "critical"}
        ]
        alerts.sort(key=lambda item: float(item.get("risk_score") or 0), reverse=True)
        count = len(roads)
        summary = {
            "level": worst.get("congestion_level", "smooth"),
            "prediction_label": worst.get("prediction_label", "Giao thông ổn định"),
            "avg_speed_kmh": round(sum(float(item.get("speed_kmh") or 0) for item in roads) / count, 1),
            "avg_density_pct": round(sum(float(item.get("density_pct") or 0) for item in roads) / count, 1),
            "vehicle_count": sum(int(item.get("vehicle_count") or 0) for item in roads),
            "risk_score": round(max(float(item.get("risk_score") or 0) for item in roads), 1),
            "updated_at": max(str(item.get("processed_at") or item.get("timestamp") or "") for item in roads),
        }
        return {"status": "ready", "summary": summary, "alerts": alerts, "roads": roads}

    def control(self, action: str, scenario: str | None = None, duration: float | None = None) -> None:
        with self.lock:
            if action == "start":
                self.state.active = True
            elif action == "stop":
                self.state.active = False
            elif action == "set_scenario" and scenario:
                incident_road = self.rng.choice(ROADS)["road_id"] if scenario == "incident" else None
                self.state.set_scenario(scenario, incident_road, duration)
                self.state.active = True

    def _make_clients(self) -> tuple[Producer, Consumer]:
        producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "traffic-iot-source"})
        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": "traffic-iot-web-results-v1",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe([PROCESSED_TOPIC])
        return producer, consumer

    @staticmethod
    def _publish(producer: Producer, topic: str, payload: dict[str, Any], key: str) -> None:
        producer.produce(topic, key=key.encode(), value=encode(payload))
        producer.poll(0)

    def _publish_status(self, producer: Producer) -> None:
        with self.lock:
            payload = {
                "source": "iot",
                "active": self.state.active,
                "scenario": self.state.scenario,
                "source_port": 8101,
                "timestamp": utc_now(),
            }
        self._publish(producer, STATUS_TOPIC, payload, "iot")

    def _consume_ai_results(self, consumer: Consumer) -> None:
        while True:
            message = consumer.poll(0)
            if message is None:
                break
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.warning("Kafka processed-result error: %s", message.error())
                continue
            try:
                result = json.loads(message.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            if result.get("source") != "iot" or not result.get("road_id"):
                continue
            with self.lock:
                self.processed_roads[str(result["road_id"])] = (time.monotonic(), result)

    def _run(self) -> None:
        producer: Producer | None = None
        consumer: Consumer | None = None
        while not self.stop_event.is_set():
            try:
                producer, consumer = self._make_clients()
                producer.list_topics(timeout=6)
                with self.lock:
                    self.kafka_connected = True
                    self.last_error = None
                self._produce_loop(producer, consumer)
            except Exception as exc:
                with self.lock:
                    self.kafka_connected = False
                    self.last_error = str(exc)
                LOGGER.warning("Kafka chưa sẵn sàng tại %s: %s", BOOTSTRAP, exc)
                self.stop_event.wait(3)
            finally:
                if consumer is not None:
                    consumer.close()
                    consumer = None
                if producer is not None:
                    producer.flush(2)
                    producer = None

    def _produce_loop(self, producer: Producer, consumer: Consumer) -> None:
        self._publish_status(producer)
        last_status = 0.0
        LOGGER.info("IoT source web ready on :8101 | scenario=%s", self.state.scenario)
        while not self.stop_event.is_set():
            self._consume_ai_results(consumer)
            with self.lock:
                self.state.tick()
                active = self.state.active
                state_copy = SourceState(**asdict(self.state))
                self.cycles += 1 if active else 0
                cycle = self.cycles

            if active:
                events = [
                    make_event(road, state_copy.scenario, cycle, state_copy.incident_road, self.rng)
                    for road in ROADS
                ]
                for event in events:
                    self._publish(producer, RAW_TOPIC, event, event["road_id"])
                with self.lock:
                    self.latest_events = events
                    self.total_events += len(events)
                    self.last_publish_at = events[0]["timestamp"]

            now = time.monotonic()
            if now - last_status >= 5:
                self._publish_status(producer)
                last_status = now
            self.stop_event.wait(INTERVAL_SECONDS if active else 0.2)

RUNTIME = IoTRuntime()


@asynccontextmanager
async def lifespan(_: FastAPI):
    RUNTIME.start()
    yield
    RUNTIME.close()


app = FastAPI(title="Nguồn IoT giả lập", version="2.0.0", lifespan=lifespan)


class ControlCommand(BaseModel):
    action: str = Field(pattern="^(start|stop|set_scenario)$")
    scenario: str | None = Field(default=None, pattern="^(normal|rush_hour|rain|incident)$")
    duration_seconds: int | None = Field(default=None, ge=1, le=600)


@app.get("/api/status")
async def status() -> dict[str, Any]:
    return RUNTIME.snapshot()


@app.post("/api/control")
async def control(command: ControlCommand) -> dict[str, Any]:
    if command.action == "set_scenario" and not command.scenario:
        raise HTTPException(status_code=400, detail="Vui lòng chọn kịch bản")
    duration = command.duration_seconds
    if command.action == "set_scenario" and command.scenario == "incident" and duration is None:
        duration = INCIDENT_DURATION_SECONDS
    RUNTIME.control(command.action, command.scenario, duration)
    return {"ok": True, "status": RUNTIME.snapshot()}


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="iot-source-ui")
