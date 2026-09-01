"""YOLO vehicle detection, ByteTrack identities, and per-vehicle speed estimates."""

from __future__ import annotations

import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import cv2
import numpy as np

from services.video_producer.video_metrics import VehicleObservation, VideoMetrics


LOGGER = logging.getLogger("video-source")
VEHICLE_CLASS_IDS = (1, 2, 3, 5, 7)  # bicycle, car, motorcycle, bus, truck (COCO)
VEHICLE_LABELS = {
    1: "Bicycle",
    2: "Car",
    3: "Motorbike",
    5: "Bus",
    7: "Truck",
}
VEHICLE_COLORS = {
    1: (245, 158, 11),
    2: (34, 211, 238),
    3: (74, 222, 128),
    5: (167, 139, 250),
    7: (251, 113, 133),
}


@dataclass
class _TrackHistory:
    points: deque[tuple[float, float, float]] = field(default_factory=lambda: deque(maxlen=45))
    smoothed_speed: float | None = None
    last_seen: float = 0.0


class YoloVehicleTracker:
    """Run a small YOLO model with ByteTrack and retain speed state per track ID."""

    def __init__(
        self,
        *,
        model_name: str = "yolo26n.pt",
        pixels_per_meter: float = 8.0,
        confidence: float = 0.28,
        image_size: int = 640,
        device: str = "auto",
        tracker_config: str = "bytetrack.yaml",
    ) -> None:
        try:
            import torch
            from ultralytics import YOLO
        except ImportError as exc:  # pragma: no cover - start script installs it
            raise RuntimeError("Thiếu Ultralytics YOLO. Hãy chạy lại scripts/start-video.ps1.") from exc

        self.device: str | int = 0 if device == "auto" and torch.cuda.is_available() else device
        if self.device == "auto":
            self.device = "cpu"
        self.model_name = model_name
        self.model = YOLO(model_name)
        self.pixels_per_meter = max(0.1, pixels_per_meter)
        self.confidence = min(0.95, max(0.05, confidence))
        self.image_size = max(320, image_size)
        self.tracker_config = tracker_config
        self.histories: dict[int, _TrackHistory] = {}
        self.last_inference_ms = 0.0
        LOGGER.info(
            "YOLO sẵn sàng | model=%s | device=%s | tracker=ByteTrack",
            model_name,
            self.device,
        )

    @property
    def device_label(self) -> str:
        return "GPU" if self.device != "cpu" else "CPU"

    def reset(self) -> None:
        self.histories.clear()
        predictor = getattr(self.model, "predictor", None)
        for tracker in getattr(predictor, "trackers", []) or []:
            reset = getattr(tracker, "reset", None)
            if callable(reset):
                reset()

    def _speed_for(
        self,
        track_id: int,
        x: float,
        y: float,
        timestamp: float,
    ) -> float | None:
        history = self.histories.setdefault(track_id, _TrackHistory())
        history.points.append((timestamp, x, y))
        history.last_seen = timestamp

        reference: tuple[float, float, float] | None = None
        for point in history.points:
            elapsed = timestamp - point[0]
            if 0.3 <= elapsed <= 1.5:
                reference = point
                break
        if reference is None:
            return history.smoothed_speed

        elapsed = max(0.001, timestamp - reference[0])
        distance_px = ((x - reference[1]) ** 2 + (y - reference[2]) ** 2) ** 0.5
        measured_speed = min(160.0, distance_px / self.pixels_per_meter / elapsed * 3.6)
        if history.smoothed_speed is None:
            history.smoothed_speed = measured_speed
        else:
            history.smoothed_speed = history.smoothed_speed * 0.68 + measured_speed * 0.32
        return round(history.smoothed_speed, 1)

    def _forget_stale_tracks(self, timestamp: float) -> None:
        stale_ids = [track_id for track_id, state in self.histories.items() if timestamp - state.last_seen > 2.5]
        for track_id in stale_ids:
            self.histories.pop(track_id, None)

    def process(self, frame: np.ndarray, timestamp: float) -> tuple[np.ndarray, VideoMetrics]:
        if frame is None or frame.size == 0:
            raise ValueError("Khung hình video rỗng")

        inference_started = time.perf_counter()
        result = self.model.track(
            frame,
            persist=True,
            tracker=self.tracker_config,
            classes=list(VEHICLE_CLASS_IDS),
            conf=self.confidence,
            iou=0.5,
            imgsz=self.image_size,
            device=self.device,
            verbose=False,
        )[0]
        self.last_inference_ms = (time.perf_counter() - inference_started) * 1000.0

        height, width = frame.shape[:2]
        boxes = result.boxes
        observations: list[VehicleObservation] = []
        if boxes is not None and len(boxes) > 0:
            coordinates = boxes.xyxy.detach().cpu().tolist()
            classes = boxes.cls.detach().cpu().tolist()
            confidences = boxes.conf.detach().cpu().tolist()
            track_ids: list[int | None]
            if boxes.id is None:
                track_ids = [None] * len(coordinates)
            else:
                track_ids = [int(value) for value in boxes.id.detach().cpu().tolist()]

            for index, (coords, class_value, confidence) in enumerate(zip(coordinates, classes, confidences)):
                x1, y1, x2, y2 = [int(round(value)) for value in coords]
                class_id = int(class_value)
                track_id = track_ids[index]
                speed: float | None = None
                if track_id is not None:
                    speed = self._speed_for(track_id, (x1 + x2) / 2.0, float(y2), timestamp)
                observations.append(
                    VehicleObservation(
                        track_id=track_id if track_id is not None else -(index + 1),
                        vehicle_type=VEHICLE_LABELS.get(class_id, "Vehicle"),
                        speed_kmh=speed,
                        confidence=float(confidence),
                        bbox=(x1, y1, x2, y2),
                    )
                )

        self._forget_stale_tracks(timestamp)
        frame_area = max(1, width * height)
        box_area = sum(max(0, x2 - x1) * max(0, y2 - y1) for x1, y1, x2, y2 in (item.bbox for item in observations))
        density = min(100.0, box_area * 100.0 / frame_area)
        known_speeds = [item.speed_kmh for item in observations if item.speed_kmh is not None]
        average_speed = statistics.median(known_speeds) if known_speeds else 0.0
        quality = statistics.fmean(item.confidence for item in observations) if observations else 0.25
        metrics = VideoMetrics(
            vehicle_count=len(observations),
            density_pct=round(density, 2),
            occupancy_pct=round(min(100.0, density * 0.88), 2),
            speed_kmh=round(average_speed, 2),
            quality=round(min(0.98, max(0.2, quality)), 3),
            warmed_up=True,
            vehicles=tuple(observations),
            inference_ms=round(self.last_inference_ms, 1),
            measurement_method="yolo_bytetrack",
        )
        return self.annotate(frame, metrics), metrics

    def annotate(self, frame: np.ndarray, metrics: VideoMetrics) -> np.ndarray:
        output = frame.copy()
        for vehicle in metrics.vehicles:
            x1, y1, x2, y2 = vehicle.bbox
            class_id = next((key for key, value in VEHICLE_LABELS.items() if value == vehicle.vehicle_type), 2)
            color = VEHICLE_COLORS.get(class_id, (34, 211, 238))
            cv2.rectangle(output, (x1, y1), (x2, y2), color, 2)
            identity = f"#{vehicle.track_id}" if vehicle.track_id >= 0 else "scan"
            speed = "-- km/h" if vehicle.speed_kmh is None else f"{vehicle.speed_kmh:.1f} km/h"
            label = f"{vehicle.vehicle_type} {identity} | {speed}"
            (label_width, label_height), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.48, 1)
            label_top = max(0, y1 - label_height - 10)
            cv2.rectangle(output, (x1, label_top), (min(output.shape[1] - 1, x1 + label_width + 10), y1), color, -1)
            cv2.putText(
                output,
                label,
                (x1 + 5, max(label_height + 1, y1 - 5)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.48,
                (5, 12, 22),
                1,
                cv2.LINE_AA,
            )

        header = (
            f"YOLO + ByteTrack | Vehicles: {metrics.vehicle_count} | "
            f"Avg: {metrics.speed_kmh:.1f} km/h | AI: {metrics.inference_ms:.0f} ms"
        )
        cv2.rectangle(output, (0, 0), (output.shape[1], 38), (7, 17, 31), -1)
        cv2.putText(output, header, (12, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (226, 245, 255), 1, cv2.LINE_AA)
        return output
