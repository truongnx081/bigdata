"""Trích xuất các chỉ số giao thông nhẹ từ những vùng chuyển động trong video.

Đây là bộ ước lượng không cần tải model AI. Số xe là số vùng chuyển động đủ lớn;
tốc độ là ước lượng từ quãng đường tâm vùng chuyển động giữa hai khung hình.
"""

from __future__ import annotations

import statistics
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import cv2
import numpy as np


@dataclass(frozen=True)
class VehicleObservation:
    track_id: int
    vehicle_type: str
    speed_kmh: float | None
    confidence: float
    bbox: tuple[int, int, int, int]

    def to_payload(self) -> dict[str, Any]:
        return {
            "track_id": self.track_id,
            "vehicle_type": self.vehicle_type,
            "speed_kmh": self.speed_kmh,
            "confidence": round(self.confidence, 3),
            "bbox": list(self.bbox),
        }


@dataclass(frozen=True)
class VideoMetrics:
    vehicle_count: int
    density_pct: float
    occupancy_pct: float
    speed_kmh: float
    quality: float
    warmed_up: bool
    vehicles: tuple[VehicleObservation, ...] = ()
    inference_ms: float = 0.0
    measurement_method: str = "motion_tracking"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def summarize_detections(
    boxes: list[tuple[int, int, int, int]],
    moving_pixels: int,
    frame_width: int,
    frame_height: int,
    speeds_kmh: list[float],
    warmed_up: bool = True,
) -> VideoMetrics:
    """Chuyển kết quả thị giác máy tính thành schema mà Spark đang sử dụng."""
    frame_area = max(1, frame_width * frame_height)
    box_area = sum(width * height for _, _, width, height in boxes)
    density = min(100.0, box_area * 100.0 / frame_area)
    occupancy = min(100.0, max(0, moving_pixels) * 100.0 / frame_area)
    speed = statistics.median(speeds_kmh) if speeds_kmh else 0.0
    speed = min(120.0, max(0.0, speed))

    if not warmed_up:
        quality = 0.2
    else:
        quality = min(0.9, 0.42 + min(len(boxes), 6) * 0.05 + (0.13 if speeds_kmh else 0.0))

    return VideoMetrics(
        vehicle_count=len(boxes),
        density_pct=round(density, 2),
        occupancy_pct=round(occupancy, 2),
        speed_kmh=round(speed, 2),
        quality=round(quality, 3),
        warmed_up=warmed_up,
    )


class MotionTrafficEstimator:
    """Ước lượng giao thông cho camera cố định bằng background subtraction."""

    def __init__(
        self,
        *,
        warmup_frames: int = 30,
        min_area_ratio: float = 0.0008,
        pixels_per_meter: float = 8.0,
        max_width: int = 960,
    ) -> None:
        self.warmup_frames = max(1, warmup_frames)
        self.min_area_ratio = max(0.00005, min_area_ratio)
        self.pixels_per_meter = max(0.1, pixels_per_meter)
        self.max_width = max(320, max_width)
        self.frame_count = 0
        self.previous_centroids: list[tuple[float, float]] = []
        self.previous_timestamp: float | None = None
        self.last_boxes: list[tuple[int, int, int, int]] = []
        self.subtractor = cv2.createBackgroundSubtractorMOG2(
            history=max(120, self.warmup_frames * 4),
            varThreshold=42,
            detectShadows=True,
        )

    def reset(self) -> None:
        self.__init__(
            warmup_frames=self.warmup_frames,
            min_area_ratio=self.min_area_ratio,
            pixels_per_meter=self.pixels_per_meter,
            max_width=self.max_width,
        )

    def process(self, frame: np.ndarray, video_timestamp: float) -> tuple[np.ndarray, VideoMetrics]:
        if frame is None or frame.size == 0:
            raise ValueError("Khung hình video rỗng")

        height, width = frame.shape[:2]
        if width > self.max_width:
            scale = self.max_width / width
            frame = cv2.resize(frame, (self.max_width, max(1, round(height * scale))))
        height, width = frame.shape[:2]

        blurred = cv2.GaussianBlur(frame, (5, 5), 0)
        foreground = self.subtractor.apply(blurred)
        _, foreground = cv2.threshold(foreground, 244, 255, cv2.THRESH_BINARY)
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        foreground = cv2.morphologyEx(foreground, cv2.MORPH_OPEN, kernel)
        foreground = cv2.morphologyEx(foreground, cv2.MORPH_CLOSE, kernel, iterations=2)
        foreground = cv2.dilate(foreground, kernel, iterations=1)

        frame_area = width * height
        min_area = frame_area * self.min_area_ratio
        max_area = frame_area * 0.32
        contours, _ = cv2.findContours(foreground, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        boxes: list[tuple[int, int, int, int]] = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area < min_area or area > max_area:
                continue
            x, y, box_width, box_height = cv2.boundingRect(contour)
            if box_width < 10 or box_height < 10:
                continue
            boxes.append((x, y, box_width, box_height))
        boxes.sort(key=lambda item: (item[0], item[1]))

        centroids = [(x + box_width / 2.0, y + box_height / 2.0) for x, y, box_width, box_height in boxes]
        speeds = self._match_speeds(centroids, video_timestamp, width, height)
        self.frame_count += 1
        warmed_up = self.frame_count > self.warmup_frames
        self.last_boxes = boxes if warmed_up else []
        moving_pixels = cv2.countNonZero(foreground) if warmed_up else 0
        metrics = summarize_detections(
            self.last_boxes,
            moving_pixels,
            width,
            height,
            speeds if warmed_up else [],
            warmed_up,
        )
        return frame, metrics

    def _match_speeds(
        self,
        centroids: list[tuple[float, float]],
        video_timestamp: float,
        width: int,
        height: int,
    ) -> list[float]:
        speeds: list[float] = []
        elapsed = None if self.previous_timestamp is None else video_timestamp - self.previous_timestamp
        if elapsed and elapsed > 0 and self.previous_centroids:
            available = set(range(len(self.previous_centroids)))
            max_distance = max(width, height) * 0.14
            for x, y in centroids:
                candidates = [
                    (index, ((x - self.previous_centroids[index][0]) ** 2 + (y - self.previous_centroids[index][1]) ** 2) ** 0.5)
                    for index in available
                ]
                if not candidates:
                    break
                index, distance = min(candidates, key=lambda item: item[1])
                if distance <= max_distance:
                    available.remove(index)
                    meters_per_second = distance / elapsed / self.pixels_per_meter
                    speeds.append(meters_per_second * 3.6)

        self.previous_centroids = centroids
        self.previous_timestamp = video_timestamp
        return speeds

    def annotate(self, frame: np.ndarray, metrics: VideoMetrics) -> np.ndarray:
        output = frame.copy()
        for x, y, width, height in self.last_boxes:
            cv2.rectangle(output, (x, y), (x + width, y + height), (49, 208, 170), 2)
        label = (
            f"Vehicles: {metrics.vehicle_count} | Density: {metrics.density_pct:.1f}% | "
            f"Speed est.: {metrics.speed_kmh:.1f} km/h"
            if metrics.warmed_up
            else f"Warming up detector: {self.frame_count}/{self.warmup_frames}"
        )
        cv2.rectangle(output, (0, 0), (output.shape[1], 38), (20, 26, 36), -1)
        cv2.putText(output, label, (12, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.58, (255, 255, 255), 1, cv2.LINE_AA)
        return output


def make_video_event(
    metrics: VideoMetrics,
    *,
    cycle: int,
    frame_index: int,
    road_id: str,
    road_name: str,
    latitude: float,
    longitude: float,
    source_ref: str,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "source": "video",
        "sensor_id": f"video-{road_id}",
        "road_id": road_id,
        "road_name": road_name,
        "latitude": latitude,
        "longitude": longitude,
        "timestamp": utc_now(),
        "speed_kmh": metrics.speed_kmh,
        "density_pct": metrics.density_pct,
        "vehicle_count": metrics.vehicle_count,
        "occupancy_pct": metrics.occupancy_pct,
        "scenario": "video",
        "cycle": cycle,
        "quality": metrics.quality,
        "vehicles": [vehicle.to_payload() for vehicle in metrics.vehicles],
        "inference_ms": round(metrics.inference_ms, 1),
        "video_frame": frame_index,
        "source_uri": source_ref,
        "measurement_method": metrics.measurement_method,
    }
