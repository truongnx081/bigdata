from pathlib import Path

import numpy as np

from services.video_producer.app import (
    build_parser,
    resolve_source,
    resolve_source_details,
    safe_upload_name,
)
from services.video_producer.video_metrics import (
    MotionTrafficEstimator,
    VehicleObservation,
    VideoMetrics,
    make_video_event,
    summarize_detections,
)
from services.video_producer.vehicle_tracking import YoloVehicleTracker


def test_summarize_detections_maps_to_traffic_metrics() -> None:
    metrics = summarize_detections(
        boxes=[(0, 0, 20, 20), (30, 20, 20, 40)],
        moving_pixels=1_000,
        frame_width=100,
        frame_height=100,
        speeds_kmh=[20.0, 40.0],
    )

    assert metrics.vehicle_count == 2
    assert metrics.density_pct == 12.0
    assert metrics.occupancy_pct == 10.0
    assert metrics.speed_kmh == 30.0
    assert metrics.warmed_up is True


def test_video_event_has_realtime_kafka_schema() -> None:
    vehicle = VehicleObservation(17, "Motorbike", 28.4, 0.91, (10, 20, 40, 80))
    metrics = VideoMetrics(
        4,
        28.5,
        12.0,
        35.2,
        0.72,
        True,
        vehicles=(vehicle,),
        inference_ms=24.5,
        measurement_method="yolo_bytetrack",
    )
    event = make_video_event(
        metrics,
        cycle=8,
        frame_index=240,
        road_id="camera-q1",
        road_name="Camera Quận 1",
        latitude=10.7732,
        longitude=106.7035,
        source_ref="traffic.mp4",
    )

    required = {
        "event_id", "source", "sensor_id", "road_id", "road_name", "latitude", "longitude",
        "timestamp", "speed_kmh", "density_pct", "vehicle_count", "occupancy_pct", "scenario",
        "cycle", "quality", "vehicles", "inference_ms", "video_frame", "source_uri", "measurement_method",
    }
    assert required <= set(event)
    assert event["source"] == "video"
    assert event["scenario"] == "video"
    assert event["video_frame"] == 240
    assert event["measurement_method"] == "yolo_bytetrack"
    assert event["vehicles"][0]["track_id"] == 17
    assert event["vehicles"][0]["speed_kmh"] == 28.4


def test_local_video_source_is_identified(tmp_path: Path) -> None:
    video = tmp_path / "sample.mp4"
    video.write_bytes(b"placeholder")
    resolved, label, is_local = resolve_source(str(video))
    assert resolved == str(video.resolve())
    assert label == "sample.mp4"
    assert is_local is True


def test_remote_video_source_is_rejected() -> None:
    try:
        resolve_source_details("https://camera.example/video.mp4")
    except FileNotFoundError as exc:
        assert "Không tìm thấy video" in str(exc)
    else:
        raise AssertionError("remote URL must not be accepted in upload-only mode")


def test_per_vehicle_speed_uses_persistent_track_history() -> None:
    tracker = YoloVehicleTracker.__new__(YoloVehicleTracker)
    tracker.pixels_per_meter = 10.0
    tracker.histories = {}

    assert tracker._speed_for(12, 0.0, 0.0, 1.0) is None
    assert tracker._speed_for(12, 10.0, 0.0, 1.5) == 7.2


def test_estimator_warms_up_before_reporting() -> None:
    estimator = MotionTrafficEstimator(warmup_frames=2)
    frame = np.zeros((120, 160, 3), dtype=np.uint8)

    _, first = estimator.process(frame, 0.04)
    _, second = estimator.process(frame, 0.08)
    processed, third = estimator.process(frame, 0.12)

    assert first.warmed_up is False
    assert second.warmed_up is False
    assert third.warmed_up is True
    assert processed.shape == frame.shape


def test_web_source_validation_and_upload_filename() -> None:
    uploaded = safe_upload_name("Giao thông Quận 1.MP4")
    assert uploaded.endswith("-Giao-th-ng-Qu-n-1.mp4")
    assert len(uploaded.split("-", 1)[0]) == 12


def test_cli_can_start_in_waiting_mode_without_source() -> None:
    args = build_parser().parse_args([])
    assert args.source is None
    assert args.web_port == 8102
