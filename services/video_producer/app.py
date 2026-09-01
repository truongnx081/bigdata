"""Nguồn video điều khiển từ web, gửi chỉ số giao thông vào Kafka/Spark."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cv2
from confluent_kafka import Consumer, KafkaError, Producer
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn

from services.video_producer.frame_pipeline import LatestFrameCapture
from services.video_producer.video_metrics import MotionTrafficEstimator, VideoMetrics, make_video_event, utc_now
from services.video_producer.vehicle_tracking import YoloVehicleTracker


logging.basicConfig(level=logging.INFO, format="%(asctime)s | Video | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("video-source")

RAW_TOPIC = "traffic.video.raw"
PROCESSED_TOPIC = "traffic.video.processed"
STATUS_TOPIC = "traffic.source.status"
UPLOAD_DIR = Path(__file__).resolve().parents[2] / ".codex_tmp" / "video-uploads"
ALLOWED_VIDEO_SUFFIXES = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v", ".mpeg", ".mpg"}
MAX_UPLOAD_BYTES = int(os.getenv("VIDEO_MAX_UPLOAD_MB", "2048")) * 1024 * 1024


class DashboardState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.active = False
        self.loading = False
        self.port = 8102
        self.kafka_connected = False
        self.source_ref: str | None = None
        self.road_id: str | None = None
        self.road_name: str | None = None
        self.latitude = 0.0
        self.longitude = 0.0
        self.fps = 0.0
        self.processing_fps = 0.0
        self.dropped_frames = 0
        self.detector = "YOLO + ByteTrack"
        self.detector_device = "-"
        self.frame_index = 0
        self.total_events = 0
        self.started_at: str | None = None
        self.last_error: str | None = None
        self.live_metrics: dict[str, Any] | None = None
        self.latest_event: dict[str, Any] | None = None
        self.analysis: dict[str, Any] | None = None
        self.frame_jpeg: bytes | None = None

    def set_port(self, port: int) -> None:
        with self.lock:
            self.port = port

    def prepare(self, source_ref: str, args: argparse.Namespace) -> None:
        with self.lock:
            self.active = False
            self.loading = True
            self.kafka_connected = False
            self.source_ref = source_ref
            self.road_id = args.road_id
            self.road_name = args.road_name
            self.latitude = args.latitude
            self.longitude = args.longitude
            self.fps = 0.0
            self.processing_fps = 0.0
            self.dropped_frames = 0
            self.detector_device = "-"
            self.frame_index = 0
            self.total_events = 0
            self.started_at = utc_now()
            self.last_error = None
            self.live_metrics = None
            self.latest_event = None
            self.analysis = None
            self.frame_jpeg = None

    def configure(
        self,
        args: argparse.Namespace,
        source_ref: str,
        fps: float,
        *,
        detector: str,
        detector_device: str,
    ) -> None:
        with self.lock:
            self.active = True
            self.loading = False
            self.source_ref = source_ref
            self.road_id = args.road_id
            self.road_name = args.road_name
            self.latitude = args.latitude
            self.longitude = args.longitude
            self.fps = fps
            self.detector = detector
            self.detector_device = detector_device
            self.started_at = utc_now()
            self.last_error = None

    def set_connected(self, connected: bool) -> None:
        with self.lock:
            self.kafka_connected = connected

    def update_frame(
        self,
        frame_index: int,
        jpeg: bytes,
        *,
        processing_fps: float,
        dropped_frames: int,
        metrics: VideoMetrics | None = None,
    ) -> None:
        with self.lock:
            self.frame_index = frame_index
            self.frame_jpeg = jpeg
            self.processing_fps = processing_fps
            self.dropped_frames = dropped_frames
            if metrics is not None:
                self.live_metrics = {
                    "timestamp": utc_now(),
                    "speed_kmh": metrics.speed_kmh,
                    "density_pct": metrics.density_pct,
                    "vehicle_count": metrics.vehicle_count,
                    "occupancy_pct": metrics.occupancy_pct,
                    "quality": metrics.quality,
                    "vehicles": [vehicle.to_payload() for vehicle in metrics.vehicles],
                    "inference_ms": metrics.inference_ms,
                    "video_frame": frame_index,
                    "measurement_method": metrics.measurement_method,
                    "warmed_up": metrics.warmed_up,
                }

    def frame_snapshot(self) -> tuple[int, bytes | None, bool]:
        with self.lock:
            return self.frame_index, self.frame_jpeg, self.active

    def update_event(self, event: dict[str, Any]) -> None:
        with self.lock:
            self.latest_event = dict(event)
            self.total_events += 1

    def update_analysis(self, result: dict[str, Any]) -> None:
        with self.lock:
            self.analysis = dict(result)

    def fail(self, error: str) -> None:
        with self.lock:
            self.last_error = error
            self.active = False
            self.loading = False
            self.kafka_connected = False

    def stop(self) -> None:
        with self.lock:
            self.active = False
            self.loading = False
            self.kafka_connected = False

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "source": "video",
                "port": self.port,
                "active": self.active,
                "loading": self.loading,
                "kafka_connected": self.kafka_connected,
                "source_ref": self.source_ref,
                "road_id": self.road_id,
                "road_name": self.road_name,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "fps": round(self.fps, 2),
                "processing_fps": round(self.processing_fps, 2),
                "dropped_frames": self.dropped_frames,
                "detector": self.detector,
                "detector_device": self.detector_device,
                "frame_index": self.frame_index,
                "total_events": self.total_events,
                "started_at": self.started_at,
                "last_error": self.last_error,
                "live_metrics": dict(self.live_metrics) if self.live_metrics else None,
                "latest_event": dict(self.latest_event) if self.latest_event else None,
                "analysis": dict(self.analysis) if self.analysis else None,
            }


def encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def safe_upload_name(filename: str) -> str:
    original = Path(filename).name
    suffix = Path(original).suffix.lower()
    if suffix not in ALLOWED_VIDEO_SUFFIXES:
        raise ValueError(f"Định dạng chưa hỗ trợ. Hãy dùng: {', '.join(sorted(ALLOWED_VIDEO_SUFFIXES))}")
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(original).stem).strip("-._") or "video"
    return f"{uuid.uuid4().hex[:12]}-{stem[:80]}{suffix}"


@dataclass(frozen=True)
class ResolvedSource:
    capture_input: str
    public_ref: str
    is_local_file: bool


def resolve_source_details(source: str) -> ResolvedSource:
    path = Path(source).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Không tìm thấy video: {path}")
    return ResolvedSource(str(path), path.name, True)


def resolve_source(source: str) -> tuple[str, str, bool]:
    """Compatibility tuple: OpenCV input, public label, and local-file flag."""
    resolved = resolve_source_details(source)
    return resolved.capture_input, resolved.public_ref, resolved.is_local_file


class KafkaBridge:
    def __init__(self, bootstrap: str, road_id: str, source_ref: str) -> None:
        self.bootstrap = bootstrap
        self.road_id = road_id
        self.source_ref = source_ref
        self.connected = False
        self.producer = Producer({"bootstrap.servers": bootstrap, "client.id": "traffic-video-source"})
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"traffic-video-terminal-results-v3-{road_id}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )
        self.consumer.subscribe([PROCESSED_TOPIC])

    def connect(self, stop_event: threading.Event, timeout_seconds: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while not stop_event.is_set():
            try:
                self.producer.list_topics(timeout=5)
                self.connected = True
                LOGGER.info("Đã kết nối Kafka tại %s", self.bootstrap)
                return True
            except Exception as exc:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Không kết nối được Kafka tại {self.bootstrap}: {exc}") from exc
                LOGGER.info("Kafka đang khởi động, thử lại sau 2 giây...")
                stop_event.wait(2)
        return False

    def publish(self, topic: str, payload: dict[str, Any], key: str) -> None:
        self.producer.produce(topic, key=key.encode(), value=encode(payload))
        self.producer.poll(0)

    def publish_status(self, active: bool) -> None:
        self.publish(
            STATUS_TOPIC,
            {
                "source": "video",
                "active": active,
                "scenario": "video",
                "source_ref": self.source_ref,
                "timestamp": utc_now(),
            },
            "video",
        )

    def consume_results(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        while True:
            message = self.consumer.poll(0)
            if message is None:
                return results
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.warning("Lỗi khi đọc kết quả Spark: %s", message.error())
                continue
            try:
                result = json.loads(message.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            if (
                result.get("source") != "video"
                or result.get("road_id") != self.road_id
                or result.get("source_uri") != self.source_ref
            ):
                continue
            results.append(result)
            LOGGER.info(
                "Spark/AI | %s | risk=%s | anomaly=%s",
                result.get("prediction_label", "Đã xử lý"),
                result.get("risk_score", "-"),
                "có" if result.get("anomaly") else "không",
            )

    def close(self) -> None:
        try:
            if self.connected:
                self.publish_status(False)
                self.producer.flush(5)
        finally:
            self.consumer.close()


def log_metrics(cycle: int, metrics: VideoMetrics) -> None:
    LOGGER.info(
        "Chu kỳ %d | xe=%d | mật độ=%.1f%% | chiếm dụng=%.1f%% | tốc độ ước lượng=%.1f km/h",
        cycle,
        metrics.vehicle_count,
        metrics.density_pct,
        metrics.occupancy_pct,
        metrics.speed_kmh,
    )


def resize_for_processing(frame: Any, max_width: int) -> tuple[Any, float]:
    if max_width <= 0:
        return frame, 1.0
    height, width = frame.shape[:2]
    if width <= max_width:
        return frame, 1.0
    scale = max_width / width
    resized = cv2.resize(frame, (max_width, max(1, round(height * scale))), interpolation=cv2.INTER_AREA)
    return resized, scale


def run_source(args: argparse.Namespace, stop_event: threading.Event) -> int:
    resolved = resolve_source_details(args.source)
    source_ref = getattr(args, "source_label", None) or resolved.public_ref
    yolo_tracker: YoloVehicleTracker | None = None
    motion_estimator: MotionTrafficEstimator | None = None
    if args.detector == "yolo":
        yolo_tracker = YoloVehicleTracker(
            model_name=args.yolo_model,
            pixels_per_meter=args.pixels_per_meter,
            confidence=args.yolo_confidence,
            image_size=args.yolo_image_size,
            device=args.yolo_device,
        )
        detector_name = "YOLO + ByteTrack"
        detector_device = yolo_tracker.device_label
    else:
        motion_estimator = MotionTrafficEstimator(
            warmup_frames=args.warmup_frames,
            min_area_ratio=args.min_area_ratio,
            pixels_per_meter=args.pixels_per_meter,
        )
        detector_name = "Motion tracking"
        detector_device = "CPU"

    capture = LatestFrameCapture(
        resolved.capture_input,
        loop=args.loop,
        realtime=not args.no_realtime,
        stop_event=stop_event,
    )
    capture_mode = "realtime playback"
    LOGGER.info(
        "Đã mở video | FPS=%.2f | frames=%s | mode=%s",
        capture.fps,
        capture.frame_count_hint or "stream",
        capture_mode,
    )
    DASHBOARD.configure(
        args,
        source_ref,
        capture.fps,
        detector=detector_name,
        detector_device=detector_device,
    )

    bridge: KafkaBridge | None = None
    cycle = 0
    last_publish_at = float("-inf")
    last_status_at = 0.0
    last_sequence = 0
    last_generation = 0
    processed_count = 0
    dropped_frames = 0
    processed_times: deque[float] = deque(maxlen=120)

    try:
        if not args.dry_run:
            bridge = KafkaBridge(args.bootstrap, args.road_id, source_ref)
            if not bridge.connect(stop_event):
                return 0
            bridge.publish_status(True)
            DASHBOARD.set_connected(True)

        capture.start()
        while not stop_event.is_set():
            packet = capture.read_after(last_sequence, timeout=0.5)
            if packet is None:
                if capture.ended:
                    if capture.error:
                        raise RuntimeError(capture.error)
                    LOGGER.info("Nguồn video đã kết thúc hoặc ngắt luồng")
                    break
                continue

            dropped_frames += max(0, packet.sequence - last_sequence - 1)
            last_sequence = packet.sequence
            if packet.generation != last_generation:
                last_generation = packet.generation
                if yolo_tracker:
                    yolo_tracker.reset()
                if motion_estimator:
                    motion_estimator.reset()

            processing_frame, frame_scale = resize_for_processing(packet.image, args.frame_max_width)
            if yolo_tracker:
                yolo_tracker.pixels_per_meter = max(0.1, args.pixels_per_meter * frame_scale)
                annotated_frame, metrics = yolo_tracker.process(processing_frame, packet.timestamp)
            else:
                assert motion_estimator is not None
                motion_estimator.pixels_per_meter = max(0.1, args.pixels_per_meter * frame_scale)
                processed_frame, metrics = motion_estimator.process(processing_frame, packet.timestamp)
                annotated_frame = motion_estimator.annotate(processed_frame, metrics)

            processed_count += 1
            processed_times.append(time.monotonic())
            while len(processed_times) > 2 and processed_times[-1] - processed_times[0] > 2.0:
                processed_times.popleft()
            processing_fps = 0.0
            if len(processed_times) > 1:
                processing_fps = (len(processed_times) - 1) / max(0.001, processed_times[-1] - processed_times[0])

            encoded, jpeg = cv2.imencode(
                ".jpg",
                annotated_frame,
                [cv2.IMWRITE_JPEG_QUALITY, min(95, max(45, args.jpeg_quality))],
            )
            if encoded:
                DASHBOARD.update_frame(
                    packet.source_frame_index,
                    jpeg.tobytes(),
                    processing_fps=processing_fps,
                    dropped_frames=dropped_frames,
                    metrics=metrics,
                )

            if metrics.warmed_up and packet.timestamp - last_publish_at >= max(0.1, args.publish_interval):
                cycle += 1
                event = make_video_event(
                    metrics,
                    cycle=cycle,
                    frame_index=packet.source_frame_index,
                    road_id=args.road_id,
                    road_name=args.road_name,
                    latitude=args.latitude,
                    longitude=args.longitude,
                    source_ref=source_ref,
                )
                if bridge:
                    bridge.publish(RAW_TOPIC, event, args.road_id)
                DASHBOARD.update_event(event)
                log_metrics(cycle, metrics)
                last_publish_at = packet.timestamp

            if bridge:
                for result in bridge.consume_results():
                    DASHBOARD.update_analysis(result)
                now = time.monotonic()
                if now - last_status_at >= 5:
                    bridge.publish_status(True)
                    last_status_at = now

            if args.preview:
                cv2.imshow("Traffic video source - Q de thoat", annotated_frame)
                if cv2.waitKey(1) & 0xFF in {ord("q"), 27}:
                    LOGGER.info("Đã nhận lệnh dừng từ cửa sổ preview")
                    break

            if args.max_frames and processed_count >= args.max_frames:
                break
    finally:
        capture.close()
        cv2.destroyAllWindows()
        if bridge:
            bridge.close()
        DASHBOARD.stop()
    return 0


class SourceController:
    def __init__(self, base_args: argparse.Namespace) -> None:
        self.base_args = base_args
        self.lock = threading.RLock()
        self.worker: threading.Thread | None = None
        self.stop_event: threading.Event | None = None

    def start(
        self,
        source: str,
        *,
        loop: bool,
        overrides: dict[str, Any] | None = None,
        display_name: str | None = None,
    ) -> None:
        self.stop()
        values = vars(self.base_args).copy()
        values.update({"source": source, "loop": loop})
        if overrides:
            values.update({key: value for key, value in overrides.items() if value is not None})
        values["source_label"] = display_name
        args = argparse.Namespace(**values)
        public_ref = display_name or Path(source).name
        stop_event = threading.Event()
        worker = threading.Thread(
            target=self._run_worker,
            args=(args, stop_event),
            name="video-source-worker",
            daemon=True,
        )
        with self.lock:
            self.stop_event = stop_event
            self.worker = worker
            DASHBOARD.prepare(public_ref, args)
            worker.start()

    def _run_worker(self, args: argparse.Namespace, stop_event: threading.Event) -> None:
        try:
            run_source(args, stop_event)
        except Exception as exc:
            DASHBOARD.fail(str(exc))
            LOGGER.error("Không thể chạy nguồn video: %s", exc)
        finally:
            with self.lock:
                if self.stop_event is stop_event:
                    self.worker = None
                    self.stop_event = None

    def stop(self, timeout: float = 10.0) -> None:
        with self.lock:
            worker = self.worker
            stop_event = self.stop_event
        if stop_event:
            stop_event.set()
        if worker and worker is not threading.current_thread():
            worker.join(timeout=timeout)
            if worker.is_alive():
                raise RuntimeError("Nguồn video cũ chưa dừng kịp; hãy thử lại sau vài giây")
        DASHBOARD.stop()

    def close(self) -> None:
        self.stop(timeout=12)


DASHBOARD = DashboardState()
SOURCE_CONTROLLER: SourceController | None = None
DASHBOARD_APP = FastAPI(title="API nguồn video giao thông", version="2.0.0")
DASHBOARD_APP.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

def controller() -> SourceController:
    if SOURCE_CONTROLLER is None:
        raise HTTPException(status_code=503, detail="Video service đang khởi động")
    return SOURCE_CONTROLLER


@DASHBOARD_APP.get("/api/status")
async def dashboard_status() -> dict[str, Any]:
    return DASHBOARD.snapshot()


@DASHBOARD_APP.get("/api/frame.jpg")
async def dashboard_frame() -> Response:
    with DASHBOARD.lock:
        jpeg = DASHBOARD.frame_jpeg
    if jpeg is None:
        return Response(status_code=204)
    return Response(
        content=jpeg,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@DASHBOARD_APP.get("/api/stream.mjpg")
def dashboard_stream() -> StreamingResponse:
    def frames():
        last_frame_index = -1
        idle_since = time.monotonic()
        while True:
            frame_index, jpeg, active = DASHBOARD.frame_snapshot()
            if jpeg is not None and frame_index != last_frame_index:
                last_frame_index = frame_index
                idle_since = time.monotonic()
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    + f"Content-Length: {len(jpeg)}\r\n\r\n".encode("ascii")
                    + jpeg
                    + b"\r\n"
                )
            elif not active and time.monotonic() - idle_since > 1.0:
                break
            else:
                time.sleep(0.01)

    return StreamingResponse(
        frames(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )

@DASHBOARD_APP.post("/api/source/upload")
async def upload_video_source(
    request: Request,
    filename: str = Query(min_length=1, max_length=255),
    loop: bool = Query(default=True),
) -> dict[str, Any]:
    try:
        stored_name = safe_upload_name(filename)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Video vượt quá giới hạn dung lượng")

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOAD_DIR / stored_name
    written = 0
    try:
        with target.open("wb") as handle:
            async for chunk in request.stream():
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Video vượt quá giới hạn dung lượng")
                handle.write(chunk)
        if written == 0:
            raise HTTPException(status_code=400, detail="Tệp video rỗng")
    except Exception:
        target.unlink(missing_ok=True)
        raise

    try:
        controller().start(str(target), loop=loop, display_name=Path(filename).name)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "filename": Path(filename).name, "status": DASHBOARD.snapshot()}


@DASHBOARD_APP.post("/api/source/stop")
async def stop_video_source() -> dict[str, Any]:
    try:
        controller().stop()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "status": DASHBOARD.snapshot()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mở API/dashboard để tải video từ máy tính và gửi dữ liệu vào Kafka/Spark.",
    )
    parser.add_argument("--source", help="File video khởi động tùy chọn; có thể chọn sau trên website")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--road-id", default=os.getenv("VIDEO_ROAD_ID", "video-camera-01"))
    parser.add_argument("--road-name", default=os.getenv("VIDEO_ROAD_NAME", "Nguồn video trực tiếp"))
    parser.add_argument("--latitude", type=float, default=float(os.getenv("VIDEO_LATITUDE", "10.7732")))
    parser.add_argument("--longitude", type=float, default=float(os.getenv("VIDEO_LONGITUDE", "106.7035")))
    parser.add_argument("--publish-interval", type=float, default=float(os.getenv("VIDEO_PUBLISH_INTERVAL_SECONDS", "1")))
    parser.add_argument("--warmup-frames", type=int, default=int(os.getenv("VIDEO_WARMUP_FRAMES", "30")))
    parser.add_argument("--min-area-ratio", type=float, default=float(os.getenv("VIDEO_MIN_AREA_RATIO", "0.0008")))
    parser.add_argument("--pixels-per-meter", type=float, default=float(os.getenv("VIDEO_PIXELS_PER_METER", "8")))
    parser.add_argument("--detector", choices=["yolo", "motion"], default=os.getenv("VIDEO_DETECTOR", "yolo"))
    parser.add_argument("--yolo-model", default=os.getenv("VIDEO_YOLO_MODEL", "yolo26n.pt"))
    parser.add_argument("--yolo-device", default=os.getenv("VIDEO_YOLO_DEVICE", "auto"))
    parser.add_argument("--yolo-confidence", type=float, default=float(os.getenv("VIDEO_YOLO_CONFIDENCE", "0.12")))
    parser.add_argument("--yolo-image-size", type=int, default=int(os.getenv("VIDEO_YOLO_IMAGE_SIZE", "640")))
    parser.add_argument("--jpeg-quality", type=int, default=int(os.getenv("VIDEO_JPEG_QUALITY", "60")))
    parser.add_argument("--frame-max-width", type=int, default=int(os.getenv("VIDEO_FRAME_MAX_WIDTH", "960")))
    parser.add_argument("--preview", action="store_true", help="Hiện thêm cửa sổ OpenCV")
    parser.add_argument("--web-port", type=int, default=int(os.getenv("VIDEO_SOURCE_PORT", "8102")))
    parser.add_argument("--no-web", action="store_true", help="Chạy một nguồn bằng CLI mà không mở API")
    parser.add_argument("--loop", action="store_true", help="Phát lại video file khi chạy hết")
    parser.add_argument("--no-realtime", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-frames", type=int, default=0, help=argparse.SUPPRESS)
    return parser


def start_dashboard(port: int) -> tuple[uvicorn.Server, threading.Thread]:
    config = uvicorn.Config(DASHBOARD_APP, host="0.0.0.0", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="video-api", daemon=True)
    thread.start()
    deadline = time.monotonic() + 8
    while not server.started and thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.05)
    if not server.started:
        raise RuntimeError(f"Không mở được API video tại cổng {port}; cổng có thể đang được sử dụng")
    LOGGER.info("API video sẵn sàng tại http://localhost:%d; hãy chọn nguồn trên website", port)
    return server, thread


def main() -> int:
    global SOURCE_CONTROLLER
    args = build_parser().parse_args()
    if args.no_web:
        if not args.source:
            LOGGER.error("Chế độ --no-web cần --source")
            return 2
        try:
            return run_source(args, threading.Event())
        except Exception as exc:
            DASHBOARD.fail(str(exc))
            LOGGER.error("Không thể chạy nguồn video: %s", exc)
            return 1

    DASHBOARD.set_port(args.web_port)
    try:
        server, server_thread = start_dashboard(args.web_port)
    except Exception as exc:
        LOGGER.error("Không thể khởi động API video: %s", exc)
        return 1

    SOURCE_CONTROLLER = SourceController(args)
    if args.source:
        SOURCE_CONTROLLER.start(args.source, loop=args.loop)

    try:
        while server_thread.is_alive():
            time.sleep(0.25)
    except KeyboardInterrupt:
        LOGGER.info("Đã nhận Ctrl+C, đang dừng dịch vụ video")
    finally:
        SOURCE_CONTROLLER.close()
        SOURCE_CONTROLLER = None
        server.should_exit = True
        server_thread.join(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
