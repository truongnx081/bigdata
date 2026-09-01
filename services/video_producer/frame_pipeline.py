"""Low-latency video capture that keeps only the newest decoded frame."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

import cv2
import numpy as np


LOGGER = logging.getLogger("video-source")


@dataclass(frozen=True)
class CapturedFrame:
    sequence: int
    source_frame_index: int
    generation: int
    timestamp: float
    image: np.ndarray


class LatestFrameCapture:
    """Decode in a dedicated thread and overwrite stale frames instead of queueing them."""

    def __init__(
        self,
        source: str,
        *,
        loop: bool,
        realtime: bool,
        stop_event: threading.Event,
    ) -> None:
        self.source = source
        self.loop = loop
        self.realtime = realtime
        self.stop_event = stop_event
        self.capture = cv2.VideoCapture()
        self.capture.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 15_000)
        self.capture.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5_000)
        self.capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not self.capture.open(source):
            raise RuntimeError("OpenCV không mở được video. Hãy kiểm tra định dạng file.")
        self.capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)

        fps = float(self.capture.get(cv2.CAP_PROP_FPS) or 0)
        self.fps = fps if 1 < fps <= 240 else 25.0
        self.frame_count_hint = int(self.capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        self.condition = threading.Condition()
        self.latest: CapturedFrame | None = None
        self.sequence = 0
        self.ended = False
        self.error: str | None = None
        self.started = False
        self.thread = threading.Thread(target=self._run, name="latest-frame-capture", daemon=True)

    def start(self) -> None:
        self.started = True
        self.thread.start()

    def read_after(self, sequence: int, timeout: float = 1.0) -> CapturedFrame | None:
        deadline = time.monotonic() + timeout
        with self.condition:
            while not self.stop_event.is_set() and not self.ended:
                if self.latest and self.latest.sequence > sequence:
                    return self.latest
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self.condition.wait(remaining)
            if self.latest and self.latest.sequence > sequence:
                return self.latest
        return None

    def close(self) -> None:
        self.stop_event.set()
        with self.condition:
            self.condition.notify_all()
        if not self.started:
            self.capture.release()
            return
        self.thread.join(timeout=6)
        if self.thread.is_alive():
            self.capture.release()
            self.thread.join(timeout=2)

    def _rewind(self) -> bool:
        if self.capture.set(cv2.CAP_PROP_POS_FRAMES, 0):
            return True
        self.capture.release()
        return self.capture.open(self.source)

    def _run(self) -> None:
        frame_index = 0
        generation = 0
        timeline_offset = 0.0
        playback_started_at = time.monotonic()
        try:
            while not self.stop_event.is_set():
                ok, frame = self.capture.read()
                if not ok:
                    if self.loop and not self.stop_event.is_set():
                        timeline_offset += frame_index / self.fps
                        if not self._rewind():
                            self.error = "Không thể phát lại nguồn video"
                            break
                        frame_index = 0
                        generation += 1
                        playback_started_at = time.monotonic()
                        LOGGER.info("Video đã hết, bắt đầu phát lại")
                        continue
                    break

                frame_index += 1
                captured_at = time.monotonic()
                media_timestamp = timeline_offset + frame_index / self.fps
                with self.condition:
                    self.sequence += 1
                    self.latest = CapturedFrame(
                        sequence=self.sequence,
                        source_frame_index=frame_index,
                        generation=generation,
                        timestamp=media_timestamp,
                        image=frame,
                    )
                    self.condition.notify_all()

                if self.realtime:
                    target = playback_started_at + frame_index / self.fps
                    remaining = target - time.monotonic()
                    if remaining > 0 and self.stop_event.wait(remaining):
                        break
        except Exception as exc:  # pragma: no cover - codec/backend dependent
            if self.stop_event.is_set():
                LOGGER.debug("Video capture stopped while OpenCV was reading: %s", exc)
            else:
                self.error = str(exc)
                LOGGER.exception("Lỗi luồng đọc video")
        finally:
            self.capture.release()
            with self.condition:
                self.ended = True
                self.condition.notify_all()
