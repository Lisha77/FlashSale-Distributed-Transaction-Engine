"""
telemetry/bus.py
----------------
Single shared publish/subscribe event bus used by every engine component and
by the dashboard. This is the backbone of the live telemetry layer.
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Any, Dict, List


class TelemetryBus:
    def __init__(self, name: str = "FlashSaleEngine"):
        self.name = name
        self.subscribers: List[Callable[[Dict[str, Any]], None]] = []
        self._lock = threading.Lock()

    def subscribe(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            if callback not in self.subscribers:
                self.subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            if callback in self.subscribers:
                self.subscribers.remove(callback)

    def emit(self, event: Dict[str, object]) -> None:
        snapshot = dict(event)
        snapshot.setdefault("bus_time", time.time())
        with self._lock:
            for cb in list(self.subscribers):
                try:
                    cb(snapshot)
                except Exception:
                    continue

    def shutdown(self) -> None:
        with self._lock:
            self.subscribers.clear()
