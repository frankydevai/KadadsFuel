"""
metrics.py — Lightweight in-process metrics. asyncio-safe, no external deps.

Exposed at /metrics by the health server (when wired up), and by the /metrics
admin Telegram command. Counters never reset; gauges reflect current state;
timers keep a rolling window of the last N samples for p50/p95/max latency.

USAGE
─────
    from dieselup.metrics import incr, gauge, Timer

    incr("alerts_sent_ok")
    gauge("trucks_active_total", len(active_orders))

    with Timer("load_sync_cycle"):
        await sync_active_loads(bot)
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque

# Last N samples kept per timer — bounded memory, plenty for p95/max.
_TIMER_WINDOW = 200

# threading.Lock is asyncio-safe in practice: we never hold it across an
# await, and the writes are atomic. Avoids needing a running event loop for
# module import.
_lock: threading.Lock = threading.Lock()
_counters: dict[str, int] = defaultdict(int)
_gauges: dict[str, float] = {}
_timers: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=_TIMER_WINDOW))


def incr(name: str, by: int = 1) -> None:
    """Increment a monotonic counter."""
    with _lock:
        _counters[name] += by


def gauge(name: str, value: float) -> None:
    """Set the current value of a gauge."""
    with _lock:
        _gauges[name] = float(value)


class Timer:
    """Context manager — records elapsed seconds into a rolling window.

    Works with both sync `with Timer(...)` and async `async with Timer(...)`
    via __aenter__/__aexit__. Internally uses perf_counter, so the duration
    is wall-clock not CPU time.
    """
    __slots__ = ("name", "_start")

    def __init__(self, name: str):
        self.name = name
        self._start: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_: object) -> None:
        elapsed = time.perf_counter() - self._start
        with _lock:
            _timers[self.name].append(elapsed)

    async def __aenter__(self) -> "Timer":
        return self.__enter__()

    async def __aexit__(self, *_: object) -> None:
        self.__exit__()


def _percentile(sorted_samples: list[float], pct: int) -> float:
    if not sorted_samples:
        return 0.0
    k = int(len(sorted_samples) * pct / 100)
    return sorted_samples[min(k, len(sorted_samples) - 1)]


def snapshot() -> dict:
    """Return a JSON-serializable snapshot of every metric currently tracked."""
    with _lock:
        timer_summary: dict[str, dict[str, float]] = {}
        for name, samples in _timers.items():
            if not samples:
                continue
            s = sorted(samples)
            timer_summary[name] = {
                "count": len(s),
                "avg_ms": round(sum(s) / len(s) * 1000, 2),
                "p50_ms": round(_percentile(s, 50) * 1000, 2),
                "p95_ms": round(_percentile(s, 95) * 1000, 2),
                "max_ms": round(max(s) * 1000, 2),
            }
        return {
            "counters": dict(_counters),
            "gauges": dict(_gauges),
            "timers": timer_summary,
        }


def render_text() -> str:
    """Plain-text format suitable for HTTP /metrics or a Telegram code block."""
    snap = snapshot()
    lines: list[str] = []

    if snap["counters"]:
        lines.append("# COUNTERS")
        for k in sorted(snap["counters"]):
            lines.append(f"{k} {snap['counters'][k]}")
        lines.append("")

    if snap["gauges"]:
        lines.append("# GAUGES")
        for k in sorted(snap["gauges"]):
            lines.append(f"{k} {snap['gauges'][k]}")
        lines.append("")

    if snap["timers"]:
        lines.append("# TIMERS (ms)")
        for k in sorted(snap["timers"]):
            t = snap["timers"][k]
            lines.append(
                f"{k}  count={t['count']} "
                f"avg={t['avg_ms']} p50={t['p50_ms']} "
                f"p95={t['p95_ms']} max={t['max_ms']}"
            )
    return "\n".join(lines) + "\n"
