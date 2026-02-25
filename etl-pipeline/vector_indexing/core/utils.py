"""Utility functions for v2 module."""

import time
from typing import Any, Callable, Optional


class IncrementalMovingAverage:
    """Computes a cumulative average incrementally without storing all values.

    This is mathematically equivalent to sum(values) / len(values), not an approximation.
    formula: new_avg = old_avg + (new_value - old_avg) / new_count

    See: https://en.wikipedia.org/wiki/Moving_average#Cumulative_average
    """

    def __init__(self):
        self._average: float = 0.0
        self._count: int = 0

    def update(self, value: float) -> None:
        """Add a new value to the moving average."""
        self._count += 1
        self._average += (value - self._average) / self._count

    @property
    def value(self) -> float:
        """Current average value."""
        return self._average

    @property
    def count(self) -> int:
        """Number of values recorded."""
        return self._count

    @property
    def total(self) -> float:
        """Total sum of all values (avg * count)."""
        return self._average * self._count


class Timer:
    """Context manager for timing operations. Accepts a callback that will be called with the name and elapsed time when the timer exits."""

    def __init__(
        self,
        name: str,
        record: Optional[dict] = None,
        on_exit: Optional[Callable[[str, float], None]] = None,
    ):
        """Initialize timer."""
        self.name = name
        self.record = record
        self.on_exit_callback = on_exit
        self.start: float = 0
        self.elapsed: float = 0

    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args) -> None:
        self.elapsed = time.perf_counter() - self.start
        if self.record is not None:
            self.record[self.name] = self.elapsed
        if self.on_exit_callback is not None:
            self.on_exit_callback(self.name, self.elapsed)


class TimerSet:
    """Collection of timers that records all timings to a single dict.
    This can be used to time the same event multiple times and compute a moving average.
    Usage:
        timers = TimerSet()
        with timers.time("insert"):
            # operation
        with timers.time("search"):
            # operation
        # Same event multiple times - computes moving average
        for item in items:
            with timers.time("process"):
                # operation
    """

    def __init__(self):
        self._averages: dict[str, IncrementalMovingAverage] = {}

    def time(self, name: str) -> Timer:
        """Get a timer that records a named operation to this set"""
        return Timer(name, on_exit=lambda n, e: self.record(n, e))

    def record(self, name: str, elapsed: float) -> None:
        """Record a timing, updating the moving average."""
        if name not in self._averages:
            self._averages[name] = IncrementalMovingAverage()
        self._averages[name].update(elapsed)

    @property
    def timings(self) -> dict[str, float]:
        """Get all average timings."""
        return {name: avg.value for name, avg in self._averages.items()}

    @property
    def counts(self) -> dict[str, int]:
        """Get all counts."""
        return {name: avg.count for name, avg in self._averages.items()}

    def total(self, name: str) -> float:
        """Get total time for an event (avg * count)."""
        if name not in self._averages:
            return 0.0
        return self._averages[name].total

    def summary(self, unit: str = "ms") -> str:
        """Get a formatted summary of all timings."""
        multiplier = 1000 if unit == "ms" else 1
        lines = []
        for name, avg in self._averages.items():
            if avg.count > 1:
                lines.append(
                    f"  {name}: {avg.value * multiplier:.1f}{unit} avg × {avg.count} = {avg.total * multiplier:.1f}{unit}"
                )
            else:
                lines.append(f"  {name}: {avg.value * multiplier:.1f}{unit}")
        return "\n".join(lines)


def format_bytes(size: int) -> str:
    """Format bytes as human-readable string.

    Args:
        size: Size in bytes.

    Returns:
        Human-readable string (e.g., "1.5 GB").
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"
