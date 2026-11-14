"""Bounded, versioned pipeline configuration; no executable metadata."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
import json
import math
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    window_seconds: int = 1800
    lateness_seconds: int = 120
    horizon_seconds: int = 1800
    min_points: int = 4
    alert_cooldown_seconds: int = 1800
    false_alert_budget_per_machine_day: float = 2.0
    min_recall: float = 0.70
    min_lead_seconds: int = 300
    max_p95_inference_ms: float = 1000.0
    max_batch_rows: int = 10000
    max_batch_bytes: int = 16777216
    feature_version: str = "v1"
    temperature_min_c: float = -40.0
    temperature_max_c: float = 180.0
    vibration_max_mm_s: float = 30.0
    pressure_max_bar: float = 30.0
    max_payload_bytes: int = 65536

    def __post_init__(self) -> None:
        integer_ranges = {
            "window_seconds": (60, 86400),
            "lateness_seconds": (0, 3600),
            "horizon_seconds": (60, 86400),
            "min_points": (2, 10000),
            "alert_cooldown_seconds": (0, 86400),
            "min_lead_seconds": (0, 86400),
            "max_batch_rows": (1, 100000),
            "max_batch_bytes": (1024, 268435456),
            "max_payload_bytes": (1024, 1048576),
        }
        for name, (low, high) in integer_ranges.items():
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be an integer in [{low}, {high}]")
        numeric_ranges = {
            "false_alert_budget_per_machine_day": (0.0, 1000.0),
            "min_recall": (0.0, 1.0),
            "max_p95_inference_ms": (0.01, 60000.0),
            "temperature_min_c": (-273.15, 300.0),
            "temperature_max_c": (-273.15, 300.0),
            "vibration_max_mm_s": (0.01, 1000.0),
            "pressure_max_bar": (0.01, 1000.0),
        }
        for name, (low, high) in numeric_ranges.items():
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not low <= value <= high
            ):
                raise ValueError(f"{name} must be finite and in [{low}, {high}]")
        if self.temperature_min_c >= self.temperature_max_c:
            raise ValueError("temperature_min_c must be below temperature_max_c")
        if self.min_lead_seconds > self.horizon_seconds:
            raise ValueError("min_lead_seconds must not exceed horizon_seconds")
        if self.feature_version != "v1":
            raise ValueError("Only the reviewed feature_version 'v1' is implemented")

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, values: dict) -> "PipelineConfig":
        if not isinstance(values, dict):
            raise ValueError("Pipeline configuration must be an object")
        unknown = set(values) - {field.name for field in fields(cls)}
        if unknown:
            raise ValueError(f"Unknown configuration keys: {sorted(unknown)}")
        return cls(**values)

    @classmethod
    def load(cls, path: str | Path) -> "PipelineConfig":
        with Path(path).open(encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))


def load_config(path: str | Path | None = None) -> PipelineConfig:
    return PipelineConfig.load(path) if path is not None else PipelineConfig()

