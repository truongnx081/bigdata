"""Pure business rules mirrored by Spark SQL expressions for lightweight tests."""

from __future__ import annotations


def congestion_level(speed_kmh: float, density_pct: float) -> str:
    if density_pct >= 80 and speed_kmh < 18:
        return "critical"
    if density_pct >= 60 and speed_kmh < 28:
        return "heavy"
    if density_pct >= 40 or speed_kmh < 38:
        return "moderate"
    return "smooth"


def risk_score(speed_kmh: float, density_pct: float, anomaly_score: float) -> float:
    score = density_pct * 0.58 + (65.0 - speed_kmh) * 0.62 + anomaly_score * 16.0
    return round(max(0.0, min(100.0, score)), 1)
