"""ML-based alert priority scoring."""

import numpy as np
from dataclasses import dataclass
from typing import List, Optional, Dict
from sklearn.ensemble import GradientBoostingClassifier
import logging

logger = logging.getLogger(__name__)


@dataclass
class WeatherAlert:
    """Weather alert data."""
    id: str
    alert_type: str
    severity: str
    latitude: float
    longitude: float
    population_affected: int
    infrastructure_score: float
    wind_speed: Optional[float] = None
    precipitation: Optional[float] = None
    timestamp: Optional[str] = None


@dataclass
class PriorityScore:
    """Alert priority assessment."""
    alert_id: str
    score: float
    priority_level: str
    factors: Dict[str, float]


class AlertPrioritizer:
    """ML-based alert prioritization system."""

    PRIORITY_LEVELS = {
        (0.0, 0.3): "LOW",
        (0.3, 0.6): "MEDIUM",
        (0.6, 0.8): "HIGH",
        (0.8, 1.0): "CRITICAL"
    }

    SEVERITY_WEIGHTS = {
        "minor": 0.2,
        "moderate": 0.4,
        "severe": 0.7,
        "extreme": 1.0
    }

    def __init__(self):
        self._model: Optional[GradientBoostingClassifier] = None
        self._feature_names = [
            "severity_score",
            "population_log",
            "infrastructure_score",
            "wind_factor",
            "precipitation_factor"
        ]

    def _extract_features(self, alert: WeatherAlert) -> np.ndarray:
        """Extract features from alert."""
        severity_score = self.SEVERITY_WEIGHTS.get(alert.severity.lower(), 0.5)
        population_log = np.log1p(alert.population_affected) / 15
        infrastructure = alert.infrastructure_score
        wind_factor = (alert.wind_speed or 0) / 200
        precip_factor = (alert.precipitation or 0) / 500
        
        return np.array([
            severity_score,
            population_log,
            infrastructure,
            wind_factor,
            precip_factor
        ])

    def calculate_priority(self, alert: WeatherAlert) -> PriorityScore:
        """Calculate priority score for alert."""
        features = self._extract_features(alert)
        
        weights = np.array([0.25, 0.30, 0.20, 0.15, 0.10])
        score = float(np.dot(features, weights))
        score = np.clip(score, 0, 1)
        
        priority_level = "MEDIUM"
        for (low, high), level in self.PRIORITY_LEVELS.items():
            if low <= score < high:
                priority_level = level
                break
        
        factors = dict(zip(self._feature_names, features.tolist()))
        
        return PriorityScore(
            alert_id=alert.id,
            score=score,
            priority_level=priority_level,
            factors=factors
        )

    def prioritize_batch(
        self,
        alerts: List[WeatherAlert]
    ) -> List[PriorityScore]:
        """Prioritize batch of alerts."""
        scores = [self.calculate_priority(alert) for alert in alerts]
        scores.sort(key=lambda x: x.score, reverse=True)
        return scores
