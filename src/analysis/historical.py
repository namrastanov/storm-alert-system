"""Historical storm pattern analysis."""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
import logging

logger = logging.getLogger(__name__)


@dataclass
class StormTrack:
    """Historical storm track data."""
    storm_id: str
    timestamps: List[datetime]
    latitudes: List[float]
    longitudes: List[float]
    intensities: List[float]
    storm_type: str


@dataclass
class PatternCluster:
    """Cluster of similar storm patterns."""
    cluster_id: int
    storm_count: int
    avg_track: List[Tuple[float, float]]
    common_season: str
    avg_intensity: float
    typical_duration_hours: float


class HistoricalAnalyzer:
    """Analyze historical storm patterns."""

    SEASONS = {
        (3, 4, 5): "spring",
        (6, 7, 8): "summer",
        (9, 10, 11): "fall",
        (12, 1, 2): "winter"
    }

    def __init__(self):
        self._tracks: List[StormTrack] = []
        self._clusters: List[PatternCluster] = []

    def add_track(self, track: StormTrack) -> None:
        """Add storm track to history."""
        self._tracks.append(track)

    def load_from_dataframe(self, df: pd.DataFrame) -> int:
        """Load tracks from DataFrame."""
        loaded = 0
        for storm_id, group in df.groupby("storm_id"):
            track = StormTrack(
                storm_id=str(storm_id),
                timestamps=group["timestamp"].tolist(),
                latitudes=group["latitude"].tolist(),
                longitudes=group["longitude"].tolist(),
                intensities=group["intensity"].tolist(),
                storm_type=group["type"].iloc[0]
            )
            self.add_track(track)
            loaded += 1
        return loaded

    def cluster_tracks(
        self,
        eps: float = 2.0,
        min_samples: int = 3
    ) -> List[PatternCluster]:
        """Cluster similar storm tracks."""
        if len(self._tracks) < min_samples:
            return []
        
        features = self._extract_track_features()
        
        clustering = DBSCAN(eps=eps, min_samples=min_samples)
        labels = clustering.fit_predict(features)
        
        self._clusters = []
        for cluster_id in set(labels):
            if cluster_id == -1:
                continue
            
            cluster_tracks = [
                self._tracks[i] for i, l in enumerate(labels) if l == cluster_id
            ]
            
            cluster = self._create_cluster(cluster_id, cluster_tracks)
            self._clusters.append(cluster)
        
        return self._clusters

    def _extract_track_features(self) -> np.ndarray:
        """Extract features from tracks for clustering."""
        features = []
        for track in self._tracks:
            feature = [
                np.mean(track.latitudes),
                np.mean(track.longitudes),
                np.mean(track.intensities),
                len(track.timestamps)
            ]
            features.append(feature)
        return np.array(features)

    def _create_cluster(
        self,
        cluster_id: int,
        tracks: List[StormTrack]
    ) -> PatternCluster:
        """Create cluster summary."""
        all_intensities = []
        all_durations = []
        months = []
        
        for track in tracks:
            all_intensities.extend(track.intensities)
            if len(track.timestamps) >= 2:
                duration = (track.timestamps[-1] - track.timestamps[0]).total_seconds() / 3600
                all_durations.append(duration)
            months.extend([t.month for t in track.timestamps])
        
        common_month = max(set(months), key=months.count)
        season = "unknown"
        for month_range, season_name in self.SEASONS.items():
            if common_month in month_range:
                season = season_name
                break
        
        return PatternCluster(
            cluster_id=cluster_id,
            storm_count=len(tracks),
            avg_track=[],
            common_season=season,
            avg_intensity=np.mean(all_intensities),
            typical_duration_hours=np.mean(all_durations) if all_durations else 0
        )

    def analyze_seasonal_patterns(self) -> Dict[str, Dict]:
        """Analyze patterns by season."""
        seasonal_stats = {}
        
        for season_name in ["spring", "summer", "fall", "winter"]:
            season_tracks = [
                t for t in self._tracks
                if self._get_season(t.timestamps[0]) == season_name
            ]
            
            if season_tracks:
                seasonal_stats[season_name] = {
                    "count": len(season_tracks),
                    "avg_intensity": np.mean([
                        np.mean(t.intensities) for t in season_tracks
                    ]),
                    "types": list(set(t.storm_type for t in season_tracks))
                }
        
        return seasonal_stats

    def _get_season(self, dt: datetime) -> str:
        """Get season for datetime."""
        month = dt.month
        for month_range, season_name in self.SEASONS.items():
            if month in month_range:
                return season_name
        return "unknown"

    def generate_report(self) -> Dict:
        """Generate comprehensive analysis report."""
        return {
            "total_tracks": len(self._tracks),
            "clusters": len(self._clusters),
            "seasonal_patterns": self.analyze_seasonal_patterns(),
            "generated_at": datetime.utcnow().isoformat()
        }
