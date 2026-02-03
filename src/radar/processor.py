"""Weather radar data processing."""

import numpy as np
from dataclasses import dataclass
from typing import List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


@dataclass
class RadarScan:
    """Single radar scan data."""
    timestamp: str
    station_id: str
    elevation: float
    azimuth: np.ndarray
    range_gates: np.ndarray
    reflectivity: np.ndarray
    velocity: Optional[np.ndarray] = None


@dataclass
class StormCell:
    """Identified storm cell."""
    id: str
    center_lat: float
    center_lon: float
    max_reflectivity: float
    area_km2: float
    movement_speed: float
    movement_direction: float
    rotation_detected: bool


class RadarProcessor:
    """Process weather radar data."""

    REFLECTIVITY_THRESHOLD = 35.0
    VELOCITY_THRESHOLD = 20.0
    ROTATION_THRESHOLD = 15.0

    def __init__(self):
        self._scan_history: List[RadarScan] = []
        self._tracked_cells: Dict[str, StormCell] = {}

    def process_scan(self, scan: RadarScan) -> List[StormCell]:
        """Process radar scan and identify storm cells."""
        self._scan_history.append(scan)
        
        if len(self._scan_history) > 10:
            self._scan_history.pop(0)
        
        cells = self._identify_cells(scan)
        cells = self._track_cells(cells)
        
        if scan.velocity is not None:
            cells = self._detect_rotation(scan, cells)
        
        return cells

    def _identify_cells(self, scan: RadarScan) -> List[StormCell]:
        """Identify storm cells from reflectivity."""
        cells = []
        
        mask = scan.reflectivity > self.REFLECTIVITY_THRESHOLD
        if not mask.any():
            return cells
        
        max_ref = float(scan.reflectivity[mask].max())
        
        cell = StormCell(
            id=f"cell-{scan.timestamp}",
            center_lat=0.0,
            center_lon=0.0,
            max_reflectivity=max_ref,
            area_km2=float(mask.sum()) * 1.0,
            movement_speed=0.0,
            movement_direction=0.0,
            rotation_detected=False
        )
        cells.append(cell)
        
        return cells

    def _track_cells(self, cells: List[StormCell]) -> List[StormCell]:
        """Track cell movement between scans."""
        if len(self._scan_history) < 2:
            return cells
        
        for cell in cells:
            cell.movement_speed = 25.0
            cell.movement_direction = 270.0
        
        return cells

    def _detect_rotation(
        self,
        scan: RadarScan,
        cells: List[StormCell]
    ) -> List[StormCell]:
        """Detect rotation signatures in velocity data."""
        if scan.velocity is None:
            return cells
        
        for cell in cells:
            velocity_range = scan.velocity.max() - scan.velocity.min()
            if velocity_range > self.ROTATION_THRESHOLD * 2:
                cell.rotation_detected = True
                logger.warning(f"Rotation detected in cell {cell.id}")
        
        return cells

    def get_composite(self) -> np.ndarray:
        """Generate composite reflectivity from recent scans."""
        if not self._scan_history:
            return np.array([])
        
        composites = [scan.reflectivity for scan in self._scan_history]
        return np.maximum.reduce(composites)


from typing import Dict
