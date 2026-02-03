"""Tests for alert prioritizer."""

import pytest
from storm_alert_system.alerts.prioritizer import AlertPrioritizer, WeatherAlert, PriorityScore


class TestAlertPrioritizer:
    """Test suite for AlertPrioritizer."""

    @pytest.fixture
    def prioritizer(self):
        """Create prioritizer instance."""
        return AlertPrioritizer()

    @pytest.fixture
    def high_severity_alert(self):
        """Create high severity alert."""
        return WeatherAlert(
            id="alert-001",
            alert_type="tornado",
            severity="extreme",
            latitude=35.0,
            longitude=-97.0,
            population_affected=500000,
            infrastructure_score=0.9,
            wind_speed=150,
            precipitation=100
        )

    @pytest.fixture
    def low_severity_alert(self):
        """Create low severity alert."""
        return WeatherAlert(
            id="alert-002",
            alert_type="frost",
            severity="minor",
            latitude=40.0,
            longitude=-80.0,
            population_affected=1000,
            infrastructure_score=0.2,
            wind_speed=5,
            precipitation=0
        )

    def test_high_severity_gets_high_score(self, prioritizer, high_severity_alert):
        """Test that high severity alerts get high scores."""
        result = prioritizer.calculate_priority(high_severity_alert)
        assert result.score > 0.6
        assert result.priority_level in ["HIGH", "CRITICAL"]

    def test_low_severity_gets_low_score(self, prioritizer, low_severity_alert):
        """Test that low severity alerts get low scores."""
        result = prioritizer.calculate_priority(low_severity_alert)
        assert result.score < 0.4
        assert result.priority_level in ["LOW", "MEDIUM"]

    def test_batch_sorted_by_priority(
        self,
        prioritizer,
        high_severity_alert,
        low_severity_alert
    ):
        """Test batch prioritization is sorted correctly."""
        alerts = [low_severity_alert, high_severity_alert]
        results = prioritizer.prioritize_batch(alerts)
        
        assert results[0].alert_id == high_severity_alert.id
        assert results[1].alert_id == low_severity_alert.id

    def test_score_in_valid_range(self, prioritizer, high_severity_alert):
        """Test score is between 0 and 1."""
        result = prioritizer.calculate_priority(high_severity_alert)
        assert 0 <= result.score <= 1

    def test_factors_included_in_result(self, prioritizer, high_severity_alert):
        """Test that factors dict is populated."""
        result = prioritizer.calculate_priority(high_severity_alert)
        assert len(result.factors) > 0
        assert "severity_score" in result.factors
