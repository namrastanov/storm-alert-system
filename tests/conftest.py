"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def sample_config():
    """Provide sample configuration for tests."""
    return {
        "api_key": "test_key",
        "database_url": "sqlite:///:memory:",
        "log_level": "DEBUG"
    }


@pytest.fixture
def temp_data_dir(tmp_path):
    """Provide temporary directory for test data."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir
