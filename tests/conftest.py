"""Pytest configuration and fixtures."""
import os
import pytest
from hypothesis import settings, Verbosity

# Hypothesis profiles
settings.register_profile("ci", max_examples=100)
settings.register_profile("dev", max_examples=10)
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "ci"))


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing."""
    monkeypatch.setenv("ICANN_USER", "test_user")
    monkeypatch.setenv("ICANN_PASS", "test_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "test_password")
