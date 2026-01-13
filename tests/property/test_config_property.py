"""Property tests for configuration loading.

Property 11: Environment Configuration Loading
For any required environment variable (ICANN_USER, ICANN_PASS, DB_HOST, CLICKHOUSE_PASSWORD),
the system SHALL read and use the value from the environment.

Validates: Requirements 10.2
"""
import os
import pytest
from hypothesis import given, strategies as st, settings, HealthCheck
from unittest.mock import patch

from src.config import Config, ConfigurationError


# Strategy for valid environment variable values (non-empty strings without null chars)
valid_env_value = st.text(
    alphabet=st.characters(blacklist_characters='\x00'),
    min_size=1, 
    max_size=100
).filter(lambda x: x.strip())


class TestEnvironmentConfigurationLoading:
    """Property 11: Environment Configuration Loading"""
    
    @given(
        icann_user=valid_env_value,
        icann_pass=valid_env_value,
        db_host=valid_env_value,
        clickhouse_password=valid_env_value,
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_config_reads_all_required_env_vars(
        self, icann_user, icann_pass, db_host, clickhouse_password
    ):
        """For any set of environment variables, Config.from_env() SHALL read and use them.
        
        Feature: icann-downloader, Property 11: Environment Configuration Loading
        Validates: Requirements 10.2
        """
        env_vars = {
            "ICANN_USER": icann_user,
            "ICANN_PASS": icann_pass,
            "DB_HOST": db_host,
            "CLICKHOUSE_PASSWORD": clickhouse_password,
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            # Load config
            config = Config.from_env()
            
            # Verify all values are read correctly
            assert config.icann_user == icann_user
            assert config.icann_pass == icann_pass
            assert config.db_host == db_host
            assert config.clickhouse_password == clickhouse_password
    
    @given(
        port=st.integers(min_value=1, max_value=65535),
        batch_size=st.integers(min_value=1, max_value=100000),
        max_retries=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_config_reads_optional_env_vars(self, port, batch_size, max_retries):
        """For any optional environment variables, Config.from_env() SHALL use them when set.
        
        Feature: icann-downloader, Property 11: Environment Configuration Loading
        Validates: Requirements 10.2
        """
        env_vars = {
            "ICANN_USER": "test_user",
            "ICANN_PASS": "test_pass",
            "DB_HOST": "localhost",
            "CLICKHOUSE_PASSWORD": "test_password",
            "PORT": str(port),
            "BATCH_SIZE": str(batch_size),
            "MAX_RETRIES": str(max_retries),
        }
        
        with patch.dict(os.environ, env_vars, clear=False):
            # Load config
            config = Config.from_env()
            
            # Verify optional values are read correctly
            assert config.port == port
            assert config.batch_size == batch_size
            assert config.max_retries == max_retries
    
    def test_config_raises_error_when_required_vars_missing(self):
        """Config.from_env() SHALL raise ConfigurationError when required vars are missing.
        
        Feature: icann-downloader, Property 11: Environment Configuration Loading
        Validates: Requirements 10.2
        """
        # Create environment without required variables
        env_vars = {}
        
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ConfigurationError) as exc_info:
                Config.from_env()
            
            # Verify error message contains missing variable names
            error_msg = str(exc_info.value)
            assert "ICANN_USER" in error_msg
            assert "ICANN_PASS" in error_msg
            assert "DB_HOST" in error_msg
            assert "CLICKHOUSE_PASSWORD" in error_msg
    
    @given(missing_var=st.sampled_from(["ICANN_USER", "ICANN_PASS", "DB_HOST", "CLICKHOUSE_PASSWORD"]))
    @settings(max_examples=4, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_config_raises_error_for_each_missing_required_var(self, missing_var):
        """For any single missing required variable, Config SHALL raise ConfigurationError.
        
        Feature: icann-downloader, Property 11: Environment Configuration Loading
        Validates: Requirements 10.2
        """
        # Set all required variables
        env_vars = {
            "ICANN_USER": "test_user",
            "ICANN_PASS": "test_pass",
            "DB_HOST": "localhost",
            "CLICKHOUSE_PASSWORD": "test_password",
        }
        
        # Remove one required variable
        del env_vars[missing_var]
        
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ConfigurationError) as exc_info:
                Config.from_env()
            
            # Verify error message contains the missing variable name
            assert missing_var in str(exc_info.value)
