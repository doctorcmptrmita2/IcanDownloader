"""Property tests for retry with exponential backoff.

Property 2: Retry with Exponential Backoff
For any failed operation (authentication or download), the system SHALL retry 
up to 3 times with exponentially increasing delays (e.g., 1s, 2s, 4s).

Validates: Requirements 1.2, 2.4
"""
from hypothesis import given, strategies as st, settings

from src.services.czds_client import CZDSClient, RetryConfig


class TestRetryWithExponentialBackoff:
    """Property 2: Retry with Exponential Backoff"""
    
    @given(
        max_retries=st.integers(min_value=1, max_value=10),
        base_delay=st.floats(min_value=0.1, max_value=5.0),
    )
    @settings(max_examples=100)
    def test_backoff_delay_increases_exponentially(self, max_retries, base_delay):
        """Backoff delay SHALL increase exponentially with each attempt.
        
        Feature: icann-downloader, Property 2: Retry with Exponential Backoff
        Validates: Requirements 1.2, 2.4
        """
        config = RetryConfig(max_retries=max_retries, base_delay=base_delay)
        client = CZDSClient("user", "pass", retry_config=config)
        
        delays = []
        for attempt in range(max_retries):
            delay = client._calculate_backoff_delay(attempt)
            delays.append(delay)
        
        # Verify exponential growth (each delay should be ~2x previous)
        for i in range(1, len(delays)):
            expected_ratio = 2.0
            actual_ratio = delays[i] / delays[i-1] if delays[i-1] > 0 else 0
            
            # Allow for max_delay capping
            if delays[i] < config.max_delay:
                assert abs(actual_ratio - expected_ratio) < 0.01, \
                    f"Delay ratio should be ~2.0, got {actual_ratio}"
    
    @given(
        attempt=st.integers(min_value=0, max_value=20),
        base_delay=st.floats(min_value=0.1, max_value=5.0),
        max_delay=st.floats(min_value=10.0, max_value=120.0),
    )
    @settings(max_examples=100)
    def test_backoff_delay_respects_max_delay(self, attempt, base_delay, max_delay):
        """Backoff delay SHALL never exceed max_delay.
        
        Feature: icann-downloader, Property 2: Retry with Exponential Backoff
        Validates: Requirements 1.2, 2.4
        """
        config = RetryConfig(base_delay=base_delay, max_delay=max_delay)
        client = CZDSClient("user", "pass", retry_config=config)
        
        delay = client._calculate_backoff_delay(attempt)
        
        assert delay <= max_delay, \
            f"Delay {delay} exceeds max_delay {max_delay}"
    
    @given(base_delay=st.floats(min_value=0.1, max_value=5.0))
    @settings(max_examples=100)
    def test_first_attempt_uses_base_delay(self, base_delay):
        """First retry attempt SHALL use base_delay.
        
        Feature: icann-downloader, Property 2: Retry with Exponential Backoff
        Validates: Requirements 1.2, 2.4
        """
        config = RetryConfig(base_delay=base_delay)
        client = CZDSClient("user", "pass", retry_config=config)
        
        delay = client._calculate_backoff_delay(0)
        
        assert delay == base_delay, \
            f"First attempt delay should be {base_delay}, got {delay}"
    
    @given(
        base_delay=st.floats(min_value=0.1, max_value=2.0),
        attempt=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=100)
    def test_backoff_formula_is_correct(self, base_delay, attempt):
        """Backoff delay SHALL follow formula: base_delay * (2 ^ attempt).
        
        Feature: icann-downloader, Property 2: Retry with Exponential Backoff
        Validates: Requirements 1.2, 2.4
        """
        config = RetryConfig(base_delay=base_delay, max_delay=1000.0)  # High max to avoid capping
        client = CZDSClient("user", "pass", retry_config=config)
        
        delay = client._calculate_backoff_delay(attempt)
        expected = base_delay * (2 ** attempt)
        
        assert abs(delay - expected) < 0.001, \
            f"Delay should be {expected}, got {delay}"
    
    def test_default_retry_config(self):
        """Default retry config SHALL have max_retries=3.
        
        Feature: icann-downloader, Property 2: Retry with Exponential Backoff
        Validates: Requirements 1.2, 2.4
        """
        config = RetryConfig()
        
        assert config.max_retries == 3, \
            f"Default max_retries should be 3, got {config.max_retries}"
    
    def test_default_base_delay(self):
        """Default base_delay SHALL be 1.0 second.
        
        Feature: icann-downloader, Property 2: Retry with Exponential Backoff
        Validates: Requirements 1.2, 2.4
        """
        config = RetryConfig()
        
        assert config.base_delay == 1.0, \
            f"Default base_delay should be 1.0, got {config.base_delay}"
    
    @given(max_retries=st.integers(min_value=1, max_value=10))
    @settings(max_examples=100)
    def test_retry_config_max_retries(self, max_retries):
        """RetryConfig SHALL accept custom max_retries.
        
        Feature: icann-downloader, Property 2: Retry with Exponential Backoff
        Validates: Requirements 1.2, 2.4
        """
        config = RetryConfig(max_retries=max_retries)
        
        assert config.max_retries == max_retries
