"""Property tests for log entry completeness.

Property 8: Log Entry Completeness
For any log entry created during download or parse operations, the entry SHALL contain:
timestamp, operation type, TLD name, duration, status, and relevant metrics 
(file_size for downloads, records_count for parsing).

Validates: Requirements 7.1, 7.2, 7.3
"""
from datetime import datetime
from hypothesis import given, strategies as st, settings

from src.services.logger_service import LoggerService, LogEntry
from src.models import DownloadResult


# Strategy for TLD names
tld_strategy = st.sampled_from(['com', 'net', 'org', 'io', 'dev', 'app'])

# Strategy for file sizes
file_size_strategy = st.integers(min_value=1000, max_value=1000000000)

# Strategy for durations
duration_strategy = st.integers(min_value=1, max_value=3600)

# Strategy for record counts
records_count_strategy = st.integers(min_value=0, max_value=10000000)


class TestLogEntryCompleteness:
    """Property 8: Log Entry Completeness"""
    
    @given(
        tld=tld_strategy,
        file_size=file_size_strategy,
        duration=duration_strategy,
    )
    @settings(max_examples=100)
    def test_download_log_contains_required_fields(self, tld, file_size, duration):
        """Download log entries SHALL contain timestamp, operation_type, tld, duration, status, file_size.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1, 7.2
        """
        logger_service = LoggerService()
        
        # Create a successful download result
        result = DownloadResult(
            tld=tld,
            file_path=f"/tmp/{tld}.zone.gz",
            file_size=file_size,
            download_duration=duration,
            status="success",
        )
        
        # Log download complete
        entry = logger_service.log_download_complete(tld, result)
        
        # Verify required fields
        assert entry.timestamp is not None, "timestamp is required"
        assert isinstance(entry.timestamp, datetime), "timestamp must be datetime"
        assert entry.operation_type == "download", "operation_type must be 'download'"
        assert entry.tld == tld, f"tld must be '{tld}'"
        assert entry.duration == duration, f"duration must be {duration}"
        assert entry.status == "success", "status must be 'success'"
        assert entry.file_size == file_size, f"file_size must be {file_size}"
    
    @given(
        tld=tld_strategy,
        records_count=records_count_strategy,
        duration=duration_strategy,
    )
    @settings(max_examples=100)
    def test_parse_log_contains_required_fields(self, tld, records_count, duration):
        """Parse log entries SHALL contain timestamp, operation_type, tld, duration, status, records_count.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1, 7.3
        """
        logger_service = LoggerService()
        
        # Log parse complete
        entry = logger_service.log_parse_complete(tld, records_count, duration)
        
        # Verify required fields
        assert entry.timestamp is not None, "timestamp is required"
        assert isinstance(entry.timestamp, datetime), "timestamp must be datetime"
        assert entry.operation_type == "parse", "operation_type must be 'parse'"
        assert entry.tld == tld, f"tld must be '{tld}'"
        assert entry.duration == duration, f"duration must be {duration}"
        assert entry.status == "success", "status must be 'success'"
        assert entry.records_count == records_count, f"records_count must be {records_count}"
    
    @given(
        tld=tld_strategy,
        error_message=st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
    )
    @settings(max_examples=100)
    def test_failed_download_log_contains_error(self, tld, error_message):
        """Failed download log entries SHALL contain error_message.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1, 7.2
        """
        logger_service = LoggerService()
        
        # Create a failed download result
        result = DownloadResult(
            tld=tld,
            file_path="",
            file_size=0,
            download_duration=10,
            status="failed",
            error_message=error_message,
        )
        
        # Log download complete (failed)
        entry = logger_service.log_download_complete(tld, result)
        
        # Verify error fields
        assert entry.status == "failed", "status must be 'failed'"
        assert entry.error_message == error_message, "error_message must match"
        assert entry.level == "ERROR", "level must be 'ERROR' for failures"
    
    @given(
        tld=tld_strategy,
        records_count=records_count_strategy,
        duration=duration_strategy,
        error_message=st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
    )
    @settings(max_examples=100)
    def test_failed_parse_log_contains_error(self, tld, records_count, duration, error_message):
        """Failed parse log entries SHALL contain error_message.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1, 7.3
        """
        logger_service = LoggerService()
        
        # Log parse complete with error
        entry = logger_service.log_parse_complete(tld, records_count, duration, error_message)
        
        # Verify error fields
        assert entry.status == "failed", "status must be 'failed'"
        assert entry.error_message == error_message, "error_message must match"
        assert entry.level == "ERROR", "level must be 'ERROR' for failures"
    
    @given(tld=tld_strategy)
    @settings(max_examples=100)
    def test_download_start_log_has_in_progress_status(self, tld):
        """Download start log entries SHALL have status 'in_progress'.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1
        """
        logger_service = LoggerService()
        
        entry = logger_service.log_download_start(tld)
        
        assert entry.status == "in_progress", "status must be 'in_progress'"
        assert entry.operation_type == "download", "operation_type must be 'download'"
        assert entry.tld == tld, f"tld must be '{tld}'"
    
    @given(tld=tld_strategy)
    @settings(max_examples=100)
    def test_parse_start_log_has_in_progress_status(self, tld):
        """Parse start log entries SHALL have status 'in_progress'.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1
        """
        logger_service = LoggerService()
        
        entry = logger_service.log_parse_start(tld)
        
        assert entry.status == "in_progress", "status must be 'in_progress'"
        assert entry.operation_type == "parse", "operation_type must be 'parse'"
        assert entry.tld == tld, f"tld must be '{tld}'"
    
    def test_log_entry_to_dict_contains_all_fields(self):
        """LogEntry.to_dict() SHALL include all required fields.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1, 7.2, 7.3
        """
        entry = LogEntry(
            timestamp=datetime.now(),
            level="INFO",
            message="Test message",
            operation_type="download",
            tld="com",
            duration=100,
            status="success",
            file_size=1000000,
            records_count=50000,
        )
        
        d = entry.to_dict()
        
        # Verify all fields are present
        assert "timestamp" in d
        assert "level" in d
        assert "message" in d
        assert "operation_type" in d
        assert "tld" in d
        assert "duration" in d
        assert "status" in d
        assert "file_size" in d
        assert "records_count" in d
        assert "error_message" in d
        assert "context" in d
    
    @given(limit=st.integers(min_value=1, max_value=50))
    @settings(max_examples=20)
    def test_get_recent_logs_respects_limit(self, limit):
        """get_recent_logs() SHALL return at most 'limit' entries.
        
        Feature: icann-downloader, Property 8: Log Entry Completeness
        Validates: Requirements 7.1
        """
        logger_service = LoggerService()
        
        # Create more logs than limit
        for i in range(limit + 10):
            logger_service.log("INFO", f"Message {i}")
        
        logs = logger_service.get_recent_logs(limit)
        
        assert len(logs) <= limit, f"Should return at most {limit} entries"
