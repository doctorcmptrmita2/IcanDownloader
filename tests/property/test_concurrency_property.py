"""Property tests for job concurrency prevention.

Property 6: Job Concurrency Prevention
For any download job in progress, subsequent download triggers SHALL be 
rejected until the current job completes.

Validates: Requirements 5.3
"""
import threading
import time
from unittest.mock import MagicMock, patch
from hypothesis import given, strategies as st, settings

from src.models import JobStatus
from src.services.download_service import DownloadService
from src.services.logger_service import LoggerService


class TestJobConcurrencyPrevention:
    """Property 6: Job Concurrency Prevention"""
    
    def _create_mock_service(self) -> DownloadService:
        """Create a DownloadService with mocked dependencies."""
        mock_czds = MagicMock()
        mock_czds.authenticate.return_value = "token"
        mock_czds.get_approved_tlds.return_value = ["com", "net"]
        mock_czds.download_zone_file.return_value = MagicMock(
            is_success=True,
            file_path="/tmp/test.zone.gz",
            file_size=1000,
            download_duration=1,
            records_count=0,
            parse_duration=0,
            status="success",
            error_message=None,
        )
        
        mock_parser_factory = MagicMock()
        mock_parser = MagicMock()
        mock_parser.parse_zone_file.return_value = iter([])
        mock_parser_factory.return_value = mock_parser
        
        mock_repo = MagicMock()
        mock_logger = LoggerService()
        
        return DownloadService(
            czds_client=mock_czds,
            parser_factory=mock_parser_factory,
            repository=mock_repo,
            logger_service=mock_logger,
            temp_dir="/tmp",
        )
    
    def test_concurrent_download_rejected_when_running(self):
        """Subsequent download triggers SHALL be rejected when job is running.
        
        Feature: icann-downloader, Property 6: Job Concurrency Prevention
        Validates: Requirements 5.3
        """
        service = self._create_mock_service()
        
        # Manually set job as running
        service._job_status.state = "running"
        service._job_status.started_at = time.time()
        
        # Try to start another download
        result = service.run_full_download()
        
        # Should return None (rejected)
        assert result is None, "Concurrent download should be rejected"
    
    def test_download_allowed_when_idle(self):
        """Download SHALL be allowed when no job is running.
        
        Feature: icann-downloader, Property 6: Job Concurrency Prevention
        Validates: Requirements 5.3
        """
        service = self._create_mock_service()
        
        # Ensure job is idle
        assert service._job_status.is_idle, "Job should be idle initially"
        
        # Start download
        result = service.run_full_download()
        
        # Should succeed
        assert result is not None, "Download should be allowed when idle"
    
    def test_is_running_returns_correct_state(self):
        """is_running() SHALL return True when job is in progress.
        
        Feature: icann-downloader, Property 6: Job Concurrency Prevention
        Validates: Requirements 5.3
        """
        service = self._create_mock_service()
        
        # Initially idle
        assert service.is_running() is False, "Should be False when idle"
        
        # Set to running
        service._job_status.state = "running"
        
        assert service.is_running() is True, "Should be True when running"
    
    def test_job_status_transitions(self):
        """Job status SHALL transition correctly through states.
        
        Feature: icann-downloader, Property 6: Job Concurrency Prevention
        Validates: Requirements 5.3
        """
        status = JobStatus()
        
        # Initial state
        assert status.is_idle, "Initial state should be idle"
        assert not status.is_running, "Should not be running initially"
        
        # Start job
        status.start(10)
        assert status.is_running, "Should be running after start"
        assert not status.is_idle, "Should not be idle after start"
        assert status.total_tlds == 10, "total_tlds should be set"
        
        # Update progress
        status.update_progress(5, 10, "com")
        assert status.completed_tlds == 5, "completed_tlds should be updated"
        assert status.progress_percent == 50, "progress should be 50%"
        assert status.current_tld == "com", "current_tld should be set"
        
        # Complete job
        status.complete()
        assert status.is_idle, "Should be idle after complete"
        assert not status.is_running, "Should not be running after complete"
        assert status.progress_percent == 100, "progress should be 100%"
    
    @given(total_tlds=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_progress_calculation(self, total_tlds):
        """Progress percentage SHALL be calculated correctly.
        
        Feature: icann-downloader, Property 6: Job Concurrency Prevention
        Validates: Requirements 5.3
        """
        status = JobStatus()
        status.start(total_tlds)
        
        for completed in range(total_tlds + 1):
            status.update_progress(completed, total_tlds, f"tld{completed}")
            
            expected_percent = int((completed / total_tlds) * 100)
            assert status.progress_percent == expected_percent, \
                f"Progress should be {expected_percent}%, got {status.progress_percent}%"
    
    def test_get_current_status_returns_copy(self):
        """get_current_status() SHALL return current state safely.
        
        Feature: icann-downloader, Property 6: Job Concurrency Prevention
        Validates: Requirements 5.3
        """
        service = self._create_mock_service()
        
        # Set some state
        service._job_status.state = "running"
        service._job_status.current_tld = "com"
        service._job_status.progress_percent = 50
        
        # Get status
        status = service.get_current_status()
        
        # Verify it reflects current state
        assert status.state == "running"
        assert status.current_tld == "com"
        assert status.progress_percent == 50
    
    def test_thread_safety_of_status_check(self):
        """Status checks SHALL be thread-safe.
        
        Feature: icann-downloader, Property 6: Job Concurrency Prevention
        Validates: Requirements 5.3
        """
        service = self._create_mock_service()
        results = []
        
        def check_and_record():
            for _ in range(100):
                results.append(service.is_running())
                time.sleep(0.001)
        
        def toggle_state():
            for _ in range(50):
                with service._lock:
                    service._job_status.state = "running"
                time.sleep(0.001)
                with service._lock:
                    service._job_status.state = "idle"
                time.sleep(0.001)
        
        # Run concurrent threads
        t1 = threading.Thread(target=check_and_record)
        t2 = threading.Thread(target=toggle_state)
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        # Should complete without errors
        assert len(results) == 100, "All checks should complete"
        # All results should be boolean
        assert all(isinstance(r, bool) for r in results)
