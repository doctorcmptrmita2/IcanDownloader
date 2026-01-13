"""Property tests for Status Response Completeness.

Property 9: Status Response Completeness
For any status API response, the response SHALL include: last_download_time,
total_domains_processed, active_jobs, and progress_percent (if job running).

Validates: Requirements 4.2, 4.3
"""
import pytest
from datetime import datetime
from hypothesis import given, strategies as st, settings, HealthCheck
from unittest.mock import MagicMock, patch

from src.api.app import create_app
from src.models import JobStatus, DownloadLog


class TestStatusResponseCompleteness:
    """Property tests for status response completeness."""
    
    @pytest.fixture
    def app_with_mocks(self):
        """Create app with mocked services."""
        mock_download_service = MagicMock()
        mock_scheduler_service = MagicMock()
        mock_logger_service = MagicMock()
        mock_repository = MagicMock()
        
        app, socketio = create_app(
            download_service=mock_download_service,
            scheduler_service=mock_scheduler_service,
            logger_service=mock_logger_service,
            repository=mock_repository,
        )
        
        return app, mock_download_service, mock_scheduler_service, mock_repository
    
    @given(
        state=st.sampled_from(["idle", "running"]),
        progress=st.integers(min_value=0, max_value=100),
        total_tlds=st.integers(min_value=0, max_value=1000),
        completed_tlds=st.integers(min_value=0, max_value=1000),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_status_response_has_required_fields(
        self, state, progress, total_tlds, completed_tlds
    ):
        """Status response must include all required fields."""
        mock_download_service = MagicMock()
        mock_scheduler_service = MagicMock()
        mock_logger_service = MagicMock()
        mock_repository = MagicMock()
        
        # Setup job status
        job_status = JobStatus(
            state=state,
            current_tld="com" if state == "running" else None,
            progress_percent=progress,
            total_tlds=total_tlds,
            completed_tlds=min(completed_tlds, total_tlds),
            started_at=datetime.now() if state == "running" else None,
        )
        mock_download_service.get_current_status.return_value = job_status
        
        # Setup scheduler status
        mock_scheduler_service.get_status.return_value = {
            "enabled": True,
            "next_run_time": datetime.now().isoformat(),
        }
        
        # Setup repository
        mock_repository.get_recent_logs.return_value = []
        
        app, socketio = create_app(
            download_service=mock_download_service,
            scheduler_service=mock_scheduler_service,
            logger_service=mock_logger_service,
            repository=mock_repository,
        )
        
        with app.test_client() as client:
            response = client.get('/api/status')
            assert response.status_code == 200
            
            data = response.get_json()
            
            # Required fields must be present
            assert "timestamp" in data
            assert "job" in data
            assert "total_domains_processed" in data
            
            # Job status fields
            if data["job"]:
                assert "state" in data["job"]
                assert "progress_percent" in data["job"]
                
                # If running, must have active_jobs
                if data["job"]["state"] == "running":
                    assert "active_jobs" in data
                    assert data["active_jobs"] >= 1
    
    @given(
        records_count=st.integers(min_value=0, max_value=1000000),
        status=st.sampled_from(["success", "failed", "partial"]),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_status_includes_last_download_info(self, records_count, status):
        """Status response must include last download information when available."""
        mock_download_service = MagicMock()
        mock_scheduler_service = MagicMock()
        mock_logger_service = MagicMock()
        mock_repository = MagicMock()
        
        # Setup job status (idle)
        job_status = JobStatus()
        mock_download_service.get_current_status.return_value = job_status
        
        # Setup scheduler
        mock_scheduler_service.get_status.return_value = {"enabled": False}
        
        # Setup last download log
        last_log = DownloadLog(
            id=1,
            tld="com",
            file_size=1000000,
            records_count=records_count,
            download_duration=60,
            parse_duration=120,
            status=status,
            error_message=None if status == "success" else "Error",
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )
        mock_repository.get_recent_logs.return_value = [last_log]
        
        app, socketio = create_app(
            download_service=mock_download_service,
            scheduler_service=mock_scheduler_service,
            logger_service=mock_logger_service,
            repository=mock_repository,
        )
        
        with app.test_client() as client:
            response = client.get('/api/status')
            assert response.status_code == 200
            
            data = response.get_json()
            
            # Last download info must be present
            assert "last_download" in data
            if data["last_download"]:
                assert "tld" in data["last_download"]
                assert "status" in data["last_download"]
                assert "records_count" in data["last_download"]
            
            # last_download_time must be present
            assert "last_download_time" in data
    
    @given(
        progress=st.integers(min_value=0, max_value=100),
        total=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_running_job_has_progress_percent(self, progress, total):
        """Running job status must include progress_percent."""
        mock_download_service = MagicMock()
        mock_scheduler_service = MagicMock()
        mock_logger_service = MagicMock()
        mock_repository = MagicMock()
        
        completed = min(progress, total)
        
        # Setup running job
        job_status = JobStatus(
            state="running",
            current_tld="net",
            progress_percent=progress,
            total_tlds=total,
            completed_tlds=completed,
            started_at=datetime.now(),
        )
        mock_download_service.get_current_status.return_value = job_status
        
        mock_scheduler_service.get_status.return_value = {"enabled": True}
        mock_repository.get_recent_logs.return_value = []
        
        app, socketio = create_app(
            download_service=mock_download_service,
            scheduler_service=mock_scheduler_service,
            logger_service=mock_logger_service,
            repository=mock_repository,
        )
        
        with app.test_client() as client:
            response = client.get('/api/status')
            assert response.status_code == 200
            
            data = response.get_json()
            
            # Progress percent must be present for running jobs
            assert data["job"]["progress_percent"] is not None
            assert 0 <= data["job"]["progress_percent"] <= 100
            
            # Active jobs must be 1 for running state
            assert data["active_jobs"] == 1
    
    def test_status_without_services_returns_defaults(self):
        """Status endpoint works even without services."""
        app, socketio = create_app()
        
        with app.test_client() as client:
            response = client.get('/api/status')
            assert response.status_code == 200
            
            data = response.get_json()
            
            # Basic fields must still be present
            assert "timestamp" in data
            assert "total_domains_processed" in data
            assert data["total_domains_processed"] == 0


class TestStatusResponseFormat:
    """Tests for status response format and structure."""
    
    def test_status_timestamp_is_iso_format(self):
        """Timestamp must be in ISO format."""
        app, socketio = create_app()
        
        with app.test_client() as client:
            response = client.get('/api/status')
            data = response.get_json()
            
            # Should be parseable as ISO datetime
            timestamp = data["timestamp"]
            parsed = datetime.fromisoformat(timestamp)
            assert parsed is not None
    
    def test_health_endpoint_returns_healthy(self):
        """Health endpoint must return healthy status."""
        app, socketio = create_app()
        
        with app.test_client() as client:
            response = client.get('/health')
            assert response.status_code == 200
            
            data = response.get_json()
            assert data["status"] == "healthy"
            assert "timestamp" in data
