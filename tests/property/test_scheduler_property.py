"""Property tests for scheduler toggle persistence and initialization.

Property 7: Scheduler Toggle Persistence
For any change to the auto-download setting, the new value SHALL be persisted 
to the database and survive application restarts.

Property 12: Scheduler Initialization
For any system startup with auto-download enabled, the scheduler SHALL have 
a job scheduled for 04:00 daily.

Validates: Requirements 6.4, 8.1
"""
from unittest.mock import MagicMock, call
from hypothesis import given, strategies as st, settings

from src.services.scheduler_service import SchedulerService


class TestSchedulerTogglePersistence:
    """Property 7: Scheduler Toggle Persistence"""
    
    def test_enable_persists_to_database(self):
        """Enabling auto-download SHALL persist 'true' to database.
        
        Feature: icann-downloader, Property 7: Scheduler Toggle Persistence
        Validates: Requirements 6.4
        """
        mock_repo = MagicMock()
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            repository=mock_repo,
        )
        
        service.enable_auto_download()
        
        # Verify setting was persisted
        mock_repo.set_setting.assert_called_with(
            SchedulerService.AUTO_DOWNLOAD_SETTING_KEY,
            "true"
        )
    
    def test_disable_persists_to_database(self):
        """Disabling auto-download SHALL persist 'false' to database.
        
        Feature: icann-downloader, Property 7: Scheduler Toggle Persistence
        Validates: Requirements 6.4
        """
        mock_repo = MagicMock()
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            repository=mock_repo,
        )
        
        service.disable_auto_download()
        
        # Verify setting was persisted
        mock_repo.set_setting.assert_called_with(
            SchedulerService.AUTO_DOWNLOAD_SETTING_KEY,
            "false"
        )
    
    def test_start_loads_setting_from_database(self):
        """On start, scheduler SHALL load setting from database.
        
        Feature: icann-downloader, Property 7: Scheduler Toggle Persistence
        Validates: Requirements 6.4
        """
        mock_repo = MagicMock()
        mock_repo.get_setting.return_value = "true"
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            repository=mock_repo,
        )
        
        service.start()
        
        # Verify setting was loaded
        mock_repo.get_setting.assert_called_with(
            SchedulerService.AUTO_DOWNLOAD_SETTING_KEY
        )
        
        # Verify enabled state
        assert service.is_enabled() is True
        
        service.stop()
    
    def test_start_with_disabled_setting(self):
        """On start with disabled setting, scheduler SHALL not schedule job.
        
        Feature: icann-downloader, Property 7: Scheduler Toggle Persistence
        Validates: Requirements 6.4
        """
        mock_repo = MagicMock()
        mock_repo.get_setting.return_value = "false"
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            repository=mock_repo,
        )
        
        service.start()
        
        # Verify disabled state
        assert service.is_enabled() is False
        
        service.stop()
    
    @given(enabled=st.booleans())
    @settings(max_examples=10)
    def test_toggle_state_matches_is_enabled(self, enabled):
        """is_enabled() SHALL return current toggle state.
        
        Feature: icann-downloader, Property 7: Scheduler Toggle Persistence
        Validates: Requirements 6.4
        """
        mock_repo = MagicMock()
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            repository=mock_repo,
        )
        
        if enabled:
            service.enable_auto_download()
        else:
            service.disable_auto_download()
        
        assert service.is_enabled() == enabled


class TestSchedulerInitialization:
    """Property 12: Scheduler Initialization"""
    
    @given(
        hour=st.integers(min_value=0, max_value=23),
        minute=st.integers(min_value=0, max_value=59),
    )
    @settings(max_examples=50)
    def test_scheduler_uses_configured_time(self, hour, minute):
        """Scheduler SHALL use configured cron time.
        
        Feature: icann-downloader, Property 12: Scheduler Initialization
        Validates: Requirements 8.1
        """
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            cron_hour=hour,
            cron_minute=minute,
        )
        
        assert service.cron_hour == hour
        assert service.cron_minute == minute
    
    def test_default_schedule_is_0400(self):
        """Default schedule SHALL be 04:00.
        
        Feature: icann-downloader, Property 12: Scheduler Initialization
        Validates: Requirements 8.1
        """
        mock_callback = MagicMock()
        
        service = SchedulerService(download_callback=mock_callback)
        
        assert service.cron_hour == 4
        assert service.cron_minute == 0
    
    def test_job_scheduled_when_enabled(self):
        """When enabled, scheduler SHALL have a job scheduled.
        
        Feature: icann-downloader, Property 12: Scheduler Initialization
        Validates: Requirements 8.1
        """
        mock_callback = MagicMock()
        
        service = SchedulerService(download_callback=mock_callback)
        service.start()
        service.enable_auto_download()
        
        # Verify job is scheduled
        assert service.has_scheduled_job() is True
        
        service.stop()
    
    def test_no_job_when_disabled(self):
        """When disabled, scheduler SHALL NOT have a job scheduled.
        
        Feature: icann-downloader, Property 12: Scheduler Initialization
        Validates: Requirements 8.1
        """
        mock_callback = MagicMock()
        
        service = SchedulerService(download_callback=mock_callback)
        service.start()
        service.disable_auto_download()
        
        # Verify no job is scheduled
        assert service.has_scheduled_job() is False
        
        service.stop()
    
    def test_get_status_returns_schedule_info(self):
        """get_status() SHALL return schedule information.
        
        Feature: icann-downloader, Property 12: Scheduler Initialization
        Validates: Requirements 8.1
        """
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            cron_hour=4,
            cron_minute=0,
        )
        
        status = service.get_status()
        
        assert "enabled" in status
        assert "running" in status
        assert "next_run_time" in status
        assert "cron_schedule" in status
        assert status["cron_schedule"] == "04:00"
    
    def test_start_with_enabled_setting_schedules_job(self):
        """Starting with enabled setting SHALL schedule job.
        
        Feature: icann-downloader, Property 12: Scheduler Initialization
        Validates: Requirements 8.1
        """
        mock_repo = MagicMock()
        mock_repo.get_setting.return_value = "true"
        mock_callback = MagicMock()
        
        service = SchedulerService(
            download_callback=mock_callback,
            repository=mock_repo,
        )
        
        service.start()
        
        # Verify job is scheduled
        assert service.is_enabled() is True
        assert service.has_scheduled_job() is True
        
        service.stop()
