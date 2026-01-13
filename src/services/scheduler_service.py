"""Scheduler Service for managing scheduled jobs."""
import logging
from datetime import datetime
from typing import Optional, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError

from src.services.db_repository import ClickHouseRepository


logger = logging.getLogger(__name__)


class SchedulerService:
    """Service for managing scheduled jobs."""
    
    AUTO_DOWNLOAD_SETTING_KEY = "auto_download_enabled"
    JOB_ID = "daily_download"
    
    def __init__(
        self,
        download_callback: Callable,
        repository: Optional[ClickHouseRepository] = None,
        cron_hour: int = 4,
        cron_minute: int = 0,
    ):
        """Initialize APScheduler.
        
        Args:
            download_callback: Function to call for downloads
            repository: ClickHouse repository for settings persistence
            cron_hour: Hour for daily job (default 4 = 04:00)
            cron_minute: Minute for daily job (default 0)
        """
        self.download_callback = download_callback
        self.repository = repository
        self.cron_hour = cron_hour
        self.cron_minute = cron_minute
        
        self._scheduler = BackgroundScheduler()
        self._enabled = False
    
    def start(self) -> None:
        """Start scheduler if auto-download is enabled."""
        # Load setting from database
        if self.repository:
            try:
                setting = self.repository.get_setting(self.AUTO_DOWNLOAD_SETTING_KEY)
                self._enabled = setting == "true"
            except Exception as e:
                logger.warning(f"Failed to load auto-download setting: {e}")
                self._enabled = False
        
        # Start the scheduler
        if not self._scheduler.running:
            self._scheduler.start()
            logger.info("Scheduler started")
        
        # Add job if enabled
        if self._enabled:
            self._add_job()
            logger.info(f"Auto-download enabled, scheduled for {self.cron_hour:02d}:{self.cron_minute:02d}")
    
    def stop(self) -> None:
        """Stop scheduler."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")
    
    def enable_auto_download(self) -> None:
        """Enable automatic daily downloads at configured time."""
        self._enabled = True
        self._persist_setting(True)
        self._add_job()
        logger.info(f"Auto-download enabled, scheduled for {self.cron_hour:02d}:{self.cron_minute:02d}")
    
    def disable_auto_download(self) -> None:
        """Disable automatic downloads."""
        self._enabled = False
        self._persist_setting(False)
        self._remove_job()
        logger.info("Auto-download disabled")
    
    def _add_job(self) -> None:
        """Add the daily download job."""
        # Remove existing job if any
        self._remove_job()
        
        # Add new job
        trigger = CronTrigger(hour=self.cron_hour, minute=self.cron_minute)
        self._scheduler.add_job(
            self._run_download,
            trigger=trigger,
            id=self.JOB_ID,
            name="Daily ICANN Zone Download",
            replace_existing=True,
        )
    
    def _remove_job(self) -> None:
        """Remove the daily download job."""
        try:
            self._scheduler.remove_job(self.JOB_ID)
        except JobLookupError:
            pass  # Job doesn't exist
    
    def _run_download(self) -> None:
        """Execute the download callback with error handling."""
        logger.info("Scheduled download starting")
        try:
            self.download_callback()
            logger.info("Scheduled download completed")
        except Exception as e:
            logger.error(f"Scheduled download failed: {e}")
            # Schedule retry after 1 hour
            self._schedule_retry()
    
    def _schedule_retry(self) -> None:
        """Schedule a retry after 1 hour."""
        from datetime import timedelta
        
        retry_time = datetime.now() + timedelta(hours=1)
        self._scheduler.add_job(
            self._run_download,
            trigger='date',
            run_date=retry_time,
            id=f"{self.JOB_ID}_retry",
            name="Retry ICANN Zone Download",
            replace_existing=True,
        )
        logger.info(f"Retry scheduled for {retry_time}")
    
    def _persist_setting(self, enabled: bool) -> None:
        """Persist auto-download setting to database.
        
        Args:
            enabled: Whether auto-download is enabled
        """
        if self.repository:
            try:
                self.repository.set_setting(
                    self.AUTO_DOWNLOAD_SETTING_KEY,
                    "true" if enabled else "false"
                )
            except Exception as e:
                logger.warning(f"Failed to persist auto-download setting: {e}")
    
    def is_enabled(self) -> bool:
        """Check if auto-download is enabled.
        
        Returns:
            True if auto-download is enabled
        """
        return self._enabled
    
    def get_next_run_time(self) -> Optional[datetime]:
        """Get next scheduled run time.
        
        Returns:
            Next run time or None if not scheduled
        """
        if not self._enabled:
            return None
        
        try:
            job = self._scheduler.get_job(self.JOB_ID)
            if job and job.next_run_time:
                return job.next_run_time
        except Exception:
            pass
        
        return None
    
    def get_status(self) -> dict:
        """Get scheduler status.
        
        Returns:
            Dictionary with scheduler status
        """
        next_run = self.get_next_run_time()
        return {
            "enabled": self._enabled,
            "running": self._scheduler.running if self._scheduler else False,
            "next_run_time": next_run.isoformat() if next_run else None,
            "cron_schedule": f"{self.cron_hour:02d}:{self.cron_minute:02d}",
        }
    
    def has_scheduled_job(self) -> bool:
        """Check if a job is scheduled.
        
        Returns:
            True if job is scheduled
        """
        try:
            job = self._scheduler.get_job(self.JOB_ID)
            return job is not None
        except Exception:
            return False
