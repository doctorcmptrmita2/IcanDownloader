"""Download Service for coordinating download and parse operations."""
import os
import time
import threading
import logging
from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass

from src.models import JobStatus, DownloadResult, DownloadLog, ZoneRecord
from src.services.czds_client import CZDSClient
from src.services.zone_parser import ZoneParser
from src.services.db_repository import ClickHouseRepository
from src.services.logger_service import LoggerService


logger = logging.getLogger(__name__)


@dataclass
class DownloadSummary:
    """Summary of a full download cycle."""
    total_tlds: int
    successful_tlds: int
    failed_tlds: int
    total_records: int
    total_duration: int
    started_at: datetime
    completed_at: datetime
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "total_tlds": self.total_tlds,
            "successful_tlds": self.successful_tlds,
            "failed_tlds": self.failed_tlds,
            "total_records": self.total_records,
            "total_duration": self.total_duration,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
        }


class DownloadService:
    """Service for coordinating download and parse operations."""
    
    def __init__(
        self,
        czds_client: CZDSClient,
        parser_factory,  # Callable that creates ZoneParser for a TLD
        repository: ClickHouseRepository,
        logger_service: LoggerService,
        temp_dir: str = "/app/temp",
        batch_size: int = 10000,
    ):
        """Initialize with dependencies.
        
        Args:
            czds_client: CZDS API client
            parser_factory: Factory function to create ZoneParser instances
            repository: ClickHouse repository
            logger_service: Logger service
            temp_dir: Directory for temporary files
            batch_size: Batch size for database inserts
        """
        self.czds_client = czds_client
        self.parser_factory = parser_factory
        self.repository = repository
        self.logger_service = logger_service
        self.temp_dir = temp_dir
        self.batch_size = batch_size
        
        self._job_status = JobStatus()
        self._lock = threading.Lock()
    
    def run_full_download(self) -> Optional[DownloadSummary]:
        """Execute full download cycle for all approved TLDs.
        
        Returns:
            Summary with total files, records, duration, or None if job already running
        """
        # Check if already running
        with self._lock:
            if self._job_status.is_running:
                self.logger_service.log(
                    "WARNING",
                    "Download job already in progress",
                    operation_type="download",
                )
                return None
        
        start_time = datetime.now()
        
        try:
            # Authenticate
            self.logger_service.log("INFO", "Authenticating with CZDS API", operation_type="auth")
            self.czds_client.authenticate()
            
            # Get approved TLDs
            self.logger_service.log("INFO", "Fetching approved TLDs", operation_type="download")
            tlds = self.czds_client.get_approved_tlds()
            
            if not tlds:
                self.logger_service.log("WARNING", "No approved TLDs found", operation_type="download")
                return DownloadSummary(
                    total_tlds=0,
                    successful_tlds=0,
                    failed_tlds=0,
                    total_records=0,
                    total_duration=0,
                    started_at=start_time,
                    completed_at=datetime.now(),
                )
            
            # Start job
            with self._lock:
                self._job_status.start(len(tlds))
            
            self.logger_service.log(
                "INFO",
                f"Starting download for {len(tlds)} TLDs",
                operation_type="download",
            )
            
            # Process each TLD sequentially
            successful = 0
            failed = 0
            total_records = 0
            
            for i, tld in enumerate(tlds):
                result = self.download_single_tld(tld)
                
                if result.is_success:
                    successful += 1
                    total_records += result.records_count
                else:
                    failed += 1
                
                # Update progress
                with self._lock:
                    self._job_status.update_progress(i + 1, len(tlds), tld)
            
            # Complete job
            with self._lock:
                self._job_status.complete()
            
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds())
            
            summary = DownloadSummary(
                total_tlds=len(tlds),
                successful_tlds=successful,
                failed_tlds=failed,
                total_records=total_records,
                total_duration=duration,
                started_at=start_time,
                completed_at=end_time,
            )
            
            self.logger_service.log(
                "INFO",
                f"Download complete: {successful}/{len(tlds)} TLDs, {total_records} records in {duration}s",
                operation_type="download",
                status="success",
                duration=duration,
            )
            
            return summary
            
        except Exception as e:
            with self._lock:
                self._job_status.complete()
            
            self.logger_service.log_error(
                f"Download failed: {e}",
                error=e,
                operation_type="download",
            )
            raise
    
    def download_single_tld(self, tld: str) -> DownloadResult:
        """Download and process single TLD.
        
        Args:
            tld: TLD to download
            
        Returns:
            DownloadResult with stats
        """
        self.logger_service.log_download_start(tld)
        
        try:
            # Ensure temp directory exists
            os.makedirs(self.temp_dir, exist_ok=True)
            
            # Download zone file
            result = self.czds_client.download_zone_file(tld, self.temp_dir)
            
            if not result.is_success:
                self.logger_service.log_download_complete(tld, result)
                self._log_to_db(tld, result)
                return result
            
            self.logger_service.log_download_complete(tld, result)
            
            # Parse zone file
            self.logger_service.log_parse_start(tld)
            parse_start = time.time()
            
            parser = self.parser_factory(tld)
            records_batch: List[ZoneRecord] = []
            total_records = 0
            
            for record in parser.parse_zone_file(result.file_path):
                records_batch.append(record)
                
                if len(records_batch) >= self.batch_size:
                    self.repository.insert_zone_records(records_batch, self.batch_size)
                    total_records += len(records_batch)
                    records_batch = []
                    
                    # Log progress every 100k records
                    if total_records % 100000 == 0:
                        self.logger_service.log_parse_progress(tld, total_records)
            
            # Insert remaining records
            if records_batch:
                self.repository.insert_zone_records(records_batch, self.batch_size)
                total_records += len(records_batch)
            
            parse_duration = int(time.time() - parse_start)
            
            # Update result with parse info
            result.records_count = total_records
            result.parse_duration = parse_duration
            
            self.logger_service.log_parse_complete(tld, total_records, parse_duration)
            
            # Clean up downloaded file
            try:
                os.remove(result.file_path)
            except OSError:
                pass
            
            # Log to database
            self._log_to_db(tld, result)
            
            return result
            
        except Exception as e:
            error_result = DownloadResult(
                tld=tld,
                file_path="",
                file_size=0,
                download_duration=0,
                status="failed",
                error_message=str(e),
            )
            
            self.logger_service.log_error(
                f"Failed to process {tld}",
                error=e,
                operation_type="download",
                tld=tld,
            )
            
            self._log_to_db(tld, error_result)
            
            return error_result
    
    def _log_to_db(self, tld: str, result: DownloadResult) -> None:
        """Log download result to database.
        
        Args:
            tld: TLD that was processed
            result: Download result
        """
        try:
            log = DownloadLog(
                tld=tld,
                file_size=result.file_size,
                records_count=result.records_count,
                download_duration=result.download_duration,
                parse_duration=result.parse_duration,
                status=result.status,
                error_message=result.error_message,
                started_at=datetime.now(),
                completed_at=datetime.now(),
            )
            self.repository.log_download(log)
        except Exception as e:
            logger.warning(f"Failed to log download to database: {e}")
    
    def get_current_status(self) -> JobStatus:
        """Get current job status.
        
        Returns:
            Current JobStatus
        """
        with self._lock:
            return JobStatus(
                state=self._job_status.state,
                current_tld=self._job_status.current_tld,
                progress_percent=self._job_status.progress_percent,
                total_tlds=self._job_status.total_tlds,
                completed_tlds=self._job_status.completed_tlds,
                started_at=self._job_status.started_at,
            )
    
    def is_running(self) -> bool:
        """Check if a job is currently running.
        
        Returns:
            True if job is running
        """
        with self._lock:
            return self._job_status.is_running
