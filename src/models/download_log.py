"""Download Log data model."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class DownloadLog:
    """Log entry for a download operation.
    
    Attributes:
        id: Unique identifier
        tld: Top-level domain
        file_size: Size of the downloaded file in bytes
        records_count: Number of records processed
        download_duration: Time taken to download in seconds
        parse_duration: Time taken to parse in seconds
        status: Status of the operation (success, failed, partial)
        error_message: Error message if operation failed
        started_at: When the operation started
        completed_at: When the operation completed
    """
    tld: str
    file_size: int
    records_count: int
    download_duration: int
    parse_duration: int
    status: str
    started_at: datetime
    completed_at: datetime
    id: Optional[int] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        return {
            "tld": self.tld,
            "file_size": self.file_size,
            "records_count": self.records_count,
            "download_duration": self.download_duration,
            "parse_duration": self.parse_duration,
            "status": self.status,
            "error_message": self.error_message,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "DownloadLog":
        """Create DownloadLog from dictionary."""
        return cls(
            id=data.get("id"),
            tld=data["tld"],
            file_size=data["file_size"],
            records_count=data["records_count"],
            download_duration=data["download_duration"],
            parse_duration=data["parse_duration"],
            status=data["status"],
            error_message=data.get("error_message"),
            started_at=data["started_at"],
            completed_at=data["completed_at"],
        )
    
    @property
    def is_success(self) -> bool:
        """Check if operation was successful."""
        return self.status == "success"
