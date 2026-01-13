"""Download Result data model."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class DownloadResult:
    """Result of a zone file download operation.
    
    Attributes:
        tld: Top-level domain that was downloaded
        file_path: Path to the downloaded file
        file_size: Size of the downloaded file in bytes
        download_duration: Time taken to download in seconds
        records_count: Number of records parsed from the file
        parse_duration: Time taken to parse in seconds
        status: Status of the operation (success, failed, partial)
        error_message: Error message if operation failed
    """
    tld: str
    file_path: str
    file_size: int
    download_duration: int
    records_count: int = 0
    parse_duration: int = 0
    status: str = "success"
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "tld": self.tld,
            "file_path": self.file_path,
            "file_size": self.file_size,
            "download_duration": self.download_duration,
            "records_count": self.records_count,
            "parse_duration": self.parse_duration,
            "status": self.status,
            "error_message": self.error_message,
        }
    
    @property
    def is_success(self) -> bool:
        """Check if download was successful."""
        return self.status == "success"
    
    @property
    def is_failed(self) -> bool:
        """Check if download failed."""
        return self.status == "failed"
