"""Job Status data model."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class JobStatus:
    """Current status of a download job.
    
    Attributes:
        state: Current state (idle, running)
        current_tld: TLD currently being processed
        progress_percent: Progress percentage (0-100)
        total_tlds: Total number of TLDs to process
        completed_tlds: Number of TLDs completed
        started_at: When the job started
    """
    state: str = "idle"
    current_tld: Optional[str] = None
    progress_percent: int = 0
    total_tlds: int = 0
    completed_tlds: int = 0
    started_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "state": self.state,
            "current_tld": self.current_tld,
            "progress_percent": self.progress_percent,
            "total_tlds": self.total_tlds,
            "completed_tlds": self.completed_tlds,
            "started_at": self.started_at.isoformat() if self.started_at else None,
        }
    
    @property
    def is_running(self) -> bool:
        """Check if job is currently running."""
        return self.state == "running"
    
    @property
    def is_idle(self) -> bool:
        """Check if job is idle."""
        return self.state == "idle"
    
    def update_progress(self, completed: int, total: int, current_tld: str) -> None:
        """Update job progress.
        
        Args:
            completed: Number of completed TLDs
            total: Total number of TLDs
            current_tld: Currently processing TLD
        """
        self.completed_tlds = completed
        self.total_tlds = total
        self.current_tld = current_tld
        self.progress_percent = int((completed / total) * 100) if total > 0 else 0
    
    def start(self, total_tlds: int) -> None:
        """Start the job.
        
        Args:
            total_tlds: Total number of TLDs to process
        """
        self.state = "running"
        self.total_tlds = total_tlds
        self.completed_tlds = 0
        self.progress_percent = 0
        self.started_at = datetime.now()
    
    def complete(self) -> None:
        """Mark job as complete."""
        self.state = "idle"
        self.current_tld = None
        self.progress_percent = 100
