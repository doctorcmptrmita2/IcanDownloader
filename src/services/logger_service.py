"""Logger Service for logging and real-time notifications."""
import logging
from datetime import datetime
from typing import Optional, List, Any
from dataclasses import dataclass, field

from src.models import DownloadResult


logger = logging.getLogger(__name__)


@dataclass
class LogEntry:
    """A single log entry."""
    timestamp: datetime
    level: str
    message: str
    operation_type: Optional[str] = None
    tld: Optional[str] = None
    duration: Optional[int] = None
    status: Optional[str] = None
    file_size: Optional[int] = None
    records_count: Optional[int] = None
    error_message: Optional[str] = None
    context: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level,
            "message": self.message,
            "operation_type": self.operation_type,
            "tld": self.tld,
            "duration": self.duration,
            "status": self.status,
            "file_size": self.file_size,
            "records_count": self.records_count,
            "error_message": self.error_message,
            "context": self.context,
        }


class LoggerService:
    """Service for logging and real-time notifications."""
    
    def __init__(self, socketio: Optional[Any] = None, max_entries: int = 100):
        """Initialize logger service.
        
        Args:
            socketio: Flask-SocketIO instance for real-time updates
            max_entries: Maximum number of log entries to keep in memory
        """
        self.socketio = socketio
        self.max_entries = max_entries
        self._entries: List[LogEntry] = []
    
    def log(
        self, 
        level: str, 
        message: str, 
        operation_type: Optional[str] = None,
        tld: Optional[str] = None,
        duration: Optional[int] = None,
        status: Optional[str] = None,
        file_size: Optional[int] = None,
        records_count: Optional[int] = None,
        error_message: Optional[str] = None,
        context: Optional[dict] = None
    ) -> LogEntry:
        """Log message and emit to connected clients.
        
        Args:
            level: Log level (INFO, WARNING, ERROR, DEBUG)
            message: Log message
            operation_type: Type of operation (download, parse, auth, etc.)
            tld: TLD being processed
            duration: Duration in seconds
            status: Status (success, failed, in_progress)
            file_size: File size in bytes
            records_count: Number of records processed
            error_message: Error message if applicable
            context: Additional context information
            
        Returns:
            Created LogEntry
        """
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level.upper(),
            message=message,
            operation_type=operation_type,
            tld=tld,
            duration=duration,
            status=status,
            file_size=file_size,
            records_count=records_count,
            error_message=error_message,
            context=context or {},
        )
        
        # Add to in-memory list
        self._entries.append(entry)
        
        # Trim if exceeds max
        if len(self._entries) > self.max_entries:
            self._entries = self._entries[-self.max_entries:]
        
        # Log to Python logger
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(f"[{operation_type or 'SYSTEM'}] {message}")
        
        # Emit to connected clients
        self._emit_log(entry)
        
        return entry
    
    def _emit_log(self, entry: LogEntry) -> None:
        """Emit log entry to connected WebSocket clients."""
        if self.socketio:
            try:
                self.socketio.emit('log', entry.to_dict(), namespace='/')
            except Exception as e:
                logger.warning(f"Failed to emit log: {e}")
    
    def log_download_start(self, tld: str) -> LogEntry:
        """Log download start event.
        
        Args:
            tld: TLD being downloaded
            
        Returns:
            Created LogEntry
        """
        return self.log(
            level="INFO",
            message=f"⬇️ [{tld}] İndirme başladı",
            operation_type="download",
            tld=tld,
            status="in_progress",
        )
    
    def log_download_complete(self, tld: str, result: DownloadResult) -> LogEntry:
        """Log download completion with stats.
        
        Args:
            tld: TLD that was downloaded
            result: Download result with stats
            
        Returns:
            Created LogEntry
        """
        if result.is_success:
            # Calculate speed in Mbps
            size_mb = result.file_size / (1024 * 1024)
            duration = max(result.download_duration, 1)
            speed_mbps = (result.file_size * 8) / (duration * 1000000)
            
            return self.log(
                level="INFO",
                message=f"✅ [{tld}] İndirme tamamlandı: {size_mb:.1f} MB | {duration}s | {speed_mbps:.1f} Mbps",
                operation_type="download",
                tld=tld,
                duration=result.download_duration,
                status="success",
                file_size=result.file_size,
            )
        else:
            return self.log(
                level="ERROR",
                message=f"❌ [{tld}] İndirme başarısız: {result.error_message}",
                operation_type="download",
                tld=tld,
                duration=result.download_duration,
                status="failed",
                error_message=result.error_message,
            )
    
    def log_parse_start(self, tld: str) -> LogEntry:
        """Log parse start event.
        
        Args:
            tld: TLD being parsed
            
        Returns:
            Created LogEntry
        """
        return self.log(
            level="INFO",
            message=f"📄 [{tld}] Parse başladı",
            operation_type="parse",
            tld=tld,
            status="in_progress",
        )
    
    def log_parse_progress(self, tld: str, records_processed: int) -> LogEntry:
        """Log parsing progress.
        
        Args:
            tld: TLD being parsed
            records_processed: Number of records processed so far
            
        Returns:
            Created LogEntry
        """
        return self.log(
            level="DEBUG",
            message=f"🔄 [{tld}] Parse devam ediyor: {records_processed:,} kayıt",
            operation_type="parse",
            tld=tld,
            status="in_progress",
            records_count=records_processed,
        )
    
    def log_parse_complete(
        self, 
        tld: str, 
        records_count: int, 
        duration: int,
        error_message: Optional[str] = None
    ) -> LogEntry:
        """Log parse completion.
        
        Args:
            tld: TLD that was parsed
            records_count: Total records parsed
            duration: Parse duration in seconds
            error_message: Error message if parsing failed
            
        Returns:
            Created LogEntry
        """
        if error_message:
            return self.log(
                level="ERROR",
                message=f"❌ [{tld}] Parse başarısız: {error_message}",
                operation_type="parse",
                tld=tld,
                duration=duration,
                status="failed",
                records_count=records_count,
                error_message=error_message,
            )
        else:
            rate = records_count / max(duration, 1)
            return self.log(
                level="INFO",
                message=f"✅ [{tld}] Parse tamamlandı: {records_count:,} kayıt | {duration}s | {rate:,.0f} kayıt/s",
                operation_type="parse",
                tld=tld,
                duration=duration,
                status="success",
                records_count=records_count,
            )
    
    def log_error(
        self, 
        message: str, 
        error: Optional[Exception] = None,
        operation_type: Optional[str] = None,
        tld: Optional[str] = None,
        context: Optional[dict] = None
    ) -> LogEntry:
        """Log error with stack trace.
        
        Args:
            message: Error message
            error: Exception object
            operation_type: Type of operation
            tld: TLD being processed
            context: Additional context
            
        Returns:
            Created LogEntry
        """
        error_msg = str(error) if error else None
        ctx = context or {}
        
        if error:
            import traceback
            ctx["stack_trace"] = traceback.format_exc()
        
        return self.log(
            level="ERROR",
            message=message,
            operation_type=operation_type,
            tld=tld,
            status="failed",
            error_message=error_msg,
            context=ctx,
        )
    
    def get_recent_logs(self, limit: int = 100) -> List[LogEntry]:
        """Get recent log entries.
        
        Args:
            limit: Maximum number of entries to return
            
        Returns:
            List of recent LogEntry objects
        """
        return self._entries[-limit:]
    
    def get_logs_as_dicts(self, limit: int = 100) -> List[dict]:
        """Get recent logs as dictionaries.
        
        Args:
            limit: Maximum number of entries to return
            
        Returns:
            List of log entry dictionaries
        """
        return [entry.to_dict() for entry in self.get_recent_logs(limit)]
    
    def clear_logs(self) -> None:
        """Clear all log entries from memory."""
        self._entries.clear()
