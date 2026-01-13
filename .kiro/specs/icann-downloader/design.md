# Design Document: ICANN Downloader

## Overview

ICANN Downloader, ICANN CZDS (Centralized Zone Data Service) API'sinden günlük domain zone dosyalarını indiren, parse eden ve ClickHouse veritabanına kaydeden bir Python worker uygulamasıdır. Uygulama Flask tabanlı bir web arayüzü sunar ve APScheduler ile cron job desteği sağlar.

### Teknoloji Stack

- **Backend**: Python 3.11+
- **Web Framework**: Flask + Flask-SocketIO (real-time logs için)
- **Database**: ClickHouse
- **Scheduler**: APScheduler
- **HTTP Client**: requests
- **Containerization**: Docker
- **Deployment**: Dokploy

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Container                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Flask Web Server                      │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │    │
│  │  │  Dashboard  │  │   Manual    │  │   Auto Toggle   │  │    │
│  │  │    Page     │  │  Download   │  │    Control      │  │    │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Core Services                          │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │    │
│  │  │   CZDS      │  │   Zone      │  │   ClickHouse    │  │    │
│  │  │   Client    │  │   Parser    │  │   Repository    │  │    │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Background Services                    │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │    │
│  │  │ APScheduler │  │   Logger    │  │   Job Queue     │  │    │
│  │  │  (Cron)     │  │   Service   │  │   Manager       │  │    │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      External Services                           │
│  ┌─────────────────────┐        ┌─────────────────────────┐     │
│  │   ICANN CZDS API    │        │   ClickHouse Database   │     │
│  │  czds-api.icann.org │        │     clickhouse-db       │     │
│  └─────────────────────┘        └─────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. CZDS Client (`czds_client.py`)

ICANN CZDS API ile iletişimi yöneten modül.

```python
class CZDSClient:
    """ICANN CZDS API client for authentication and zone file downloads."""
    
    def __init__(self, username: str, password: str):
        """Initialize client with ICANN credentials."""
        
    def authenticate(self) -> str:
        """
        Authenticate with CZDS API and return access token.
        Raises: AuthenticationError on failure after 3 retries.
        """
        
    def get_approved_tlds(self) -> List[str]:
        """
        Fetch list of approved TLDs for download.
        Returns: List of TLD names (e.g., ['com', 'net', 'org'])
        """
        
    def download_zone_file(self, tld: str, output_dir: str) -> DownloadResult:
        """
        Download zone file for specified TLD.
        Returns: DownloadResult with file_path, file_size, duration
        """
        
    def _refresh_token_if_needed(self) -> None:
        """Refresh access token if expired."""
```

### 2. Zone Parser (`zone_parser.py`)

Zone dosyalarını parse eden modül.

```python
class ZoneParser:
    """Parser for DNS zone files."""
    
    def parse_zone_file(self, file_path: str) -> Generator[ZoneRecord, None, None]:
        """
        Parse gzipped zone file and yield DNS records.
        Yields: ZoneRecord objects for each valid DNS record
        """
        
    def _decompress_file(self, file_path: str) -> str:
        """Decompress gzipped file to temporary location."""
        
    def _parse_line(self, line: str, tld: str) -> Optional[ZoneRecord]:
        """
        Parse single line from zone file.
        Returns: ZoneRecord or None if line is comment/invalid
        """
```

### 3. ClickHouse Repository (`db_repository.py`)

Veritabanı işlemlerini yöneten modül.

```python
class ClickHouseRepository:
    """Repository for ClickHouse database operations."""
    
    def __init__(self, host: str, password: str, database: str = 'icann'):
        """Initialize connection to ClickHouse."""
        
    def init_tables(self) -> None:
        """Create tables if they don't exist."""
        
    def insert_zone_records(self, records: List[ZoneRecord], batch_size: int = 10000) -> int:
        """
        Insert zone records in batches.
        Returns: Total number of records inserted
        """
        
    def log_download(self, log: DownloadLog) -> None:
        """Insert download log entry."""
        
    def get_recent_logs(self, limit: int = 100) -> List[DownloadLog]:
        """Fetch recent download logs."""
        
    def get_setting(self, key: str) -> Optional[str]:
        """Get system setting value."""
        
    def set_setting(self, key: str, value: str) -> None:
        """Set system setting value."""
```

### 4. Download Service (`download_service.py`)

İndirme ve parse işlemlerini koordine eden servis.

```python
class DownloadService:
    """Service for coordinating download and parse operations."""
    
    def __init__(self, czds_client: CZDSClient, parser: ZoneParser, 
                 repository: ClickHouseRepository, logger: LoggerService):
        """Initialize with dependencies."""
        
    def run_full_download(self) -> DownloadSummary:
        """
        Execute full download cycle for all approved TLDs.
        Returns: Summary with total files, records, duration
        """
        
    def download_single_tld(self, tld: str) -> DownloadResult:
        """Download and process single TLD."""
        
    def get_current_status(self) -> JobStatus:
        """Get current job status (idle, running, progress)."""
```

### 5. Logger Service (`logger_service.py`)

Loglama ve real-time bildirim servisi.

```python
class LoggerService:
    """Service for logging and real-time notifications."""
    
    def __init__(self, socketio: SocketIO, repository: ClickHouseRepository):
        """Initialize with SocketIO for real-time updates."""
        
    def log(self, level: str, message: str, context: dict = None) -> None:
        """
        Log message and emit to connected clients.
        Levels: INFO, WARNING, ERROR, DEBUG
        """
        
    def log_download_start(self, tld: str) -> None:
        """Log download start event."""
        
    def log_download_complete(self, tld: str, result: DownloadResult) -> None:
        """Log download completion with stats."""
        
    def log_parse_progress(self, tld: str, records_processed: int) -> None:
        """Log parsing progress."""
```

### 6. Scheduler Service (`scheduler_service.py`)

Cron job yönetimi.

```python
class SchedulerService:
    """Service for managing scheduled jobs."""
    
    def __init__(self, download_service: DownloadService, 
                 repository: ClickHouseRepository):
        """Initialize APScheduler."""
        
    def start(self) -> None:
        """Start scheduler if auto-download is enabled."""
        
    def stop(self) -> None:
        """Stop scheduler."""
        
    def enable_auto_download(self) -> None:
        """Enable automatic daily downloads at 04:00."""
        
    def disable_auto_download(self) -> None:
        """Disable automatic downloads."""
        
    def is_enabled(self) -> bool:
        """Check if auto-download is enabled."""
        
    def get_next_run_time(self) -> Optional[datetime]:
        """Get next scheduled run time."""
```

### 7. Flask Web Application (`app.py`)

Web arayüzü ve API endpoints.

```python
# Routes
@app.route('/')
def dashboard():
    """Render dashboard page with current status."""

@app.route('/api/download', methods=['POST'])
def trigger_download():
    """Trigger manual download."""

@app.route('/api/auto-download', methods=['POST'])
def toggle_auto_download():
    """Enable/disable automatic downloads."""

@app.route('/api/status')
def get_status():
    """Get current system status."""

@app.route('/api/logs')
def get_logs():
    """Get recent log entries."""

# SocketIO Events
@socketio.on('connect')
def handle_connect():
    """Handle client connection."""

@socketio.on('subscribe_logs')
def handle_subscribe():
    """Subscribe client to real-time logs."""
```

## Data Models

### ZoneRecord

```python
@dataclass
class ZoneRecord:
    domain_name: str      # "example.com"
    tld: str              # "com"
    record_type: str      # "NS", "A", "AAAA", etc.
    record_data: str      # Nameserver, IP, etc.
    ttl: int              # Time to live
    download_date: date   # Date of download
```

### DownloadResult

```python
@dataclass
class DownloadResult:
    tld: str
    file_path: str
    file_size: int        # bytes
    download_duration: int # seconds
    records_count: int
    parse_duration: int   # seconds
    status: str           # "success", "failed", "partial"
    error_message: Optional[str]
```

### DownloadLog

```python
@dataclass
class DownloadLog:
    id: int
    tld: str
    file_size: int
    records_count: int
    download_duration: int
    parse_duration: int
    status: str
    error_message: Optional[str]
    started_at: datetime
    completed_at: datetime
```

### JobStatus

```python
@dataclass
class JobStatus:
    state: str            # "idle", "running"
    current_tld: Optional[str]
    progress_percent: int
    total_tlds: int
    completed_tlds: int
    started_at: Optional[datetime]
```

## ClickHouse Schema

```sql
-- Zone records table
CREATE TABLE IF NOT EXISTS zone_records (
    domain_name String,
    tld String,
    record_type String,
    record_data String,
    ttl UInt32,
    download_date Date,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(download_date)
ORDER BY (tld, domain_name, record_type, download_date)
SETTINGS index_granularity = 8192;

-- Download logs table
CREATE TABLE IF NOT EXISTS download_logs (
    id UInt64,
    tld String,
    file_size UInt64,
    records_count UInt64,
    download_duration UInt32,
    parse_duration UInt32,
    status String,
    error_message Nullable(String),
    started_at DateTime,
    completed_at DateTime
) ENGINE = MergeTree()
ORDER BY (started_at, tld)
SETTINGS index_granularity = 8192;

-- System settings table
CREATE TABLE IF NOT EXISTS system_settings (
    key String,
    value String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY key;

-- Create indexes
ALTER TABLE zone_records ADD INDEX idx_domain domain_name TYPE bloom_filter GRANULARITY 1;
ALTER TABLE zone_records ADD INDEX idx_tld tld TYPE set(100) GRANULARITY 1;
```



## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Zone File Naming Convention

*For any* TLD name and download date, the saved zone file SHALL follow the naming pattern `{TLD}_{YYYYMMDD}.zone.gz`.

**Validates: Requirements 2.2**

### Property 2: Retry with Exponential Backoff

*For any* failed operation (authentication or download), the system SHALL retry up to 3 times with exponentially increasing delays (e.g., 1s, 2s, 4s).

**Validates: Requirements 1.2, 2.4**

### Property 3: File Integrity Verification

*For any* downloaded zone file, the actual file size SHALL match the Content-Length header from the HTTP response.

**Validates: Requirements 2.3**

### Property 4: Zone Record Parsing Correctness

*For any* valid DNS zone file line in format `domain TTL class type rdata`, the parser SHALL extract domain_name, ttl, record_type, and record_data correctly.

**Validates: Requirements 3.2**

### Property 5: Batch Insert Size

*For any* set of zone records being inserted, the system SHALL insert them in batches of exactly 10,000 records (or fewer for the final batch).

**Validates: Requirements 3.3**

### Property 6: Job Concurrency Prevention

*For any* download job in progress, subsequent download triggers SHALL be rejected until the current job completes.

**Validates: Requirements 5.3**

### Property 7: Scheduler Toggle Persistence

*For any* change to the auto-download setting, the new value SHALL be persisted to the database and survive application restarts.

**Validates: Requirements 6.4**

### Property 8: Log Entry Completeness

*For any* log entry created during download or parse operations, the entry SHALL contain: timestamp, operation type, TLD name, duration, status, and relevant metrics (file_size for downloads, records_count for parsing).

**Validates: Requirements 7.1, 7.2, 7.3**

### Property 9: Status Response Completeness

*For any* status API response, the response SHALL include: last_download_time, total_domains_processed, active_jobs, and progress_percent (if job running).

**Validates: Requirements 4.2, 4.3**

### Property 10: ClickHouse Deduplication

*For any* duplicate zone records (same domain_name, tld, record_type, download_date), only the most recent record SHALL be retained after ReplacingMergeTree optimization.

**Validates: Requirements 9.2**

### Property 11: Environment Configuration Loading

*For any* required environment variable (ICANN_USER, ICANN_PASS, DB_HOST, CLICKHOUSE_PASSWORD), the system SHALL read and use the value from the environment.

**Validates: Requirements 10.2**

### Property 12: Scheduler Initialization

*For any* system startup with auto-download enabled, the scheduler SHALL have a job scheduled for 04:00 daily.

**Validates: Requirements 8.1**

## Error Handling

### Authentication Errors

| Error | Handling |
|-------|----------|
| Invalid credentials | Log error, raise `AuthenticationError`, do not retry |
| Network timeout | Retry with exponential backoff (max 3 attempts) |
| Rate limited (429) | Wait for `Retry-After` header duration, then retry |
| Server error (5xx) | Retry with exponential backoff (max 3 attempts) |

### Download Errors

| Error | Handling |
|-------|----------|
| File not found (404) | Log warning, skip TLD, continue with next |
| Partial download | Delete incomplete file, retry download |
| Disk full | Log critical error, abort all downloads |
| Network timeout | Retry with exponential backoff (max 3 attempts) |

### Parse Errors

| Error | Handling |
|-------|----------|
| Invalid line format | Log warning with line number, skip line, continue |
| Decompression error | Log error, mark TLD as failed, continue with next |
| Memory error | Process file in smaller chunks |

### Database Errors

| Error | Handling |
|-------|----------|
| Connection failed | Retry connection 3 times, then abort |
| Insert failed | Log error, retry batch, if still fails mark as partial |
| Table not found | Auto-create tables on startup |

## Testing Strategy

### Unit Tests

Unit tests will verify specific examples and edge cases:

- CZDS client authentication flow
- Zone file naming convention
- Zone line parsing for various record types (NS, A, AAAA, MX, TXT, SOA)
- Batch size calculation
- Status response formatting
- Error handling for invalid inputs

### Property-Based Tests

Property-based tests will use **Hypothesis** library to verify universal properties:

- **Property 1**: Zone file naming - generate random TLDs and dates, verify naming pattern
- **Property 2**: Retry behavior - simulate failures, verify retry count and delays
- **Property 4**: Zone parsing - generate valid zone lines, verify correct extraction
- **Property 5**: Batch sizing - generate record lists of various sizes, verify batch boundaries
- **Property 8**: Log completeness - generate operations, verify all required fields present
- **Property 9**: Status completeness - generate job states, verify response fields

### Integration Tests

Integration tests will verify component interactions:

- Full download cycle with mock CZDS API
- Database operations with test ClickHouse instance
- Scheduler job execution
- WebSocket log streaming

### Test Configuration

```python
# pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_functions = test_*
addopts = -v --tb=short

# Hypothesis settings
hypothesis_profile = ci
hypothesis_seed = 12345
```

```python
# conftest.py
from hypothesis import settings, Verbosity

settings.register_profile("ci", max_examples=100)
settings.register_profile("dev", max_examples=10)
settings.load_profile("ci")
```

### Test File Structure

```
tests/
├── unit/
│   ├── test_czds_client.py
│   ├── test_zone_parser.py
│   ├── test_db_repository.py
│   └── test_download_service.py
├── property/
│   ├── test_naming_property.py
│   ├── test_parsing_property.py
│   ├── test_batch_property.py
│   └── test_log_property.py
└── integration/
    ├── test_full_download.py
    └── test_scheduler.py
```
