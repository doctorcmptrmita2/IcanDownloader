# Requirements Document

## Introduction

Bu sistem, ICANN CZDS (Centralized Zone Data Service) API'sinden günlük domain zone dosyalarını indiren, parse eden ve ClickHouse veritabanına kaydeden bir Python worker uygulamasıdır. Dokploy üzerinde çalışacak, web arayüzü ile manuel/otomatik indirme seçenekleri sunacak ve cron job ile her gece 04:00'da otomatik çalışacaktır.

## Glossary

- **ICANN_Downloader**: ICANN CZDS API'sinden zone dosyalarını indiren ana Python uygulaması
- **Zone_File**: TLD (Top Level Domain) için DNS kayıtlarını içeren dosya
- **CZDS_API**: ICANN'ın Centralized Zone Data Service REST API'si
- **ClickHouse_DB**: Verilerin depolandığı ClickHouse veritabanı
- **Web_Interface**: Manuel ve otomatik indirme işlemlerini yönetmek için kullanılan web arayüzü
- **Download_Job**: Tek bir zone dosyası indirme işlemi
- **Parse_Job**: İndirilen zone dosyasını işleyip veritabanına yazma işlemi
- **Log_Entry**: İndirme ve parse işlemlerinin kayıt girişi
- **Cron_Scheduler**: Zamanlanmış görevleri çalıştıran zamanlayıcı

## Requirements

### Requirement 1: ICANN CZDS Authentication

**User Story:** As a system administrator, I want the system to authenticate with ICANN CZDS API, so that I can download authorized zone files.

#### Acceptance Criteria

1. WHEN the system starts, THE ICANN_Downloader SHALL authenticate with CZDS API using configured credentials (ICANN_USER, ICANN_PASS)
2. WHEN authentication fails, THE ICANN_Downloader SHALL log the error and retry with exponential backoff up to 3 times
3. WHEN authentication succeeds, THE ICANN_Downloader SHALL store the access token for subsequent API calls
4. IF the access token expires, THEN THE ICANN_Downloader SHALL automatically refresh the token

### Requirement 2: Zone File Download

**User Story:** As a system administrator, I want to download zone files from ICANN CZDS, so that I can process domain data.

#### Acceptance Criteria

1. WHEN a download is triggered, THE ICANN_Downloader SHALL fetch the list of approved TLDs from CZDS API
2. WHEN downloading a zone file, THE ICANN_Downloader SHALL save it to a temporary directory with proper naming (TLD_YYYYMMDD.zone.gz)
3. WHEN a download completes, THE ICANN_Downloader SHALL verify file integrity using content-length header
4. IF a download fails, THEN THE ICANN_Downloader SHALL retry up to 3 times with exponential backoff
5. WHEN downloading multiple zone files, THE ICANN_Downloader SHALL process them sequentially to avoid rate limiting

### Requirement 3: Zone File Parsing

**User Story:** As a data analyst, I want zone files to be parsed and stored in ClickHouse, so that I can query domain data.

#### Acceptance Criteria

1. WHEN a zone file is downloaded, THE Parse_Job SHALL decompress and parse the gzipped zone file
2. WHEN parsing a zone file, THE Parse_Job SHALL extract domain names, record types, and associated data
3. WHEN parsing completes, THE Parse_Job SHALL insert records into ClickHouse in batches of 10,000 rows
4. IF parsing fails, THEN THE Parse_Job SHALL log the error with file name and line number
5. WHEN inserting to ClickHouse, THE Parse_Job SHALL use the configured connection (DB_HOST, CLICKHOUSE_PASSWORD)

### Requirement 4: Web Interface - Dashboard

**User Story:** As a user, I want a web dashboard to monitor download status, so that I can track system activity.

#### Acceptance Criteria

1. WHEN a user visits the dashboard, THE Web_Interface SHALL display current download status and recent activity
2. WHEN displaying status, THE Web_Interface SHALL show: last download time, total domains processed, active jobs
3. WHEN a job is running, THE Web_Interface SHALL display real-time progress with percentage complete
4. WHEN displaying logs, THE Web_Interface SHALL show the last 100 log entries with timestamps

### Requirement 5: Web Interface - Manual Download

**User Story:** As a user, I want to manually trigger downloads, so that I can get fresh data on demand.

#### Acceptance Criteria

1. WHEN a user clicks "Download Now", THE Web_Interface SHALL trigger an immediate download job
2. WHEN a manual download is triggered, THE Web_Interface SHALL display a confirmation message
3. WHILE a download is in progress, THE Web_Interface SHALL disable the download button to prevent duplicate jobs
4. WHEN a manual download completes, THE Web_Interface SHALL notify the user with success/failure status

### Requirement 6: Web Interface - Automatic Download Toggle

**User Story:** As a user, I want to enable/disable automatic downloads, so that I can control scheduled operations.

#### Acceptance Criteria

1. WHEN a user toggles automatic download, THE Web_Interface SHALL enable or disable the cron scheduler
2. WHEN automatic download is enabled, THE Web_Interface SHALL display the next scheduled run time (04:00 daily)
3. WHEN automatic download is disabled, THE Web_Interface SHALL stop all scheduled jobs
4. WHEN the toggle state changes, THE Web_Interface SHALL persist the setting to survive restarts

### Requirement 7: Logging System

**User Story:** As a system administrator, I want comprehensive logging, so that I can troubleshoot issues and monitor operations.

#### Acceptance Criteria

1. WHEN any operation starts, THE ICANN_Downloader SHALL create a Log_Entry with timestamp, operation type, and status
2. WHEN downloading a file, THE Log_Entry SHALL include: TLD name, file size, download duration, success/failure
3. WHEN parsing a file, THE Log_Entry SHALL include: TLD name, records processed, parse duration, errors encountered
4. WHEN an error occurs, THE Log_Entry SHALL include: error message, stack trace, and context information
5. THE Web_Interface SHALL display logs in real-time as they are generated

### Requirement 8: Cron Scheduler

**User Story:** As a system administrator, I want automatic daily downloads at 04:00, so that data stays current without manual intervention.

#### Acceptance Criteria

1. WHEN the system starts with automatic download enabled, THE Cron_Scheduler SHALL schedule a job for 04:00 daily
2. WHEN the scheduled time arrives, THE Cron_Scheduler SHALL trigger a full download and parse cycle
3. IF a scheduled job fails, THEN THE Cron_Scheduler SHALL log the failure and attempt retry after 1 hour
4. WHEN a scheduled job completes, THE Cron_Scheduler SHALL log completion status and statistics

### Requirement 9: ClickHouse Data Model

**User Story:** As a data analyst, I want domain data stored in a queryable format, so that I can analyze domain registrations.

#### Acceptance Criteria

1. THE ClickHouse_DB SHALL create and use the following tables:

   **Table: zone_records** (Ana DNS kayıtları tablosu)
   - `id` - UInt64, auto-increment
   - `domain_name` - String (örn: "example.com")
   - `tld` - String (örn: "com", "net", "org")
   - `record_type` - String (NS, A, AAAA, CNAME, MX, TXT, SOA)
   - `record_data` - String (nameserver, IP adresi, vb.)
   - `ttl` - UInt32 (Time To Live değeri)
   - `download_date` - Date (indirme tarihi)
   - `created_at` - DateTime (kayıt oluşturma zamanı)

   **Table: download_logs** (İndirme logları tablosu)
   - `id` - UInt64, auto-increment
   - `tld` - String
   - `file_size` - UInt64 (bytes)
   - `records_count` - UInt64
   - `download_duration` - UInt32 (seconds)
   - `parse_duration` - UInt32 (seconds)
   - `status` - String (success, failed, partial)
   - `error_message` - Nullable(String)
   - `started_at` - DateTime
   - `completed_at` - DateTime

   **Table: system_settings** (Sistem ayarları tablosu)
   - `key` - String
   - `value` - String
   - `updated_at` - DateTime

2. WHEN inserting records, THE ClickHouse_DB SHALL use ReplacingMergeTree engine with (domain_name, tld, record_type, download_date) as unique key
3. THE ClickHouse_DB SHALL partition zone_records by toYYYYMM(download_date) for efficient querying
4. WHEN querying, THE ClickHouse_DB SHALL support filtering by TLD, date range, and record type
5. THE ClickHouse_DB SHALL create indexes on domain_name and tld columns for fast lookups

### Requirement 10: Docker Deployment

**User Story:** As a DevOps engineer, I want the application containerized, so that it can be deployed on Dokploy.

#### Acceptance Criteria

1. THE ICANN_Downloader SHALL be packaged as a Docker container with all dependencies
2. THE Docker container SHALL read configuration from environment variables (ICANN_USER, ICANN_PASS, DB_HOST, CLICKHOUSE_PASSWORD)
3. THE Docker container SHALL expose a web port (default 8080) for the Web_Interface
4. WHEN the container starts, THE ICANN_Downloader SHALL initialize database tables if they don't exist
