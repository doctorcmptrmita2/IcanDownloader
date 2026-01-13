# Implementation Plan: ICANN Downloader

## Overview

Bu plan, ICANN CZDS zone dosyalarını indiren, parse eden ve ClickHouse'a kaydeden Python worker uygulamasının implementasyonunu adım adım tanımlar. Flask web arayüzü ve APScheduler cron desteği içerir.

## Tasks

- [x] 1. Proje yapısı ve temel konfigürasyon
  - [x] 1.1 Proje dizin yapısını oluştur
    - `src/` ana kaynak dizini
    - `src/models/` data modelleri
    - `src/services/` servis katmanı
    - `src/api/` Flask routes
    - `tests/` test dizini
    - `templates/` HTML şablonları
    - _Requirements: 10.1, 10.2_
  - [x] 1.2 requirements.txt ve Dockerfile oluştur
    - Flask, Flask-SocketIO, APScheduler, clickhouse-driver, requests, hypothesis
    - Python 3.11 base image
    - _Requirements: 10.1, 10.2_
  - [x] 1.3 Config modülü oluştur (`src/config.py`)
    - Environment variables okuma (ICANN_USER, ICANN_PASS, DB_HOST, CLICKHOUSE_PASSWORD)
    - Default değerler ve validation
    - _Requirements: 10.2_
  - [x] 1.4 Property test: Environment configuration loading
    - **Property 11: Environment Configuration Loading**
    - **Validates: Requirements 10.2**

- [x] 2. Data modelleri oluştur
  - [x] 2.1 Model sınıflarını oluştur (`src/models/`)
    - ZoneRecord dataclass
    - DownloadResult dataclass
    - DownloadLog dataclass
    - JobStatus dataclass
    - _Requirements: 3.2, 7.2, 7.3_

- [x] 3. ClickHouse Repository implementasyonu
  - [x] 3.1 ClickHouseRepository sınıfını oluştur (`src/services/db_repository.py`)
    - Connection management
    - `init_tables()` - tablo oluşturma
    - `insert_zone_records()` - batch insert
    - `log_download()` - log kaydetme
    - `get_recent_logs()` - log okuma
    - `get_setting()` / `set_setting()` - ayar yönetimi
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_
  - [x] 3.2 Property test: Batch insert size
    - **Property 5: Batch Insert Size**
    - **Validates: Requirements 3.3**
  - [x] 3.3 Property test: ClickHouse deduplication
    - **Property 10: ClickHouse Deduplication**
    - **Validates: Requirements 9.2**

- [x] 4. CZDS Client implementasyonu
  - [x] 4.1 CZDSClient sınıfını oluştur (`src/services/czds_client.py`)
    - `authenticate()` - ICANN API authentication
    - `get_approved_tlds()` - TLD listesi çekme
    - `download_zone_file()` - zone dosyası indirme
    - `_refresh_token_if_needed()` - token yenileme
    - Exponential backoff retry logic
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.4, 2.5_
  - [x] 4.2 Property test: Zone file naming convention
    - **Property 1: Zone File Naming Convention**
    - **Validates: Requirements 2.2**
  - [x] 4.3 Property test: Retry with exponential backoff
    - **Property 2: Retry with Exponential Backoff**
    - **Validates: Requirements 1.2, 2.4**
  - [x] 4.4 Property test: File integrity verification
    - **Property 3: File Integrity Verification**
    - **Validates: Requirements 2.3**

- [x] 5. Zone Parser implementasyonu
  - [x] 5.1 ZoneParser sınıfını oluştur (`src/services/zone_parser.py`)
    - `parse_zone_file()` - gzip açma ve parse etme
    - `_decompress_file()` - decompression
    - `_parse_line()` - tek satır parse
    - Generator pattern ile memory-efficient parsing
    - _Requirements: 3.1, 3.2, 3.4_
  - [x] 5.2 Property test: Zone record parsing correctness
    - **Property 4: Zone Record Parsing Correctness**
    - **Validates: Requirements 3.2**

- [x] 6. Checkpoint - Core services tamamlandı
  - Tüm testlerin geçtiğinden emin ol
  - Kullanıcıya soru varsa sor

- [x] 7. Logger Service implementasyonu
  - [x] 7.1 LoggerService sınıfını oluştur (`src/services/logger_service.py`)
    - `log()` - genel loglama
    - `log_download_start()` - indirme başlangıcı
    - `log_download_complete()` - indirme bitişi
    - `log_parse_progress()` - parse ilerlemesi
    - SocketIO entegrasyonu
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_
  - [x] 7.2 Property test: Log entry completeness
    - **Property 8: Log Entry Completeness**
    - **Validates: Requirements 7.1, 7.2, 7.3**

- [x] 8. Download Service implementasyonu
  - [x] 8.1 DownloadService sınıfını oluştur (`src/services/download_service.py`)
    - `run_full_download()` - tam indirme döngüsü
    - `download_single_tld()` - tek TLD indirme
    - `get_current_status()` - durum sorgulama
    - Job state management
    - _Requirements: 2.1, 2.5, 3.3, 3.5, 5.3_
  - [x] 8.2 Property test: Job concurrency prevention
    - **Property 6: Job Concurrency Prevention**
    - **Validates: Requirements 5.3**

- [x] 9. Scheduler Service implementasyonu
  - [x] 9.1 SchedulerService sınıfını oluştur (`src/services/scheduler_service.py`)
    - APScheduler entegrasyonu
    - `start()` / `stop()` - scheduler kontrolü
    - `enable_auto_download()` / `disable_auto_download()` - toggle
    - `is_enabled()` / `get_next_run_time()` - durum sorgulama
    - 04:00 cron job konfigürasyonu
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 8.1, 8.2, 8.3, 8.4_
  - [x] 9.2 Property test: Scheduler toggle persistence
    - **Property 7: Scheduler Toggle Persistence**
    - **Validates: Requirements 6.4**
  - [x] 9.3 Property test: Scheduler initialization
    - **Property 12: Scheduler Initialization**
    - **Validates: Requirements 8.1**

- [x] 10. Checkpoint - Backend services tamamlandı
  - Tüm testlerin geçtiğinden emin ol
  - Kullanıcıya soru varsa sor

- [x] 11. Flask Web Application
  - [x] 11.1 Flask app ve routes oluştur (`src/api/app.py`)
    - Dashboard route (`/`)
    - API routes (`/api/download`, `/api/auto-download`, `/api/status`, `/api/logs`)
    - SocketIO event handlers
    - _Requirements: 4.1, 5.1, 5.2, 5.4, 6.1_
  - [x] 11.2 Property test: Status response completeness
    - **Property 9: Status Response Completeness**
    - **Validates: Requirements 4.2, 4.3**
  - [x] 11.3 Dashboard HTML template oluştur (`templates/dashboard.html`)
    - Status gösterimi
    - Manuel download butonu
    - Auto-download toggle
    - Real-time log görüntüleme
    - Progress bar
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 5.1, 5.2, 5.3, 5.4, 6.1, 6.2_

- [x] 12. Main entry point ve Docker
  - [x] 12.1 Main entry point oluştur (`src/main.py`)
    - Dependency injection
    - Database initialization
    - Scheduler başlatma
    - Flask app çalıştırma
    - _Requirements: 10.4_
  - [x] 12.2 Dockerfile finalize et
    - Multi-stage build
    - Health check
    - Port expose (8080)
    - _Requirements: 10.1, 10.3_

- [x] 13. Final Checkpoint
  - Tüm testlerin geçtiğinden emin ol
  - Docker build test et
  - Kullanıcıya soru varsa sor

## Notes

- Her task belirli requirements'lara referans verir
- Checkpoint'ler incremental validation sağlar
- Property testler Hypothesis kütüphanesi ile yazılacak
- Unit testler pytest ile yazılacak
