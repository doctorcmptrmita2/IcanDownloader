"""ClickHouse Repository for database operations."""
from datetime import datetime
from typing import List, Optional, Generator
import logging
import threading
import time

from clickhouse_driver import Client

from src.models import ZoneRecord, DownloadLog


logger = logging.getLogger(__name__)


class ClickHouseRepository:
    """Repository for ClickHouse database operations.
    
    Uses separate connections for insert operations and read operations
    to avoid "Simultaneous queries on single connection" errors.
    
    - Insert operations: Use dedicated insert_client with lock
    - Read operations: Create new client per request (thread-safe)
    """
    
    def __init__(self, host: str, password: str, database: str = "icann", port: int = 9000):
        """Initialize connection to ClickHouse.
        
        Args:
            host: ClickHouse server host
            password: ClickHouse password
            database: Database name
            port: ClickHouse port
        """
        self.host = host
        self.password = password
        self.database = database
        self.port = port
        self._insert_client: Optional[Client] = None
        self._insert_lock = threading.Lock()
    
    def _create_client(self) -> Client:
        """Create a new ClickHouse client."""
        return Client(
            host=self.host,
            port=self.port,
            password=self.password,
            database=self.database,
            connect_timeout=30,
            send_receive_timeout=300,
            sync_request_timeout=300,
        )
    
    def _get_read_client(self) -> Client:
        """Get a new client for read operations (one per request)."""
        return self._create_client()
    
    def _get_insert_client(self) -> Client:
        """Get or create client for insert operations (reused with lock)."""
        if self._insert_client is None:
            self._insert_client = self._create_client()
        return self._insert_client
    
    def _reconnect_insert(self) -> None:
        """Force reconnection for insert client."""
        if self._insert_client:
            try:
                self._insert_client.disconnect()
            except Exception:
                pass
        self._insert_client = self._create_client()
    
    def _ensure_database_exists(self) -> None:
        """Create database if it doesn't exist using default database."""
        default_client = Client(
            host=self.host,
            port=self.port,
            password=self.password,
            database='default',
        )
        default_client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        default_client.disconnect()
        logger.info(f"Database '{self.database}' ensured to exist")
    
    def init_tables(self) -> None:
        """Create tables if they don't exist."""
        self._ensure_database_exists()
        
        client = self._get_read_client()
        try:
            # Zone records table
            client.execute("""
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
                SETTINGS index_granularity = 8192
            """)
            
            # Download logs table
            client.execute("""
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
                SETTINGS index_granularity = 8192
            """)
            
            # System settings table
            client.execute("""
                CREATE TABLE IF NOT EXISTS system_settings (
                    key String,
                    value String,
                    updated_at DateTime DEFAULT now()
                ) ENGINE = ReplacingMergeTree(updated_at)
                ORDER BY key
            """)
            
            # Create indexes
            try:
                client.execute("""
                    ALTER TABLE zone_records 
                    ADD INDEX IF NOT EXISTS idx_domain domain_name TYPE bloom_filter GRANULARITY 1
                """)
            except Exception:
                pass
                
            try:
                client.execute("""
                    ALTER TABLE zone_records 
                    ADD INDEX IF NOT EXISTS idx_tld tld TYPE set(100) GRANULARITY 1
                """)
            except Exception:
                pass
            
            logger.info("Database tables initialized")
        finally:
            client.disconnect()

    def insert_zone_records(self, records: List[ZoneRecord], batch_size: int = 100000) -> int:
        """Insert zone records with robust retry logic using dedicated insert client.
        
        Args:
            records: List of ZoneRecord objects to insert
            batch_size: Ignored - inserts all records at once for speed
            
        Returns:
            Total number of records inserted
        """
        if not records:
            return 0
        
        data = [
            (
                self._sanitize_string(r.domain_name),
                r.tld,
                r.record_type,
                self._sanitize_string(r.record_data),
                r.ttl,
                r.download_date,
            )
            for r in records
        ]
        
        max_retries = 5
        with self._insert_lock:
            for attempt in range(max_retries):
                try:
                    client = self._get_insert_client()
                    client.execute(
                        """
                        INSERT INTO zone_records 
                        (domain_name, tld, record_type, record_data, ttl, download_date)
                        VALUES
                        """,
                        data,
                    )
                    return len(records)
                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"Insert attempt {attempt + 1} failed: {error_msg[:100]}")
                    self._reconnect_insert()
                    
                    if attempt < max_retries - 1:
                        time.sleep(1 + attempt)
                    else:
                        logger.error(f"Insert failed after {max_retries} attempts: {error_msg}")
                        raise
        return 0
    
    def _sanitize_string(self, value: str) -> str:
        """Sanitize string for ClickHouse insertion."""
        if not value:
            return value
        value = value.replace('\x00', '')
        # Handle encoding issues
        try:
            value = value.encode('utf-8', errors='replace').decode('utf-8')
        except Exception:
            value = ''.join(c if ord(c) < 128 else '?' for c in value)
        if len(value) > 65535:
            value = value[:65535]
        return value
    
    def _sanitize_search_query(self, query: str) -> str:
        """Sanitize search query for safe LIKE operations.
        
        Escapes special characters that could cause SQL issues.
        """
        if not query:
            return query
        # Remove null bytes
        query = query.replace('\x00', '')
        # Escape special LIKE characters
        query = query.replace('\\', '\\\\')
        query = query.replace('%', '\\%')
        query = query.replace('_', '\\_')
        # Remove quotes that could break the query
        query = query.replace("'", "")
        query = query.replace('"', '')
        query = query.replace('`', '')
        # Limit length
        if len(query) > 255:
            query = query[:255]
        return query
    
    def _batch_records(
        self, records: List[ZoneRecord], batch_size: int
    ) -> Generator[List[ZoneRecord], None, None]:
        """Split records into batches."""
        for i in range(0, len(records), batch_size):
            yield records[i:i + batch_size]
    
    def log_download(self, log: DownloadLog) -> None:
        """Insert download log entry using insert client."""
        with self._insert_lock:
            try:
                client = self._get_insert_client()
                result = client.execute("SELECT max(id) FROM download_logs")
                next_id = (result[0][0] or 0) + 1
                
                client.execute(
                    """
                    INSERT INTO download_logs 
                    (id, tld, file_size, records_count, download_duration, parse_duration, 
                     status, error_message, started_at, completed_at)
                    VALUES
                    """,
                    [(
                        next_id,
                        log.tld,
                        log.file_size,
                        log.records_count,
                        log.download_duration,
                        log.parse_duration,
                        log.status,
                        log.error_message,
                        log.started_at,
                        log.completed_at,
                    )],
                )
            except Exception as e:
                logger.error(f"Failed to log download: {e}")
                self._reconnect_insert()
                raise
    
    def get_recent_logs(self, limit: int = 100) -> List[DownloadLog]:
        """Fetch recent download logs using read client."""
        client = self._get_read_client()
        try:
            result = client.execute(
                """
                SELECT id, tld, file_size, records_count, download_duration, 
                       parse_duration, status, error_message, started_at, completed_at
                FROM download_logs
                ORDER BY started_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            
            return [
                DownloadLog(
                    id=row[0],
                    tld=row[1],
                    file_size=row[2],
                    records_count=row[3],
                    download_duration=row[4],
                    parse_duration=row[5],
                    status=row[6],
                    error_message=row[7],
                    started_at=row[8],
                    completed_at=row[9],
                )
                for row in result
            ]
        finally:
            client.disconnect()
    
    def get_setting(self, key: str) -> Optional[str]:
        """Get system setting value using read client."""
        client = self._get_read_client()
        try:
            result = client.execute(
                """
                SELECT value FROM system_settings 
                WHERE key = %(key)s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                {"key": key},
            )
            return result[0][0] if result else None
        finally:
            client.disconnect()
    
    def set_setting(self, key: str, value: str) -> None:
        """Set system setting value using insert client."""
        with self._insert_lock:
            try:
                client = self._get_insert_client()
                client.execute(
                    """
                    INSERT INTO system_settings (key, value, updated_at)
                    VALUES
                    """,
                    [(key, value, datetime.now())],
                )
            except Exception as e:
                logger.error(f"Failed to set setting: {e}")
                self._reconnect_insert()
                raise

    def get_total_records_count(self) -> int:
        """Get total number of zone records using read client."""
        client = self._get_read_client()
        try:
            result = client.execute("SELECT count() FROM zone_records")
            return result[0][0]
        finally:
            client.disconnect()
    
    def get_last_download_time(self) -> Optional[datetime]:
        """Get the time of the last successful download using read client."""
        client = self._get_read_client()
        try:
            result = client.execute(
                """
                SELECT max(completed_at) FROM download_logs 
                WHERE status = 'success'
                """
            )
            return result[0][0] if result and result[0][0] else None
        finally:
            client.disconnect()
    
    def close(self) -> None:
        """Close database connections."""
        with self._insert_lock:
            if self._insert_client:
                try:
                    self._insert_client.disconnect()
                except Exception:
                    pass
                self._insert_client = None
    
    def search_domains(
        self, 
        query: str, 
        tld: Optional[str] = None,
        record_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> tuple:
        """Search domains by name pattern using read client."""
        client = self._get_read_client()
        try:
            # Sanitize query to prevent SQL issues
            safe_query = self._sanitize_search_query(query)
            
            conditions = ["domain_name LIKE %(query)s"]
            params = {"query": f"%{safe_query}%", "limit": limit, "offset": offset}
            
            if tld:
                conditions.append("tld = %(tld)s")
                params["tld"] = tld
            
            if record_type:
                conditions.append("record_type = %(record_type)s")
                params["record_type"] = record_type
            
            where_clause = " AND ".join(conditions)
            
            count_result = client.execute(
                f"SELECT count() FROM zone_records WHERE {where_clause}",
                params
            )
            total = count_result[0][0]
            
            result = client.execute(
                f"""
                SELECT domain_name, tld, record_type, record_data, ttl, download_date
                FROM zone_records
                WHERE {where_clause}
                ORDER BY domain_name
                LIMIT %(limit)s OFFSET %(offset)s
                """,
                params
            )
            
            domains = [
                {
                    "domain_name": row[0],
                    "tld": row[1],
                    "record_type": row[2],
                    "record_data": row[3],
                    "ttl": row[4],
                    "download_date": row[5].isoformat() if row[5] else None,
                }
                for row in result
            ]
            
            return domains, total
        finally:
            client.disconnect()
    
    def get_tld_stats(self) -> List[dict]:
        """Get statistics per TLD using read client."""
        client = self._get_read_client()
        try:
            result = client.execute("""
                SELECT 
                    tld,
                    count() as record_count,
                    countDistinct(domain_name) as unique_domains,
                    max(download_date) as last_updated
                FROM zone_records
                GROUP BY tld
                ORDER BY record_count DESC
            """)
            
            return [
                {
                    "tld": row[0],
                    "record_count": row[1],
                    "unique_domains": row[2],
                    "last_updated": row[3].isoformat() if row[3] else None,
                }
                for row in result
            ]
        except Exception as e:
            logger.warning(f"Failed to get TLD stats: {e}")
            return []
        finally:
            client.disconnect()
    
    def get_record_type_stats(self) -> List[dict]:
        """Get statistics per record type using read client."""
        client = self._get_read_client()
        try:
            result = client.execute("""
                SELECT 
                    record_type,
                    count() as count
                FROM zone_records
                GROUP BY record_type
                ORDER BY count DESC
            """)
            return [{"type": row[0], "count": row[1]} for row in result]
        finally:
            client.disconnect()

    def get_dashboard_stats(self) -> dict:
        """Get overall dashboard statistics using read client."""
        stats = {
            "total_records": 0,
            "unique_domains": 0,
            "tld_count": 0,
            "last_update": None,
            "successful_downloads": 0,
            "failed_downloads": 0,
        }
        
        client = self._get_read_client()
        try:
            try:
                result = client.execute("SELECT count() FROM zone_records")
                stats["total_records"] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get total records: {e}")
            
            try:
                result = client.execute(
                    "SELECT countDistinct(domain_name) FROM zone_records"
                )
                stats["unique_domains"] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get unique domains: {e}")
            
            try:
                result = client.execute(
                    "SELECT countDistinct(tld) FROM zone_records"
                )
                stats["tld_count"] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get TLD count: {e}")
            
            try:
                result = client.execute(
                    "SELECT max(download_date) FROM zone_records"
                )
                if result and result[0][0]:
                    stats["last_update"] = result[0][0].isoformat()
            except Exception as e:
                logger.warning(f"Failed to get last update: {e}")
            
            try:
                result = client.execute(
                    "SELECT count() FROM download_logs WHERE status = 'success'"
                )
                stats["successful_downloads"] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get success count: {e}")
            
            try:
                result = client.execute(
                    "SELECT count() FROM download_logs WHERE status = 'failed'"
                )
                stats["failed_downloads"] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get failed count: {e}")
            
            return stats
        finally:
            client.disconnect()
    
    def get_available_tlds(self) -> List[str]:
        """Get list of available TLDs in database using read client."""
        client = self._get_read_client()
        try:
            result = client.execute(
                "SELECT DISTINCT tld FROM zone_records ORDER BY tld"
            )
            return [row[0] for row in result]
        except Exception as e:
            logger.warning(f"Failed to get available TLDs: {e}")
            return []
        finally:
            client.disconnect()
    
    def delete_tld_records(self, tld: str) -> int:
        """Delete all records for a specific TLD using insert client.
        
        Used before re-downloading to prevent data duplication.
        """
        with self._insert_lock:
            try:
                client = self._get_insert_client()
                
                count_result = client.execute(
                    "SELECT count() FROM zone_records WHERE tld = %(tld)s",
                    {"tld": tld}
                )
                count = count_result[0][0] if count_result else 0
                
                if count > 0:
                    client.execute(
                        "ALTER TABLE zone_records DELETE WHERE tld = %(tld)s",
                        {"tld": tld}
                    )
                    logger.info(f"🗑️ Deleted {count:,} records for TLD: {tld}")
                
                return count
            except Exception as e:
                logger.error(f"Failed to delete records for TLD {tld}: {e}")
                self._reconnect_insert()
                raise
    
    def delete_old_records(self, days: int = 7) -> int:
        """Delete records older than specified days using insert client."""
        with self._insert_lock:
            try:
                client = self._get_insert_client()
                
                count_result = client.execute(
                    "SELECT count() FROM zone_records WHERE download_date < today() - %(days)s",
                    {"days": days}
                )
                count = count_result[0][0] if count_result else 0
                
                if count > 0:
                    client.execute(
                        "ALTER TABLE zone_records DELETE WHERE download_date < today() - %(days)s",
                        {"days": days}
                    )
                    logger.info(f"🗑️ Deleted {count:,} records older than {days} days")
                
                return count
            except Exception as e:
                logger.error(f"Failed to delete old records: {e}")
                self._reconnect_insert()
                raise
