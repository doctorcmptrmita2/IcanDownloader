"""ClickHouse Repository for database operations."""
from datetime import datetime
from typing import List, Optional, Generator
import logging

from clickhouse_driver import Client

from src.models import ZoneRecord, DownloadLog


logger = logging.getLogger(__name__)


class ClickHouseRepository:
    """Repository for ClickHouse database operations."""
    
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
        self._client: Optional[Client] = None
    
    @property
    def client(self) -> Client:
        """Get or create ClickHouse client with optimized settings."""
        if self._client is None:
            self._client = Client(
                host=self.host,
                port=self.port,
                password=self.password,
                database=self.database,
                settings={
                    'insert_block_size': 1000000,  # Larger blocks
                    'max_insert_block_size': 1000000,
                    'min_insert_block_size_rows': 100000,
                    'max_threads': 8,  # Use more threads
                },
                compression=True,  # Enable compression
            )
        return self._client
    
    def _ensure_database_exists(self) -> None:
        """Create database if it doesn't exist using default database."""
        # Connect to default database first
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
        # First ensure database exists
        self._ensure_database_exists()
        
        # Zone records table
        self.client.execute("""
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
        self.client.execute("""
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
        self.client.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key String,
                value String,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY key
        """)
        
        # Create indexes
        try:
            self.client.execute("""
                ALTER TABLE zone_records 
                ADD INDEX IF NOT EXISTS idx_domain domain_name TYPE bloom_filter GRANULARITY 1
            """)
        except Exception:
            pass  # Index might already exist
            
        try:
            self.client.execute("""
                ALTER TABLE zone_records 
                ADD INDEX IF NOT EXISTS idx_tld tld TYPE set(100) GRANULARITY 1
            """)
        except Exception:
            pass  # Index might already exist
        
        logger.info("Database tables initialized")
    
    def insert_zone_records(self, records: List[ZoneRecord], batch_size: int = 100000) -> int:
        """Insert zone records in a single batch for maximum performance.
        
        Args:
            records: List of ZoneRecord objects to insert
            batch_size: Ignored - inserts all records at once for speed
            
        Returns:
            Total number of records inserted
        """
        if not records:
            return 0
        
        # Prepare all data at once - no batching for speed
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
        
        try:
            self.client.execute(
                """
                INSERT INTO zone_records 
                (domain_name, tld, record_type, record_data, ttl, download_date)
                VALUES
                """,
                data,
            )
            return len(records)
        except Exception as e:
            # Try to reconnect and retry once
            logger.warning(f"Insert failed, attempting reconnect: {e}")
            self._client = None  # Force reconnect
            try:
                self.client.execute(
                    """
                    INSERT INTO zone_records 
                    (domain_name, tld, record_type, record_data, ttl, download_date)
                    VALUES
                    """,
                    data,
                )
                return len(records)
            except Exception as e2:
                logger.error(f"Insert failed after reconnect: {e2}")
                raise
    
    def _sanitize_string(self, value: str) -> str:
        """Sanitize string for ClickHouse insertion.
        
        Removes or replaces problematic characters.
        
        Args:
            value: String to sanitize
            
        Returns:
            Sanitized string
        """
        if not value:
            return value
        # Replace null bytes and other problematic characters
        value = value.replace('\x00', '')
        # Limit string length to prevent issues
        if len(value) > 65535:
            value = value[:65535]
        return value
    
    def _batch_records(
        self, records: List[ZoneRecord], batch_size: int
    ) -> Generator[List[ZoneRecord], None, None]:
        """Split records into batches.
        
        Args:
            records: List of records
            batch_size: Size of each batch
            
        Yields:
            Batches of records
        """
        for i in range(0, len(records), batch_size):
            yield records[i:i + batch_size]
    
    def log_download(self, log: DownloadLog) -> None:
        """Insert download log entry.
        
        Args:
            log: DownloadLog object to insert
        """
        # Get next ID
        result = self.client.execute("SELECT max(id) FROM download_logs")
        next_id = (result[0][0] or 0) + 1
        
        self.client.execute(
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
    
    def get_recent_logs(self, limit: int = 100) -> List[DownloadLog]:
        """Fetch recent download logs.
        
        Args:
            limit: Maximum number of logs to return
            
        Returns:
            List of DownloadLog objects
        """
        result = self.client.execute(
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
    
    def get_setting(self, key: str) -> Optional[str]:
        """Get system setting value.
        
        Args:
            key: Setting key
            
        Returns:
            Setting value or None if not found
        """
        result = self.client.execute(
            """
            SELECT value FROM system_settings 
            WHERE key = %(key)s
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {"key": key},
        )
        
        return result[0][0] if result else None
    
    def set_setting(self, key: str, value: str) -> None:
        """Set system setting value.
        
        Args:
            key: Setting key
            value: Setting value
        """
        self.client.execute(
            """
            INSERT INTO system_settings (key, value, updated_at)
            VALUES
            """,
            [(key, value, datetime.now())],
        )
    
    def get_total_records_count(self) -> int:
        """Get total number of zone records.
        
        Returns:
            Total record count
        """
        result = self.client.execute("SELECT count() FROM zone_records")
        return result[0][0]
    
    def get_last_download_time(self) -> Optional[datetime]:
        """Get the time of the last successful download.
        
        Returns:
            Last download time or None
        """
        result = self.client.execute(
            """
            SELECT max(completed_at) FROM download_logs 
            WHERE status = 'success'
            """
        )
        return result[0][0] if result and result[0][0] else None
    
    def close(self) -> None:
        """Close database connection."""
        if self._client:
            self._client.disconnect()
            self._client = None
    
    def search_domains(
        self, 
        query: str, 
        tld: Optional[str] = None,
        record_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> tuple:
        """Search domains by name pattern.
        
        Args:
            query: Domain name search pattern (supports % wildcard)
            tld: Filter by TLD (optional)
            record_type: Filter by record type (optional)
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            Tuple of (results list, total count)
        """
        conditions = ["domain_name LIKE %(query)s"]
        params = {"query": f"%{query}%", "limit": limit, "offset": offset}
        
        if tld:
            conditions.append("tld = %(tld)s")
            params["tld"] = tld
        
        if record_type:
            conditions.append("record_type = %(record_type)s")
            params["record_type"] = record_type
        
        where_clause = " AND ".join(conditions)
        
        # Get total count
        count_result = self.client.execute(
            f"SELECT count() FROM zone_records WHERE {where_clause}",
            params
        )
        total = count_result[0][0]
        
        # Get results
        result = self.client.execute(
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
    
    def get_tld_stats(self) -> List[dict]:
        """Get statistics per TLD.
        
        Returns:
            List of TLD statistics
        """
        try:
            result = self.client.execute("""
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
    
    def get_record_type_stats(self) -> List[dict]:
        """Get statistics per record type.
        
        Returns:
            List of record type statistics
        """
        result = self.client.execute("""
            SELECT 
                record_type,
                count() as count
            FROM zone_records
            GROUP BY record_type
            ORDER BY count DESC
        """)
        
        return [{"type": row[0], "count": row[1]} for row in result]
    
    def get_dashboard_stats(self) -> dict:
        """Get overall dashboard statistics.
        
        Returns:
            Dictionary with dashboard stats
        """
        stats = {
            "total_records": 0,
            "unique_domains": 0,
            "tld_count": 0,
            "last_update": None,
            "successful_downloads": 0,
            "failed_downloads": 0,
        }
        
        try:
            # Total records
            result = self.client.execute("SELECT count() FROM zone_records")
            stats["total_records"] = result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get total records: {e}")
        
        try:
            # Unique domains
            result = self.client.execute(
                "SELECT countDistinct(domain_name) FROM zone_records"
            )
            stats["unique_domains"] = result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get unique domains: {e}")
        
        try:
            # TLD count
            result = self.client.execute(
                "SELECT countDistinct(tld) FROM zone_records"
            )
            stats["tld_count"] = result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get TLD count: {e}")
        
        try:
            # Last update
            result = self.client.execute(
                "SELECT max(download_date) FROM zone_records"
            )
            if result and result[0][0]:
                stats["last_update"] = result[0][0].isoformat()
        except Exception as e:
            logger.warning(f"Failed to get last update: {e}")
        
        try:
            # Successful downloads
            result = self.client.execute(
                "SELECT count() FROM download_logs WHERE status = 'success'"
            )
            stats["successful_downloads"] = result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get success count: {e}")
        
        try:
            # Failed downloads
            result = self.client.execute(
                "SELECT count() FROM download_logs WHERE status = 'failed'"
            )
            stats["failed_downloads"] = result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get failed count: {e}")
        
        return stats
    
    def get_available_tlds(self) -> List[str]:
        """Get list of available TLDs in database.
        
        Returns:
            List of TLD names
        """
        try:
            result = self.client.execute(
                "SELECT DISTINCT tld FROM zone_records ORDER BY tld"
            )
            return [row[0] for row in result]
        except Exception as e:
            logger.warning(f"Failed to get available TLDs: {e}")
            return []
