"""Configuration module for ICANN Downloader.

Reads configuration from environment variables with validation.
"""
import os
from dataclasses import dataclass
from typing import Optional


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""
    pass


@dataclass
class Config:
    """Application configuration loaded from environment variables."""
    
    # ICANN CZDS credentials
    icann_user: str
    icann_pass: str
    
    # ClickHouse database
    db_host: str
    clickhouse_password: str
    db_name: str = "icann"
    db_port: int = 9000
    
    # Web server
    port: int = 8080
    debug: bool = False
    
    # Download settings
    temp_dir: str = "/app/temp"
    batch_size: int = 10000
    max_retries: int = 3
    
    # Scheduler settings
    cron_hour: int = 4
    cron_minute: int = 0
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables.
        
        Required environment variables:
        - ICANN_USER: ICANN CZDS username
        - ICANN_PASS: ICANN CZDS password
        - DB_HOST: ClickHouse host
        - CLICKHOUSE_PASSWORD: ClickHouse password
        
        Optional environment variables:
        - DB_NAME: Database name (default: icann)
        - DB_PORT: ClickHouse port (default: 9000)
        - PORT: Web server port (default: 8080)
        - DEBUG: Enable debug mode (default: false)
        - TEMP_DIR: Temporary directory for downloads (default: /app/temp)
        - BATCH_SIZE: Batch size for DB inserts (default: 10000)
        - MAX_RETRIES: Max retry attempts (default: 3)
        - CRON_HOUR: Hour for scheduled job (default: 4)
        - CRON_MINUTE: Minute for scheduled job (default: 0)
        
        Returns:
            Config: Configuration object
            
        Raises:
            ConfigurationError: If required variables are missing
        """
        # Required variables
        icann_user = os.environ.get("ICANN_USER")
        icann_pass = os.environ.get("ICANN_PASS")
        db_host = os.environ.get("DB_HOST")
        clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")
        
        missing = []
        if not icann_user:
            missing.append("ICANN_USER")
        if not icann_pass:
            missing.append("ICANN_PASS")
        if not db_host:
            missing.append("DB_HOST")
        if not clickhouse_password:
            missing.append("CLICKHOUSE_PASSWORD")
            
        if missing:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        
        # Optional variables with defaults
        return cls(
            icann_user=icann_user,
            icann_pass=icann_pass,
            db_host=db_host,
            clickhouse_password=clickhouse_password,
            db_name=os.environ.get("DB_NAME", "icann"),
            db_port=int(os.environ.get("DB_PORT", "9000")),
            port=int(os.environ.get("PORT", "8080")),
            debug=os.environ.get("DEBUG", "").lower() in ("true", "1", "yes"),
            temp_dir=os.environ.get("TEMP_DIR", "/app/temp"),
            batch_size=int(os.environ.get("BATCH_SIZE", "10000")),
            max_retries=int(os.environ.get("MAX_RETRIES", "3")),
            cron_hour=int(os.environ.get("CRON_HOUR", "4")),
            cron_minute=int(os.environ.get("CRON_MINUTE", "0")),
        )
    
    @staticmethod
    def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
        """Get environment variable value.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            Environment variable value or default
        """
        return os.environ.get(key, default)
