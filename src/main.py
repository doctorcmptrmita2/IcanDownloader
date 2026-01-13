"""Main entry point for ICANN Downloader application."""
import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def create_services(config):
    """Create and wire all services.
    
    Args:
        config: Application configuration
        
    Returns:
        Dictionary of service instances
    """
    from src.services.db_repository import ClickHouseRepository
    from src.services.czds_client import CZDSClient
    from src.services.zone_parser import ZoneParser
    from src.services.logger_service import LoggerService
    from src.services.download_service import DownloadService
    from src.services.scheduler_service import SchedulerService
    
    # Create repository
    logger.info(f"Connecting to ClickHouse at {config.db_host}:{config.db_port}")
    repository = ClickHouseRepository(
        host=config.db_host,
        password=config.clickhouse_password,
        database=config.db_name,
        port=config.db_port,
    )
    
    # Initialize database tables
    logger.info("Initializing database tables")
    repository.init_tables()
    
    # Create CZDS client
    czds_client = CZDSClient(
        username=config.icann_user,
        password=config.icann_pass,
        max_retries=config.max_retries,
    )
    
    # Create logger service (socketio will be set later)
    logger_service = LoggerService(socketio=None)
    
    # Create parser factory
    def parser_factory(tld: str) -> ZoneParser:
        return ZoneParser(tld=tld)
    
    # Create download service
    download_service = DownloadService(
        czds_client=czds_client,
        parser_factory=parser_factory,
        repository=repository,
        logger_service=logger_service,
        temp_dir=config.temp_dir,
        batch_size=config.batch_size,
    )
    
    # Create scheduler service
    scheduler_service = SchedulerService(
        download_callback=download_service.run_full_download,
        repository=repository,
        cron_hour=config.cron_hour,
        cron_minute=config.cron_minute,
    )
    
    return {
        'repository': repository,
        'czds_client': czds_client,
        'logger_service': logger_service,
        'download_service': download_service,
        'scheduler_service': scheduler_service,
    }


def main():
    """Main entry point."""
    from src.config import Config, ConfigurationError
    from src.api.app import create_app
    
    logger.info("Starting ICANN Downloader")
    
    # Load configuration
    try:
        config = Config.from_env()
        logger.info("Configuration loaded successfully")
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    # Create temp directory
    os.makedirs(config.temp_dir, exist_ok=True)
    
    # Create services
    try:
        services = create_services(config)
        logger.info("Services initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        sys.exit(1)
    
    # Create Flask app
    app, socketio = create_app(
        config=config,
        download_service=services['download_service'],
        scheduler_service=services['scheduler_service'],
        logger_service=services['logger_service'],
        repository=services['repository'],
    )
    
    # Update logger service with socketio
    services['logger_service'].socketio = socketio
    
    # Start scheduler
    logger.info("Starting scheduler")
    services['scheduler_service'].start()
    
    # Log startup complete
    services['logger_service'].log(
        "INFO",
        f"ICANN Downloader started on port {config.port}",
        operation_type="startup",
    )
    
    # Run Flask app
    logger.info(f"Starting web server on port {config.port}")
    try:
        socketio.run(
            app,
            host='0.0.0.0',
            port=config.port,
            debug=config.debug,
            allow_unsafe_werkzeug=True,
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        services['scheduler_service'].stop()
        logger.info("Shutdown complete")


if __name__ == '__main__':
    main()
