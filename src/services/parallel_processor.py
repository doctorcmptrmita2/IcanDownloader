"""Parallel processing for zone file downloads and parsing."""
import gc
import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, Tuple, Callable
from queue import Queue
import multiprocessing as mp

from src.models import ZoneRecord, DownloadResult, DownloadLog
from src.services.zone_parser import ZoneParser


logger = logging.getLogger(__name__)


@dataclass
class ParallelConfig:
    """Configuration for parallel processing."""
    # Number of parallel TLD downloads
    download_workers: int = 4
    # Number of parallel chunk processors per TLD
    parse_workers: int = 8
    # Chunk size for parallel parsing
    chunk_size: int = 100000
    # Queue size for chunk processing
    queue_size: int = 20
    # DB insert batch size
    batch_size: int = 100000


class ChunkProcessor:
    """Processes chunks of zone records in parallel."""
    
    def __init__(
        self,
        db_factory: Callable,
        logger_service,
        num_workers: int = 8,
        batch_size: int = 100000,
    ):
        """Initialize chunk processor.
        
        Args:
            db_factory: Factory function to create DB repository instances
            logger_service: Logger service for status updates
            num_workers: Number of parallel workers
            batch_size: Batch size for DB inserts
        """
        self.db_factory = db_factory
        self.logger_service = logger_service
        self.num_workers = num_workers
        self.batch_size = batch_size
        self._stop_event = threading.Event()
    
    def process_chunks_parallel(
        self,
        tld: str,
        file_path: str,
        chunk_size: int = 100000,
    ) -> Tuple[int, int]:
        """Process zone file chunks in parallel.
        
        Uses producer-consumer pattern:
        - 1 producer thread reads and parses file into chunks
        - N consumer threads insert chunks into database
        
        Args:
            tld: TLD being processed
            file_path: Path to zone file
            chunk_size: Records per chunk
            
        Returns:
            Tuple of (total_records, duration_seconds)
        """
        start_time = time.time()
        chunk_queue: Queue = Queue(maxsize=self.num_workers * 2)
        total_records = [0]  # Use list for mutable reference
        chunks_processed = [0]
        errors = []
        lock = threading.Lock()
        
        self.logger_service.log(
            "INFO",
            f"🚀 [{tld}] Paralel işlem başlıyor: {self.num_workers} worker",
            operation_type="parse",
            tld=tld,
        )
        
        def producer():
            """Read file and produce chunks."""
            parser = ZoneParser(tld)
            parser.configure_chunking(chunk_size=chunk_size, chunk_delay=0, gc_interval=10)
            
            try:
                for chunk, chunk_num in parser.parse_zone_file_chunked(file_path):
                    if self._stop_event.is_set():
                        break
                    chunk_queue.put((chunk, chunk_num))
                
                # Signal end of chunks
                for _ in range(self.num_workers):
                    chunk_queue.put((None, -1))
                    
            except Exception as e:
                logger.error(f"Producer error: {e}")
                errors.append(str(e))
                # Signal workers to stop
                for _ in range(self.num_workers):
                    chunk_queue.put((None, -1))
        
        def consumer(worker_id: int):
            """Consume and insert chunks."""
            # Each worker gets its own DB connection
            db = self.db_factory()
            
            try:
                while not self._stop_event.is_set():
                    item = chunk_queue.get()
                    chunk, chunk_num = item
                    
                    if chunk is None:
                        break
                    
                    # Insert chunk with retry
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            db.insert_zone_records(chunk, self.batch_size)
                            break
                        except Exception as e:
                            if attempt < max_retries - 1:
                                time.sleep(1 + attempt)
                            else:
                                logger.error(f"Worker {worker_id} chunk {chunk_num} failed: {e}")
                                errors.append(f"Chunk {chunk_num}: {str(e)[:100]}")
                    
                    with lock:
                        total_records[0] += len(chunk)
                        chunks_processed[0] += 1
                        
                        # Log progress every 10 chunks per worker
                        if chunks_processed[0] % (self.num_workers * 10) == 0:
                            elapsed = time.time() - start_time
                            rate = total_records[0] / elapsed if elapsed > 0 else 0
                            self.logger_service.log(
                                "INFO",
                                f"📊 [{tld}] {total_records[0]:,} kayıt | {rate:,.0f} rec/s | {chunks_processed[0]} chunk",
                                operation_type="parse",
                                tld=tld,
                            )
                    
                    del chunk
                    chunk_queue.task_done()
                    
            except Exception as e:
                logger.error(f"Consumer {worker_id} error: {e}")
                errors.append(str(e))
            finally:
                try:
                    db.close()
                except:
                    pass
        
        # Start producer thread
        producer_thread = threading.Thread(target=producer, name=f"producer-{tld}")
        producer_thread.start()
        
        # Start consumer threads
        consumer_threads = []
        for i in range(self.num_workers):
            t = threading.Thread(target=consumer, args=(i,), name=f"consumer-{tld}-{i}")
            t.start()
            consumer_threads.append(t)
        
        # Wait for completion
        producer_thread.join()
        for t in consumer_threads:
            t.join()
        
        duration = int(time.time() - start_time)
        
        if errors:
            self.logger_service.log(
                "WARNING",
                f"⚠️ [{tld}] {len(errors)} hata oluştu: {errors[0][:100]}",
                operation_type="parse",
                tld=tld,
            )
        
        self.logger_service.log(
            "INFO",
            f"✅ [{tld}] Paralel işlem tamamlandı: {total_records[0]:,} kayıt | {duration}s | {total_records[0]/duration:,.0f} rec/s",
            operation_type="parse",
            tld=tld,
        )
        
        gc.collect()
        return total_records[0], duration
    
    def stop(self):
        """Signal all workers to stop."""
        self._stop_event.set()


class ParallelDownloadService:
    """Service for parallel TLD downloads."""
    
    def __init__(
        self,
        czds_client,
        db_factory: Callable,
        logger_service,
        temp_dir: str = "/app/temp",
        config: Optional[ParallelConfig] = None,
    ):
        """Initialize parallel download service.
        
        Args:
            czds_client: CZDS API client
            db_factory: Factory to create DB repository instances
            logger_service: Logger service
            temp_dir: Temporary directory for downloads
            config: Parallel processing configuration
        """
        self.czds_client = czds_client
        self.db_factory = db_factory
        self.logger_service = logger_service
        self.temp_dir = temp_dir
        self.config = config or ParallelConfig()
        
        self._is_running = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
    
    def download_tlds_parallel(
        self,
        tlds: List[str],
        max_workers: Optional[int] = None,
    ) -> dict:
        """Download multiple TLDs in parallel.
        
        Args:
            tlds: List of TLDs to download
            max_workers: Override number of parallel downloads
            
        Returns:
            Summary dict with results
        """
        workers = max_workers or self.config.download_workers
        start_time = datetime.now()
        
        results = {
            "total": len(tlds),
            "successful": 0,
            "failed": 0,
            "total_records": 0,
            "tld_results": {},
        }
        
        self.logger_service.log(
            "INFO",
            f"🚀 {len(tlds)} TLD paralel indirme başlıyor ({workers} worker)",
            operation_type="download",
        )
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._download_single_tld, tld): tld
                for tld in tlds
            }
            
            for future in as_completed(futures):
                if self._stop_event.is_set():
                    break
                    
                tld = futures[future]
                try:
                    result = future.result()
                    results["tld_results"][tld] = result
                    
                    if result["success"]:
                        results["successful"] += 1
                        results["total_records"] += result["records"]
                    else:
                        results["failed"] += 1
                        
                except Exception as e:
                    results["failed"] += 1
                    results["tld_results"][tld] = {
                        "success": False,
                        "error": str(e),
                        "records": 0,
                    }
        
        duration = int((datetime.now() - start_time).total_seconds())
        results["duration"] = duration
        
        self.logger_service.log(
            "INFO",
            f"🎉 Paralel indirme tamamlandı: {results['successful']}/{results['total']} başarılı | "
            f"{results['total_records']:,} kayıt | {duration}s",
            operation_type="download",
        )
        
        return results
    
    def _download_single_tld(self, tld: str) -> dict:
        """Download and process single TLD with parallel chunk processing.
        
        Args:
            tld: TLD to download
            
        Returns:
            Result dict
        """
        result = {
            "success": False,
            "records": 0,
            "duration": 0,
            "error": None,
        }
        
        try:
            # NOT: Eski kayıtları silmiyoruz - tarihsel veri karşılaştırması için
            # (Dropped domains tespiti için gerekli)
            
            # Download zone file
            os.makedirs(self.temp_dir, exist_ok=True)
            download_result = self.czds_client.download_zone_file(tld, self.temp_dir)
            
            if not download_result.is_success:
                result["error"] = download_result.error_message
                return result
            
            self.logger_service.log(
                "INFO",
                f"📥 [{tld}] İndirildi: {download_result.file_size / 1024 / 1024:.1f} MB",
                operation_type="download",
                tld=tld,
            )
            
            # Process with parallel chunk processor
            processor = ChunkProcessor(
                db_factory=self.db_factory,
                logger_service=self.logger_service,
                num_workers=self.config.parse_workers,
                batch_size=self.config.batch_size,
            )
            
            total_records, duration = processor.process_chunks_parallel(
                tld=tld,
                file_path=download_result.file_path,
                chunk_size=self.config.chunk_size,
            )
            
            result["success"] = True
            result["records"] = total_records
            result["duration"] = duration
            
            # Clean up file
            try:
                os.remove(download_result.file_path)
            except:
                pass
            
            # Log to database
            self._log_to_db(tld, download_result, total_records, duration)
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Failed to process {tld}: {e}")
        
        return result
    
    def _log_to_db(
        self,
        tld: str,
        download_result: DownloadResult,
        records: int,
        parse_duration: int,
    ):
        """Log download result to database."""
        try:
            db = self.db_factory()
            try:
                log = DownloadLog(
                    tld=tld,
                    file_size=download_result.file_size,
                    records_count=records,
                    download_duration=download_result.download_duration,
                    parse_duration=parse_duration,
                    status="success" if records > 0 else "failed",
                    error_message=download_result.error_message,
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                )
                db.log_download(log)
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"Failed to log download: {e}")
    
    def stop(self):
        """Stop all parallel operations."""
        self._stop_event.set()
