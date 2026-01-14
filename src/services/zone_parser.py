"""Zone file parser for DNS zone files."""
import gc
import gzip
import os
import re
import tempfile
import time
import logging
from datetime import date
from typing import Generator, Optional, List, Tuple

from src.models import ZoneRecord


logger = logging.getLogger(__name__)


class ParseError(Exception):
    """Raised when zone file parsing fails."""
    pass


class ZoneParser:
    """Parser for DNS zone files."""
    
    # DNS record types we're interested in
    SUPPORTED_RECORD_TYPES = {'NS', 'A', 'AAAA', 'CNAME', 'MX', 'TXT', 'SOA'}
    
    # Regex pattern for zone file lines
    # Format: domain TTL class type rdata
    # Example: example.com. 3600 IN NS ns1.example.com.
    ZONE_LINE_PATTERN = re.compile(
        r'^(\S+)\s+(\d+)\s+IN\s+(\S+)\s+(.+)$',
        re.IGNORECASE
    )
    
    def __init__(self, tld: str, download_date: Optional[date] = None):
        """Initialize parser.
        
        Args:
            tld: Top-level domain being parsed
            download_date: Date of download (defaults to today)
        """
        self.tld = tld
        self.download_date = download_date or date.today()
        self.chunk_size = 50000  # Default chunk size
        self.chunk_delay = 0.1  # Default delay between chunks
        self.gc_interval = 5  # Run GC every N chunks
    
    def configure_chunking(
        self, 
        chunk_size: int = 50000, 
        chunk_delay: float = 0.1,
        gc_interval: int = 5
    ) -> None:
        """Configure chunking parameters.
        
        Args:
            chunk_size: Number of records per chunk
            chunk_delay: Delay in seconds between chunks
            gc_interval: Run garbage collection every N chunks
        """
        self.chunk_size = chunk_size
        self.chunk_delay = chunk_delay
        self.gc_interval = gc_interval
    
    def parse_zone_file(self, file_path: str) -> Generator[ZoneRecord, None, None]:
        """Parse gzipped zone file and yield DNS records.
        
        Args:
            file_path: Path to the gzipped zone file
            
        Yields:
            ZoneRecord objects for each valid DNS record
            
        Raises:
            ParseError: If file cannot be opened or decompressed
        """
        line_number = 0
        
        try:
            # Open gzipped file directly
            with gzip.open(file_path, 'rt', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line_number += 1
                    record = self._parse_line(line, line_number)
                    if record:
                        yield record
                        
        except gzip.BadGzipFile as e:
            raise ParseError(f"Invalid gzip file: {file_path}") from e
        except IOError as e:
            raise ParseError(f"Cannot read file {file_path}: {e}") from e
    
    def _parse_line(self, line: str, line_number: int) -> Optional[ZoneRecord]:
        """Parse single line from zone file.
        
        Args:
            line: Raw line from zone file
            line_number: Line number for error reporting
            
        Returns:
            ZoneRecord or None if line is comment/invalid
        """
        # Strip whitespace
        line = line.strip()
        
        # Skip empty lines and comments
        if not line or line.startswith(';') or line.startswith('$'):
            return None
        
        # Try to match zone file format
        match = self.ZONE_LINE_PATTERN.match(line)
        if not match:
            # Log warning but continue parsing
            logger.debug(f"Line {line_number}: Could not parse: {line[:100]}")
            return None
        
        domain_name, ttl_str, record_type, record_data = match.groups()
        
        # Normalize record type to uppercase
        record_type = record_type.upper()
        
        # Skip unsupported record types
        if record_type not in self.SUPPORTED_RECORD_TYPES:
            return None
        
        try:
            ttl = int(ttl_str)
        except ValueError:
            logger.warning(f"Line {line_number}: Invalid TTL value: {ttl_str}")
            return None
        
        # Clean up domain name (remove trailing dot)
        domain_name = domain_name.rstrip('.')
        
        # Clean up record data (remove trailing dot for domain names)
        record_data = record_data.strip().rstrip('.')
        
        return ZoneRecord(
            domain_name=domain_name,
            tld=self.tld,
            record_type=record_type,
            record_data=record_data,
            ttl=ttl,
            download_date=self.download_date,
        )
    
    def parse_line_simple(self, line: str) -> Optional[ZoneRecord]:
        """Parse a single line without line number tracking.
        
        Convenience method for testing.
        
        Args:
            line: Raw line from zone file
            
        Returns:
            ZoneRecord or None if line is invalid
        """
        return self._parse_line(line, 0)
    
    @staticmethod
    def decompress_file(gzip_path: str, output_dir: Optional[str] = None) -> str:
        """Decompress gzipped file to temporary location.
        
        Args:
            gzip_path: Path to gzipped file
            output_dir: Directory for output (uses temp dir if None)
            
        Returns:
            Path to decompressed file
            
        Raises:
            ParseError: If decompression fails
        """
        if output_dir is None:
            output_dir = tempfile.gettempdir()
        
        # Generate output filename
        base_name = os.path.basename(gzip_path)
        if base_name.endswith('.gz'):
            base_name = base_name[:-3]
        output_path = os.path.join(output_dir, base_name)
        
        try:
            with gzip.open(gzip_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    # Read and write in chunks to handle large files
                    while True:
                        chunk = f_in.read(8192)
                        if not chunk:
                            break
                        f_out.write(chunk)
            
            return output_path
            
        except gzip.BadGzipFile as e:
            raise ParseError(f"Invalid gzip file: {gzip_path}") from e
        except IOError as e:
            raise ParseError(f"Decompression failed: {e}") from e
    
    def count_records(self, file_path: str) -> int:
        """Count total records in zone file without storing them.
        
        Args:
            file_path: Path to the gzipped zone file
            
        Returns:
            Number of valid records
        """
        count = 0
        for _ in self.parse_zone_file(file_path):
            count += 1
        return count
    
    def parse_zone_file_chunked(
        self, 
        file_path: str
    ) -> Generator[Tuple[List[ZoneRecord], int], None, None]:
        """Parse gzipped zone file and yield chunks of DNS records.
        
        Memory-efficient parsing for large zone files. Yields batches of records
        with automatic garbage collection and optional delays between chunks.
        
        Args:
            file_path: Path to the gzipped zone file
            
        Yields:
            Tuple of (list of ZoneRecord objects, chunk number)
            
        Raises:
            ParseError: If file cannot be opened or decompressed
        """
        line_number = 0
        chunk: List[ZoneRecord] = []
        chunk_number = 0
        
        try:
            # Open gzipped file directly with streaming
            with gzip.open(file_path, 'rt', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line_number += 1
                    record = self._parse_line(line, line_number)
                    
                    if record:
                        chunk.append(record)
                        
                        # Yield chunk when full
                        if len(chunk) >= self.chunk_size:
                            chunk_number += 1
                            yield chunk, chunk_number
                            
                            # Clear chunk and run GC periodically
                            chunk = []
                            
                            if chunk_number % self.gc_interval == 0:
                                gc.collect()
                                logger.debug(f"GC run after chunk {chunk_number}")
                            
                            # Add delay between chunks to prevent memory pressure
                            if self.chunk_delay > 0:
                                time.sleep(self.chunk_delay)
                
                # Yield remaining records
                if chunk:
                    chunk_number += 1
                    yield chunk, chunk_number
                    
        except gzip.BadGzipFile as e:
            raise ParseError(f"Invalid gzip file: {file_path}") from e
        except IOError as e:
            raise ParseError(f"Cannot read file {file_path}: {e}") from e
        finally:
            # Final garbage collection
            gc.collect()
    
    def estimate_file_records(self, file_path: str) -> int:
        """Estimate number of records in file based on file size.
        
        Uses heuristic: ~100 bytes per record on average.
        
        Args:
            file_path: Path to the gzipped zone file
            
        Returns:
            Estimated number of records
        """
        try:
            file_size = os.path.getsize(file_path)
            # Gzip typically has 10:1 compression ratio for text
            # Average zone record is ~100 bytes uncompressed
            estimated_uncompressed = file_size * 10
            return estimated_uncompressed // 100
        except OSError:
            return 0
