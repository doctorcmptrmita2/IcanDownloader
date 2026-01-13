"""ICANN CZDS API Client for zone file downloads."""
import os
import time
import logging
from datetime import datetime, date
from typing import List, Optional
from dataclasses import dataclass

import requests

from src.models import DownloadResult


logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Raised when authentication with CZDS API fails."""
    pass


class DownloadError(Exception):
    """Raised when zone file download fails."""
    pass


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0


class CZDSClient:
    """ICANN CZDS API client for authentication and zone file downloads."""
    
    CZDS_API_BASE = "https://czds-api.icann.org"
    AUTH_URL = "https://account-api.icann.org/api/authenticate"
    
    def __init__(
        self, 
        username: str, 
        password: str, 
        retry_config: Optional[RetryConfig] = None
    ):
        """Initialize client with ICANN credentials.
        
        Args:
            username: ICANN CZDS username
            password: ICANN CZDS password
            retry_config: Configuration for retry behavior
        """
        self.username = username
        self.password = password
        self.retry_config = retry_config or RetryConfig()
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._session = requests.Session()
    
    def authenticate(self) -> str:
        """Authenticate with CZDS API and return access token.
        
        Returns:
            Access token string
            
        Raises:
            AuthenticationError: On failure after retries
        """
        last_error = None
        
        for attempt in range(self.retry_config.max_retries):
            try:
                response = self._session.post(
                    self.AUTH_URL,
                    json={
                        "username": self.username,
                        "password": self.password,
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self._access_token = data.get("accessToken")
                    # Token typically expires in 1 hour
                    self._token_expiry = datetime.now()
                    logger.info("Successfully authenticated with CZDS API")
                    return self._access_token
                elif response.status_code == 401:
                    # Invalid credentials - don't retry
                    raise AuthenticationError("Invalid ICANN credentials")
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                else:
                    last_error = f"HTTP {response.status_code}: {response.text}"
                    
            except requests.exceptions.Timeout:
                last_error = "Request timeout"
            except requests.exceptions.RequestException as e:
                last_error = str(e)
            
            # Exponential backoff
            if attempt < self.retry_config.max_retries - 1:
                delay = self._calculate_backoff_delay(attempt)
                logger.warning(f"Auth attempt {attempt + 1} failed, retrying in {delay}s")
                time.sleep(delay)
        
        raise AuthenticationError(f"Authentication failed after {self.retry_config.max_retries} attempts: {last_error}")
    
    def _calculate_backoff_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay.
        
        Args:
            attempt: Current attempt number (0-indexed)
            
        Returns:
            Delay in seconds
        """
        delay = self.retry_config.base_delay * (2 ** attempt)
        return min(delay, self.retry_config.max_delay)
    
    def _refresh_token_if_needed(self) -> None:
        """Refresh access token if expired or not set."""
        if self._access_token is None:
            self.authenticate()
            return
        
        # Refresh if token is older than 50 minutes (tokens expire in 1 hour)
        if self._token_expiry:
            elapsed = (datetime.now() - self._token_expiry).total_seconds()
            if elapsed > 3000:  # 50 minutes
                logger.info("Token expired, refreshing...")
                self.authenticate()
    
    def _get_auth_headers(self) -> dict:
        """Get authorization headers for API requests."""
        self._refresh_token_if_needed()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
    
    def get_approved_tlds(self) -> List[str]:
        """Fetch list of approved TLDs for download.
        
        Returns:
            List of TLD names (e.g., ['com', 'net', 'org'])
            
        Raises:
            AuthenticationError: If not authenticated
        """
        self._refresh_token_if_needed()
        
        response = self._session.get(
            f"{self.CZDS_API_BASE}/czds/downloads/links",
            headers=self._get_auth_headers(),
            timeout=30,
        )
        
        if response.status_code == 200:
            links = response.json()
            # Extract TLD names from download URLs
            tlds = []
            for link in links:
                # URL format: https://czds-api.icann.org/czds/downloads/com.zone
                tld = link.split("/")[-1].replace(".zone", "")
                tlds.append(tld)
            logger.info(f"Found {len(tlds)} approved TLDs")
            return tlds
        elif response.status_code == 401:
            raise AuthenticationError("Token expired or invalid")
        else:
            raise DownloadError(f"Failed to get TLD list: HTTP {response.status_code}")
    
    def download_zone_file(self, tld: str, output_dir: str) -> DownloadResult:
        """Download zone file for specified TLD.
        
        Args:
            tld: Top-level domain to download
            output_dir: Directory to save the file
            
        Returns:
            DownloadResult with file_path, file_size, duration
            
        Raises:
            DownloadError: On failure after retries
        """
        self._refresh_token_if_needed()
        
        # Generate filename with date
        today = date.today()
        filename = self.generate_filename(tld, today)
        file_path = os.path.join(output_dir, filename)
        
        url = f"{self.CZDS_API_BASE}/czds/downloads/{tld}.zone"
        last_error = None
        start_time = time.time()
        
        for attempt in range(self.retry_config.max_retries):
            try:
                response = self._session.get(
                    url,
                    headers=self._get_auth_headers(),
                    stream=True,
                    timeout=300,  # 5 minutes for large files
                )
                
                if response.status_code == 200:
                    expected_size = int(response.headers.get("Content-Length", 0))
                    
                    # Download file
                    actual_size = 0
                    with open(file_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                actual_size += len(chunk)
                    
                    # Verify file integrity
                    if expected_size > 0 and actual_size != expected_size:
                        os.remove(file_path)
                        raise DownloadError(
                            f"File size mismatch: expected {expected_size}, got {actual_size}"
                        )
                    
                    duration = int(time.time() - start_time)
                    logger.info(f"Downloaded {tld}.zone ({actual_size} bytes) in {duration}s")
                    
                    return DownloadResult(
                        tld=tld,
                        file_path=file_path,
                        file_size=actual_size,
                        download_duration=duration,
                        status="success",
                    )
                    
                elif response.status_code == 404:
                    raise DownloadError(f"Zone file not found for TLD: {tld}")
                elif response.status_code == 401:
                    # Token might have expired, refresh and retry
                    self.authenticate()
                    continue
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                else:
                    last_error = f"HTTP {response.status_code}"
                    
            except requests.exceptions.Timeout:
                last_error = "Request timeout"
            except requests.exceptions.RequestException as e:
                last_error = str(e)
            except IOError as e:
                last_error = f"File write error: {e}"
            
            # Exponential backoff
            if attempt < self.retry_config.max_retries - 1:
                delay = self._calculate_backoff_delay(attempt)
                logger.warning(f"Download attempt {attempt + 1} failed, retrying in {delay}s")
                time.sleep(delay)
        
        duration = int(time.time() - start_time)
        return DownloadResult(
            tld=tld,
            file_path="",
            file_size=0,
            download_duration=duration,
            status="failed",
            error_message=f"Download failed after {self.retry_config.max_retries} attempts: {last_error}",
        )
    
    @staticmethod
    def generate_filename(tld: str, download_date: date) -> str:
        """Generate filename for zone file.
        
        Args:
            tld: Top-level domain
            download_date: Date of download
            
        Returns:
            Filename in format TLD_YYYYMMDD.zone.gz
        """
        date_str = download_date.strftime("%Y%m%d")
        return f"{tld}_{date_str}.zone.gz"
    
    @staticmethod
    def verify_file_integrity(file_path: str, expected_size: int) -> bool:
        """Verify downloaded file integrity.
        
        Args:
            file_path: Path to the downloaded file
            expected_size: Expected file size from Content-Length header
            
        Returns:
            True if file size matches expected size
        """
        if not os.path.exists(file_path):
            return False
        
        actual_size = os.path.getsize(file_path)
        return actual_size == expected_size
