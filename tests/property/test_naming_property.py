"""Property tests for zone file naming convention.

Property 1: Zone File Naming Convention
For any TLD name and download date, the saved zone file SHALL follow 
the naming pattern {TLD}_{YYYYMMDD}.zone.gz.

Validates: Requirements 2.2
"""
import re
from datetime import date
from hypothesis import given, strategies as st, settings

from src.services.czds_client import CZDSClient


# Strategy for valid TLD names (ASCII letters and hyphens only)
tld_strategy = st.text(
    min_size=2, 
    max_size=20, 
    alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-'
).filter(lambda x: x.strip() and not x.startswith('-') and not x.endswith('-') and '--' not in x)

# Strategy for valid dates
date_strategy = st.dates(
    min_value=date(2000, 1, 1),
    max_value=date(2100, 12, 31)
)


class TestZoneFileNamingConvention:
    """Property 1: Zone File Naming Convention"""
    
    @given(tld=tld_strategy, download_date=date_strategy)
    @settings(max_examples=100)
    def test_filename_follows_pattern(self, tld, download_date):
        """For any TLD and date, filename SHALL follow {TLD}_{YYYYMMDD}.zone.gz pattern.
        
        Feature: icann-downloader, Property 1: Zone File Naming Convention
        Validates: Requirements 2.2
        """
        filename = CZDSClient.generate_filename(tld, download_date)
        
        # Verify pattern: TLD_YYYYMMDD.zone.gz
        pattern = r'^[a-zA-Z\-]+_\d{8}\.zone\.gz$'
        assert re.match(pattern, filename), f"Filename '{filename}' doesn't match pattern"
    
    @given(tld=tld_strategy, download_date=date_strategy)
    @settings(max_examples=100)
    def test_filename_contains_tld(self, tld, download_date):
        """Filename SHALL contain the TLD name.
        
        Feature: icann-downloader, Property 1: Zone File Naming Convention
        Validates: Requirements 2.2
        """
        filename = CZDSClient.generate_filename(tld, download_date)
        
        # TLD should be at the start of filename
        assert filename.startswith(f"{tld}_"), f"Filename should start with '{tld}_'"
    
    @given(tld=tld_strategy, download_date=date_strategy)
    @settings(max_examples=100)
    def test_filename_contains_correct_date(self, tld, download_date):
        """Filename SHALL contain the date in YYYYMMDD format.
        
        Feature: icann-downloader, Property 1: Zone File Naming Convention
        Validates: Requirements 2.2
        """
        filename = CZDSClient.generate_filename(tld, download_date)
        
        # Extract date from filename
        expected_date_str = download_date.strftime("%Y%m%d")
        assert expected_date_str in filename, \
            f"Filename should contain date '{expected_date_str}'"
    
    @given(tld=tld_strategy, download_date=date_strategy)
    @settings(max_examples=100)
    def test_filename_has_correct_extension(self, tld, download_date):
        """Filename SHALL end with .zone.gz extension.
        
        Feature: icann-downloader, Property 1: Zone File Naming Convention
        Validates: Requirements 2.2
        """
        filename = CZDSClient.generate_filename(tld, download_date)
        
        assert filename.endswith(".zone.gz"), \
            f"Filename should end with '.zone.gz', got '{filename}'"
    
    @given(
        tld=tld_strategy,
        date1=date_strategy,
        date2=date_strategy,
    )
    @settings(max_examples=100)
    def test_different_dates_produce_different_filenames(self, tld, date1, date2):
        """Different dates SHALL produce different filenames for same TLD.
        
        Feature: icann-downloader, Property 1: Zone File Naming Convention
        Validates: Requirements 2.2
        """
        if date1 == date2:
            return  # Skip if dates are the same
        
        filename1 = CZDSClient.generate_filename(tld, date1)
        filename2 = CZDSClient.generate_filename(tld, date2)
        
        assert filename1 != filename2, \
            f"Different dates should produce different filenames"
    
    @given(
        tld1=tld_strategy,
        tld2=tld_strategy,
        download_date=date_strategy,
    )
    @settings(max_examples=100)
    def test_different_tlds_produce_different_filenames(self, tld1, tld2, download_date):
        """Different TLDs SHALL produce different filenames for same date.
        
        Feature: icann-downloader, Property 1: Zone File Naming Convention
        Validates: Requirements 2.2
        """
        if tld1 == tld2:
            return  # Skip if TLDs are the same
        
        filename1 = CZDSClient.generate_filename(tld1, download_date)
        filename2 = CZDSClient.generate_filename(tld2, download_date)
        
        assert filename1 != filename2, \
            f"Different TLDs should produce different filenames"
