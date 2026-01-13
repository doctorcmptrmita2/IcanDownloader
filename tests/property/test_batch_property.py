"""Property tests for batch insert size.

Property 5: Batch Insert Size
For any set of zone records being inserted, the system SHALL insert them 
in batches of exactly 10,000 records (or fewer for the final batch).

Validates: Requirements 3.3
"""
from datetime import date
from typing import List
from hypothesis import given, strategies as st, settings
from unittest.mock import MagicMock, patch, call

from src.models import ZoneRecord
from src.services.db_repository import ClickHouseRepository


# Strategy for generating zone records
@st.composite
def zone_record_strategy(draw):
    """Generate a valid ZoneRecord."""
    return ZoneRecord(
        domain_name=draw(st.text(min_size=1, max_size=50, alphabet=st.characters(
            whitelist_categories=('L', 'N'),
            whitelist_characters='.-'
        )).filter(lambda x: x.strip())),
        tld=draw(st.sampled_from(['com', 'net', 'org', 'io', 'dev'])),
        record_type=draw(st.sampled_from(['NS', 'A', 'AAAA', 'MX', 'TXT', 'CNAME'])),
        record_data=draw(st.text(min_size=1, max_size=100, alphabet=st.characters(
            whitelist_categories=('L', 'N'),
            whitelist_characters='.-:'
        )).filter(lambda x: x.strip())),
        ttl=draw(st.integers(min_value=60, max_value=86400)),
        download_date=date.today(),
    )


class TestBatchInsertSize:
    """Property 5: Batch Insert Size"""
    
    @given(
        num_records=st.integers(min_value=1, max_value=35000),
        batch_size=st.integers(min_value=100, max_value=15000),
    )
    @settings(max_examples=100)
    def test_batch_sizes_are_correct(self, num_records, batch_size):
        """For any number of records, batches SHALL be exactly batch_size or fewer for final.
        
        Feature: icann-downloader, Property 5: Batch Insert Size
        Validates: Requirements 3.3
        """
        # Create mock records
        records = [
            ZoneRecord(
                domain_name=f"domain{i}.com",
                tld="com",
                record_type="NS",
                record_data=f"ns{i}.example.com",
                ttl=3600,
                download_date=date.today(),
            )
            for i in range(num_records)
        ]
        
        # Track batch sizes
        batch_sizes = []
        
        # Create repository with mocked client
        repo = ClickHouseRepository(
            host="localhost",
            password="test",
            database="test",
        )
        
        # Mock the client.execute method
        mock_client = MagicMock()
        repo._client = mock_client
        
        # Capture batch sizes when execute is called
        def capture_batch(query, data=None):
            if data is not None:
                batch_sizes.append(len(data))
        
        mock_client.execute.side_effect = capture_batch
        
        # Insert records
        repo.insert_zone_records(records, batch_size=batch_size)
        
        # Verify batch sizes
        if num_records == 0:
            assert len(batch_sizes) == 0
        else:
            # All batches except possibly the last should be exactly batch_size
            for i, size in enumerate(batch_sizes[:-1]):
                assert size == batch_size, f"Batch {i} has size {size}, expected {batch_size}"
            
            # Last batch should be <= batch_size
            if batch_sizes:
                assert batch_sizes[-1] <= batch_size, \
                    f"Final batch has size {batch_sizes[-1]}, expected <= {batch_size}"
            
            # Total records should match
            assert sum(batch_sizes) == num_records, \
                f"Total records {sum(batch_sizes)} != expected {num_records}"
    
    @given(num_records=st.integers(min_value=1, max_value=25000))
    @settings(max_examples=100)
    def test_default_batch_size_is_10000(self, num_records):
        """Default batch size SHALL be 10,000 records.
        
        Feature: icann-downloader, Property 5: Batch Insert Size
        Validates: Requirements 3.3
        """
        # Create mock records
        records = [
            ZoneRecord(
                domain_name=f"domain{i}.com",
                tld="com",
                record_type="NS",
                record_data=f"ns{i}.example.com",
                ttl=3600,
                download_date=date.today(),
            )
            for i in range(num_records)
        ]
        
        batch_sizes = []
        
        repo = ClickHouseRepository(
            host="localhost",
            password="test",
            database="test",
        )
        
        mock_client = MagicMock()
        repo._client = mock_client
        
        def capture_batch(query, data=None):
            if data is not None:
                batch_sizes.append(len(data))
        
        mock_client.execute.side_effect = capture_batch
        
        # Insert with default batch size
        repo.insert_zone_records(records)
        
        # Verify default batch size is 10000
        default_batch_size = 10000
        
        for i, size in enumerate(batch_sizes[:-1]):
            assert size == default_batch_size, \
                f"Batch {i} has size {size}, expected default {default_batch_size}"
        
        if batch_sizes:
            assert batch_sizes[-1] <= default_batch_size
    
    def test_empty_records_list(self):
        """Empty records list SHALL result in no database calls.
        
        Feature: icann-downloader, Property 5: Batch Insert Size
        Validates: Requirements 3.3
        """
        repo = ClickHouseRepository(
            host="localhost",
            password="test",
            database="test",
        )
        
        mock_client = MagicMock()
        repo._client = mock_client
        
        result = repo.insert_zone_records([])
        
        assert result == 0
        mock_client.execute.assert_not_called()
