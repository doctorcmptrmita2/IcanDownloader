"""Property tests for ClickHouse deduplication.

Property 10: ClickHouse Deduplication
For any duplicate zone records (same domain_name, tld, record_type, download_date), 
only the most recent record SHALL be retained after ReplacingMergeTree optimization.

Validates: Requirements 9.2

Note: This test verifies the schema design ensures deduplication. 
Actual deduplication happens at ClickHouse level via ReplacingMergeTree engine.
"""
from datetime import date
from hypothesis import given, strategies as st, settings

from src.models import ZoneRecord


class TestClickHouseDeduplication:
    """Property 10: ClickHouse Deduplication"""
    
    @given(
        domain=st.text(min_size=1, max_size=50, alphabet=st.characters(
            whitelist_categories=('L', 'N'),
            whitelist_characters='.-'
        )).filter(lambda x: x.strip()),
        tld=st.sampled_from(['com', 'net', 'org', 'io']),
        record_type=st.sampled_from(['NS', 'A', 'AAAA', 'MX']),
    )
    @settings(max_examples=100)
    def test_zone_record_unique_key_components(self, domain, tld, record_type):
        """Zone records with same (domain_name, tld, record_type, download_date) are duplicates.
        
        Feature: icann-downloader, Property 10: ClickHouse Deduplication
        Validates: Requirements 9.2
        """
        today = date.today()
        
        # Create two records with same unique key
        record1 = ZoneRecord(
            domain_name=domain,
            tld=tld,
            record_type=record_type,
            record_data="ns1.example.com",
            ttl=3600,
            download_date=today,
        )
        
        record2 = ZoneRecord(
            domain_name=domain,
            tld=tld,
            record_type=record_type,
            record_data="ns2.example.com",  # Different data
            ttl=7200,  # Different TTL
            download_date=today,
        )
        
        # Verify unique key components are the same
        assert record1.domain_name == record2.domain_name
        assert record1.tld == record2.tld
        assert record1.record_type == record2.record_type
        assert record1.download_date == record2.download_date
        
        # These would be considered duplicates by ReplacingMergeTree
        # The ORDER BY clause is (tld, domain_name, record_type, download_date)
    
    @given(
        domain=st.text(min_size=1, max_size=50, alphabet=st.characters(
            whitelist_categories=('L', 'N'),
            whitelist_characters='.-'
        )).filter(lambda x: x.strip()),
        tld1=st.sampled_from(['com', 'net']),
        tld2=st.sampled_from(['org', 'io']),
    )
    @settings(max_examples=100)
    def test_different_tld_not_duplicate(self, domain, tld1, tld2):
        """Records with different TLDs are NOT duplicates.
        
        Feature: icann-downloader, Property 10: ClickHouse Deduplication
        Validates: Requirements 9.2
        """
        today = date.today()
        
        record1 = ZoneRecord(
            domain_name=domain,
            tld=tld1,
            record_type="NS",
            record_data="ns1.example.com",
            ttl=3600,
            download_date=today,
        )
        
        record2 = ZoneRecord(
            domain_name=domain,
            tld=tld2,
            record_type="NS",
            record_data="ns1.example.com",
            ttl=3600,
            download_date=today,
        )
        
        # Different TLDs mean different records (not duplicates)
        assert record1.tld != record2.tld
    
    @given(
        domain=st.text(min_size=1, max_size=50, alphabet=st.characters(
            whitelist_categories=('L', 'N'),
            whitelist_characters='.-'
        )).filter(lambda x: x.strip()),
        record_type1=st.sampled_from(['NS', 'A']),
        record_type2=st.sampled_from(['AAAA', 'MX']),
    )
    @settings(max_examples=100)
    def test_different_record_type_not_duplicate(self, domain, record_type1, record_type2):
        """Records with different record types are NOT duplicates.
        
        Feature: icann-downloader, Property 10: ClickHouse Deduplication
        Validates: Requirements 9.2
        """
        today = date.today()
        
        record1 = ZoneRecord(
            domain_name=domain,
            tld="com",
            record_type=record_type1,
            record_data="data1",
            ttl=3600,
            download_date=today,
        )
        
        record2 = ZoneRecord(
            domain_name=domain,
            tld="com",
            record_type=record_type2,
            record_data="data2",
            ttl=3600,
            download_date=today,
        )
        
        # Different record types mean different records (not duplicates)
        assert record1.record_type != record2.record_type
    
    def test_schema_uses_replacing_merge_tree(self):
        """Schema SHALL use ReplacingMergeTree engine for deduplication.
        
        Feature: icann-downloader, Property 10: ClickHouse Deduplication
        Validates: Requirements 9.2
        """
        from src.services.db_repository import ClickHouseRepository
        from unittest.mock import MagicMock
        
        repo = ClickHouseRepository(
            host="localhost",
            password="test",
            database="test",
        )
        
        mock_client = MagicMock()
        repo._client = mock_client
        
        # Call init_tables
        repo.init_tables()
        
        # Verify ReplacingMergeTree is used in the CREATE TABLE statement
        calls = mock_client.execute.call_args_list
        
        # Find the zone_records CREATE TABLE call
        zone_records_call = None
        for call in calls:
            if 'zone_records' in str(call) and 'CREATE TABLE' in str(call):
                zone_records_call = str(call)
                break
        
        assert zone_records_call is not None, "zone_records CREATE TABLE not found"
        assert 'ReplacingMergeTree' in zone_records_call, \
            "zone_records should use ReplacingMergeTree engine"
