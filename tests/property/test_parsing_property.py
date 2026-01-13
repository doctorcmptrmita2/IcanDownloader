"""Property tests for zone record parsing correctness.

Property 4: Zone Record Parsing Correctness
For any valid DNS zone file line in format `domain TTL class type rdata`, 
the parser SHALL extract domain_name, ttl, record_type, and record_data correctly.

Validates: Requirements 3.2
"""
from datetime import date
from hypothesis import given, strategies as st, settings

from src.services.zone_parser import ZoneParser


# Strategy for valid domain names (no trailing dots)
domain_strategy = st.text(
    min_size=1,
    max_size=50,
    alphabet='abcdefghijklmnopqrstuvwxyz0123456789-'
).filter(lambda x: x.strip() and not x.startswith('-') and not x.endswith('-'))

# Strategy for valid TTL values
ttl_strategy = st.integers(min_value=60, max_value=86400)

# Strategy for record types
record_type_strategy = st.sampled_from(['NS', 'A', 'AAAA', 'MX', 'TXT', 'CNAME'])

# Strategy for record data
record_data_strategy = st.text(
    min_size=1,
    max_size=100,
    alphabet='abcdefghijklmnopqrstuvwxyz0123456789-.:@'
).filter(lambda x: x.strip())


class TestZoneRecordParsingCorrectness:
    """Property 4: Zone Record Parsing Correctness"""
    
    @given(
        domain=domain_strategy,
        ttl=ttl_strategy,
        record_type=record_type_strategy,
        record_data=record_data_strategy,
    )
    @settings(max_examples=100)
    def test_parser_extracts_domain_name(self, domain, ttl, record_type, record_data):
        """Parser SHALL correctly extract domain_name from zone line.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        # Construct valid zone line
        line = f"{domain}. {ttl} IN {record_type} {record_data}"
        
        parser = ZoneParser(tld="com")
        record = parser.parse_line_simple(line)
        
        assert record is not None, f"Failed to parse: {line}"
        assert record.domain_name == domain, \
            f"Domain mismatch: expected '{domain}', got '{record.domain_name}'"
    
    @given(
        domain=domain_strategy,
        ttl=ttl_strategy,
        record_type=record_type_strategy,
        record_data=record_data_strategy,
    )
    @settings(max_examples=100)
    def test_parser_extracts_ttl(self, domain, ttl, record_type, record_data):
        """Parser SHALL correctly extract TTL from zone line.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        line = f"{domain}. {ttl} IN {record_type} {record_data}"
        
        parser = ZoneParser(tld="com")
        record = parser.parse_line_simple(line)
        
        assert record is not None, f"Failed to parse: {line}"
        assert record.ttl == ttl, \
            f"TTL mismatch: expected {ttl}, got {record.ttl}"
    
    @given(
        domain=domain_strategy,
        ttl=ttl_strategy,
        record_type=record_type_strategy,
        record_data=record_data_strategy,
    )
    @settings(max_examples=100)
    def test_parser_extracts_record_type(self, domain, ttl, record_type, record_data):
        """Parser SHALL correctly extract record_type from zone line.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        line = f"{domain}. {ttl} IN {record_type} {record_data}"
        
        parser = ZoneParser(tld="com")
        record = parser.parse_line_simple(line)
        
        assert record is not None, f"Failed to parse: {line}"
        assert record.record_type == record_type.upper(), \
            f"Record type mismatch: expected '{record_type}', got '{record.record_type}'"
    
    @given(
        domain=domain_strategy,
        ttl=ttl_strategy,
        record_type=record_type_strategy,
        record_data=record_data_strategy,
    )
    @settings(max_examples=100)
    def test_parser_extracts_record_data(self, domain, ttl, record_type, record_data):
        """Parser SHALL correctly extract record_data from zone line.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        line = f"{domain}. {ttl} IN {record_type} {record_data}"
        
        parser = ZoneParser(tld="com")
        record = parser.parse_line_simple(line)
        
        assert record is not None, f"Failed to parse: {line}"
        # Record data might have trailing dot removed
        expected_data = record_data.rstrip('.')
        assert record.record_data == expected_data, \
            f"Record data mismatch: expected '{expected_data}', got '{record.record_data}'"
    
    @given(tld=st.sampled_from(['com', 'net', 'org', 'io']))
    @settings(max_examples=10)
    def test_parser_uses_provided_tld(self, tld):
        """Parser SHALL use the TLD provided during initialization.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        line = "example.com. 3600 IN NS ns1.example.com"
        
        parser = ZoneParser(tld=tld)
        record = parser.parse_line_simple(line)
        
        assert record is not None
        assert record.tld == tld, \
            f"TLD mismatch: expected '{tld}', got '{record.tld}'"
    
    def test_parser_skips_comments(self):
        """Parser SHALL skip comment lines starting with ';'.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        parser = ZoneParser(tld="com")
        
        comment_lines = [
            "; This is a comment",
            ";; Another comment",
            "   ; Indented comment",
        ]
        
        for line in comment_lines:
            record = parser.parse_line_simple(line)
            assert record is None, f"Comment line should be skipped: {line}"
    
    def test_parser_skips_directives(self):
        """Parser SHALL skip directive lines starting with '$'.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        parser = ZoneParser(tld="com")
        
        directive_lines = [
            "$ORIGIN example.com.",
            "$TTL 3600",
        ]
        
        for line in directive_lines:
            record = parser.parse_line_simple(line)
            assert record is None, f"Directive line should be skipped: {line}"
    
    def test_parser_skips_empty_lines(self):
        """Parser SHALL skip empty lines.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        parser = ZoneParser(tld="com")
        
        empty_lines = ["", "   ", "\t", "\n"]
        
        for line in empty_lines:
            record = parser.parse_line_simple(line)
            assert record is None, f"Empty line should be skipped"
    
    @given(record_type=st.sampled_from(['NS', 'A', 'AAAA', 'MX', 'TXT', 'CNAME', 'SOA']))
    @settings(max_examples=7)
    def test_parser_handles_supported_record_types(self, record_type):
        """Parser SHALL handle all supported record types.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        line = f"example.com. 3600 IN {record_type} data.example.com"
        
        parser = ZoneParser(tld="com")
        record = parser.parse_line_simple(line)
        
        assert record is not None, f"Should parse {record_type} record"
        assert record.record_type == record_type
    
    def test_parser_sets_download_date(self):
        """Parser SHALL set download_date on parsed records.
        
        Feature: icann-downloader, Property 4: Zone Record Parsing Correctness
        Validates: Requirements 3.2
        """
        test_date = date(2024, 1, 15)
        parser = ZoneParser(tld="com", download_date=test_date)
        
        line = "example.com. 3600 IN NS ns1.example.com"
        record = parser.parse_line_simple(line)
        
        assert record is not None
        assert record.download_date == test_date
