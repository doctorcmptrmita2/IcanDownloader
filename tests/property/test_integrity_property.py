"""Property tests for file integrity verification.

Property 3: File Integrity Verification
For any downloaded zone file, the actual file size SHALL match 
the Content-Length header from the HTTP response.

Validates: Requirements 2.3
"""
import os
import tempfile
from hypothesis import given, strategies as st, settings

from src.services.czds_client import CZDSClient


class TestFileIntegrityVerification:
    """Property 3: File Integrity Verification"""
    
    @given(
        file_size=st.integers(min_value=1, max_value=10000),
        content=st.binary(min_size=1, max_size=10000),
    )
    @settings(max_examples=100)
    def test_verify_integrity_with_matching_size(self, file_size, content):
        """File integrity check SHALL pass when actual size matches expected.
        
        Feature: icann-downloader, Property 3: File Integrity Verification
        Validates: Requirements 2.3
        """
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            file_path = f.name
        
        try:
            actual_size = len(content)
            result = CZDSClient.verify_file_integrity(file_path, actual_size)
            
            assert result is True, \
                f"Integrity check should pass when sizes match"
        finally:
            os.unlink(file_path)
    
    @given(
        content=st.binary(min_size=1, max_size=10000),
        size_diff=st.integers(min_value=1, max_value=1000),
    )
    @settings(max_examples=100)
    def test_verify_integrity_with_mismatched_size(self, content, size_diff):
        """File integrity check SHALL fail when actual size differs from expected.
        
        Feature: icann-downloader, Property 3: File Integrity Verification
        Validates: Requirements 2.3
        """
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            file_path = f.name
        
        try:
            actual_size = len(content)
            wrong_expected_size = actual_size + size_diff
            
            result = CZDSClient.verify_file_integrity(file_path, wrong_expected_size)
            
            assert result is False, \
                f"Integrity check should fail when sizes don't match"
        finally:
            os.unlink(file_path)
    
    def test_verify_integrity_with_nonexistent_file(self):
        """File integrity check SHALL fail for non-existent files.
        
        Feature: icann-downloader, Property 3: File Integrity Verification
        Validates: Requirements 2.3
        """
        result = CZDSClient.verify_file_integrity("/nonexistent/path/file.zone", 1000)
        
        assert result is False, \
            "Integrity check should fail for non-existent files"
    
    @given(content=st.binary(min_size=0, max_size=10000))
    @settings(max_examples=100)
    def test_verify_integrity_returns_boolean(self, content):
        """File integrity check SHALL return a boolean value.
        
        Feature: icann-downloader, Property 3: File Integrity Verification
        Validates: Requirements 2.3
        """
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            file_path = f.name
        
        try:
            result = CZDSClient.verify_file_integrity(file_path, len(content))
            
            assert isinstance(result, bool), \
                f"verify_file_integrity should return bool, got {type(result)}"
        finally:
            os.unlink(file_path)
    
    @given(expected_size=st.integers(min_value=0, max_value=1000000))
    @settings(max_examples=100)
    def test_verify_integrity_with_zero_size_file(self, expected_size):
        """File integrity check SHALL correctly handle empty files.
        
        Feature: icann-downloader, Property 3: File Integrity Verification
        Validates: Requirements 2.3
        """
        with tempfile.NamedTemporaryFile(delete=False) as f:
            # Write nothing - empty file
            file_path = f.name
        
        try:
            result = CZDSClient.verify_file_integrity(file_path, expected_size)
            
            # Should only pass if expected_size is 0
            if expected_size == 0:
                assert result is True, \
                    "Empty file should pass integrity check when expected size is 0"
            else:
                assert result is False, \
                    "Empty file should fail integrity check when expected size > 0"
        finally:
            os.unlink(file_path)
