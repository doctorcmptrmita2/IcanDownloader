"""Zone Record data model."""
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class ZoneRecord:
    """Represents a DNS zone record parsed from a zone file.
    
    Attributes:
        domain_name: Full domain name (e.g., "example.com")
        tld: Top-level domain (e.g., "com", "net", "org")
        record_type: DNS record type (NS, A, AAAA, CNAME, MX, TXT, SOA)
        record_data: Record data (nameserver, IP address, etc.)
        ttl: Time to live in seconds
        download_date: Date when the zone file was downloaded
    """
    domain_name: str
    tld: str
    record_type: str
    record_data: str
    ttl: int
    download_date: date
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        return {
            "domain_name": self.domain_name,
            "tld": self.tld,
            "record_type": self.record_type,
            "record_data": self.record_data,
            "ttl": self.ttl,
            "download_date": self.download_date,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ZoneRecord":
        """Create ZoneRecord from dictionary."""
        return cls(
            domain_name=data["domain_name"],
            tld=data["tld"],
            record_type=data["record_type"],
            record_data=data["record_data"],
            ttl=data["ttl"],
            download_date=data["download_date"],
        )
