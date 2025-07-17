"""
Timezone utility functions for database migrations.

This module provides timezone conversion functions to convert
datetime fields from Indian Standard Time (IST) to UTC
during the migration process.
"""

from datetime import datetime
from pytz import timezone, UTC


def convert_ist_to_utc(ist_dt):
    """Convert a datetime object from Indian Standard Time (IST) to UTC.
    
    If the datetime object is naive (i.e. no timezone info),
    it is assumed to be in IST (Asia/Kolkata) timezone.
    
    Args:
        ist_dt: A datetime object that is either naive (assumed IST) or timezone-aware
        
    Returns:
        datetime: UTC datetime object, or None if input is None/invalid
    """
    if not ist_dt:
        return None
    try:
        ist_tz = timezone("Asia/Kolkata")  # Indian Standard Time
        
        # If datetime is naive, localize it to IST timezone.
        if ist_dt.tzinfo is None:
            ist_dt = ist_tz.localize(ist_dt)
        
        # Convert to UTC
        dt_utc = ist_dt.astimezone(UTC)
        return dt_utc
    except Exception as e:
        print(f"Time conversion error for {ist_dt}: {e}")
        return None


def get_utc_now():
    """Get current UTC datetime.
    
    Returns:
        datetime: Current UTC datetime object
    """
    return datetime.now(UTC)


def ensure_utc_datetime(dt):
    """Ensure a datetime is in UTC timezone.
    
    Args:
        dt: datetime object to check/convert
        
    Returns:
        datetime: UTC datetime object, or None if input is None/invalid
    """
    if not dt:
        return None
    
    try:
        # If already UTC, return as-is
        if dt.tzinfo == UTC:
            return dt
        
        # If naive, assume it's IST timezone and convert
        if dt.tzinfo is None:
            return convert_ist_to_utc(dt)
        
        # If has other timezone, convert to UTC
        return dt.astimezone(UTC)
    except Exception as e:
        print(f"Error ensuring UTC datetime for {dt}: {e}")
        return None 