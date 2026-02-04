# logger.py
# Logging utility with consistent formatting

from datetime import datetime
from typing import Any, Optional

# Import production settings
try:
    from constants import PRODUCTION_MODE, LOG_LEVEL
except ImportError:
    PRODUCTION_MODE = True
    LOG_LEVEL = "INFO"

# Log level hierarchy
_LOG_LEVELS = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "SUCCESS": 1}

class Logger:
    """Simple logger for tracking operations"""
    
    _production_mode = PRODUCTION_MODE
    _log_level = LOG_LEVEL
    
    @classmethod
    def set_production_mode(cls, enabled: bool):
        """Enable/disable production mode"""
        cls._production_mode = enabled
    
    @classmethod
    def set_log_level(cls, level: str):
        """Set minimum log level"""
        cls._log_level = level.upper()
    
    @classmethod
    def _should_log(cls, level: str) -> bool:
        """Check if this level should be logged"""
        return _LOG_LEVELS.get(level, 1) >= _LOG_LEVELS.get(cls._log_level, 1)
    
    @staticmethod
    def info(message: str):
        if not Logger._should_log("INFO"):
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [INFO] {message}")
    
    @staticmethod
    def success(message: str):
        if not Logger._should_log("SUCCESS"):
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [SUCCESS] {message}")
    
    @staticmethod
    def error(message: str, error: Optional[Exception] = None):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [ERROR] {message}")
        if error:
            print(f"  Exception: {str(error)}")
    
    @staticmethod
    def warning(message: str):
        if not Logger._should_log("WARNING"):
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [WARNING] {message}")
    
    @staticmethod
    def debug(message: str, data: Optional[Any] = None):
        """Debug logging - skipped entirely in production mode"""
        if Logger._production_mode or not Logger._should_log("DEBUG"):
            return
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [DEBUG] {message}")
        if data:
            try:
                import json
                if isinstance(data, (dict, list)):
                    json_str = json.dumps(data, indent=2)
                    if len(json_str) > 500:
                        json_str = json_str[:500] + "... [truncated]"
                    print(f"  {json_str}")
                else:
                    print(f"  {data}")
            except:
                print(f"  {str(data)[:200]}")

# Create singleton instance
logger = Logger()
