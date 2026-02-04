# config.py
# Configuration loader for Waites Customer Sync Service
# All secrets should be set as environment variables in Railway

import os
from typing import Optional

# Load .env file in development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not required in production

# Debug: Print available environment variables on startup
print("=" * 50)
print("CUSTOMER SYNC - ENVIRONMENT VARIABLE DEBUG:")
print(f"  HUBSPOT_PAT set: {'Yes' if os.getenv('HUBSPOT_PAT') else 'NO'}")
print(f"  NETSUITE_ACCOUNT_ID set: {'Yes' if os.getenv('NETSUITE_ACCOUNT_ID') else 'NO'}")
print(f"  REDIS_URL set: {'Yes' if os.getenv('REDIS_URL') else 'NO'}")
print(f"  HUBDB_TABLE_ID set: {'Yes' if os.getenv('HUBDB_TABLE_ID') else 'NO'}")
print("=" * 50)


def get_env(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """Get environment variable with optional default and required flag"""
    value = os.getenv(key, default)
    if required and not value:
        print(f"ERROR: Required variable '{key}' is missing!")
        raise ValueError(f"Required environment variable '{key}' is not set.")
    return value


def get_bool_env(key: str, default: bool = False) -> bool:
    """Get boolean environment variable"""
    value = os.getenv(key, str(default)).lower()
    return value in ('true', '1', 'yes', 'on')


def get_int_env(key: str, default: int = 0) -> int:
    """Get integer environment variable"""
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


class Config:
    """Application configuration loaded from environment variables"""
    
    # ==========================================================================
    # HubSpot Configuration
    # ==========================================================================
    HUBSPOT_PAT = get_env('HUBSPOT_PAT', required=True)
    HUBSPOT_PORTAL_ID = get_env('HUBSPOT_PORTAL_ID', '48162723')
    HUBSPOT_BASE_URL = get_env('HUBSPOT_BASE_URL', 'https://api.hubapi.com')
    HUBSPOT_FACILITY_OBJECT_TYPE_ID = get_env('HUBSPOT_FACILITY_OBJECT_TYPE_ID', '2-43136116')
    HUBSPOT_QUOTE_DOMAIN = get_env('HUBSPOT_QUOTE_DOMAIN', 'waites.net')
    
    # ==========================================================================
    # NetSuite Configuration (Token-Based Authentication)
    # ==========================================================================
    NETSUITE_ACCOUNT_ID = get_env('NETSUITE_ACCOUNT_ID', required=True)
    NETSUITE_CONSUMER_KEY = get_env('NETSUITE_CONSUMER_KEY', required=True)
    NETSUITE_CONSUMER_SECRET = get_env('NETSUITE_CONSUMER_SECRET', required=True)
    NETSUITE_TOKEN_ID = get_env('NETSUITE_TOKEN_ID', required=True)
    NETSUITE_TOKEN_SECRET = get_env('NETSUITE_TOKEN_SECRET', required=True)
    
    @classmethod
    def get_netsuite_rest_url(cls) -> str:
        """Get the NetSuite REST API base URL"""
        account = cls.NETSUITE_ACCOUNT_ID.lower().replace('_', '-')
        return f"https://{account}.suitetalk.api.netsuite.com/services/rest/record/v1"
    
    @classmethod
    def get_netsuite_base_url(cls) -> str:
        """Get the NetSuite web UI base URL"""
        account = cls.NETSUITE_ACCOUNT_ID.replace('_SB', '').replace('_', '')
        return f"https://{account}.app.netsuite.com"
    
    # ==========================================================================
    # Slack Configuration
    # ==========================================================================
    SLACK_TOKEN = get_env('SLACK_TOKEN', '')
    SLACK_CHANNEL = get_env('SLACK_CHANNEL', 'C092UMM690S')
    SLACK_NOTIFICATIONS_ENABLED = get_bool_env('SLACK_NOTIFICATIONS_ENABLED', True)
    
    # ==========================================================================
    # Application Configuration
    # ==========================================================================
    FLASK_SECRET_KEY = get_env('FLASK_SECRET_KEY', 'customer-sync-secret-key')
    ENVIRONMENT = get_env('ENVIRONMENT', 'production')
    PORT = get_int_env('PORT', 8080)
    
    # ==========================================================================
    # Production Mode Settings
    # ==========================================================================
    PRODUCTION_MODE = get_bool_env('PRODUCTION_MODE', True)
    LOG_LEVEL = get_env('LOG_LEVEL', 'INFO')
    
    # ==========================================================================
    # Performance Configuration
    # ==========================================================================
    MAX_CACHE_SIZE = get_int_env('MAX_CACHE_SIZE', 25)
    MAX_FAILED_CACHE_SIZE = get_int_env('MAX_FAILED_CACHE_SIZE', 50)
    MAX_RETRIES = get_int_env('MAX_RETRIES', 3)
    RETRY_DELAY = get_int_env('RETRY_DELAY', 1)
    BATCH_SIZE = get_int_env('BATCH_SIZE', 5)
    
    # ==========================================================================
    # Redis Configuration
    # ==========================================================================
    REDIS_URL = get_env('REDIS_URL', 'redis://localhost:6379')
    
    # ==========================================================================
    # HubDB Configuration
    # ==========================================================================
    HUBDB_TABLE_ID = get_env('HUBDB_TABLE_ID', '')  # Set to your HubDB table ID
    CUSTOMER_SYNC_BATCH_SIZE = get_int_env('CUSTOMER_SYNC_BATCH_SIZE', 5)
    CUSTOMER_SYNC_INTERVAL = get_int_env('CUSTOMER_SYNC_INTERVAL', 300)  # 5 minutes
    CUSTOMER_SYNC_ENABLED = get_bool_env('CUSTOMER_SYNC_ENABLED', False)  # Legacy
    
    # ==========================================================================
    # Cron Configuration (APScheduler)
    # ==========================================================================
    CRON_INTERVAL_MINUTES = get_int_env('CRON_INTERVAL_MINUTES', 5)
    CRON_ENABLED_ON_START = get_bool_env('CRON_ENABLED_ON_START', True)


# Create singleton instance
config = Config()
