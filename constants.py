# constants.py
# Configuration constants for Customer Sync Service
# Uses environment variables via config.py

import os

# =============================================================================
# Load configuration from environment variables
# =============================================================================

try:
    from config import config
    
    # HubSpot Configuration
    HUBSPOT_PAT = config.HUBSPOT_PAT
    HUBSPOT_BASE_URL = config.HUBSPOT_BASE_URL
    HUBSPOT_FACILITY_OBJECT_TYPE_ID = config.HUBSPOT_FACILITY_OBJECT_TYPE_ID
    HUBSPOT_PORTAL_ID = config.HUBSPOT_PORTAL_ID
    HUBSPOT_QUOTE_DOMAIN = config.HUBSPOT_QUOTE_DOMAIN
    
    # NetSuite Configuration
    NETSUITE_ACCOUNT_ID = config.NETSUITE_ACCOUNT_ID
    NETSUITE_BASE_URL = config.get_netsuite_base_url()
    NETSUITE_REST_URL = config.get_netsuite_rest_url()
    NETSUITE_CONSUMER_KEY = config.NETSUITE_CONSUMER_KEY
    NETSUITE_CONSUMER_SECRET = config.NETSUITE_CONSUMER_SECRET
    NETSUITE_TOKEN_ID = config.NETSUITE_TOKEN_ID
    NETSUITE_TOKEN_SECRET = config.NETSUITE_TOKEN_SECRET
    
    # Slack Configuration
    SLACK_TOKEN = config.SLACK_TOKEN
    SLACK_CHANNEL = config.SLACK_CHANNEL
    SLACK_NOTIFICATIONS_ENABLED = config.SLACK_NOTIFICATIONS_ENABLED
    
    # Runtime Configuration
    PRODUCTION_MODE = config.PRODUCTION_MODE
    LOG_LEVEL = config.LOG_LEVEL
    MAX_CACHE_SIZE = config.MAX_CACHE_SIZE
    MAX_FAILED_CACHE_SIZE = config.MAX_FAILED_CACHE_SIZE
    MAX_RETRIES = config.MAX_RETRIES
    RETRY_DELAY = config.RETRY_DELAY
    BATCH_SIZE = config.BATCH_SIZE

except ImportError:
    # Fallback for backwards compatibility
    print("[WARNING] config.py not found, using environment variables directly")
    
    HUBSPOT_PAT = os.getenv("HUBSPOT_PAT", "")
    HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
    HUBSPOT_FACILITY_OBJECT_TYPE_ID = os.getenv("HUBSPOT_FACILITY_OBJECT_TYPE_ID", "2-43136116")
    HUBSPOT_PORTAL_ID = os.getenv("HUBSPOT_PORTAL_ID", "48162723")
    HUBSPOT_QUOTE_DOMAIN = os.getenv("HUBSPOT_QUOTE_DOMAIN", "waites.net")
    
    NETSUITE_ACCOUNT_ID = os.getenv("NETSUITE_ACCOUNT_ID", "")
    NETSUITE_BASE_URL = os.getenv("NETSUITE_BASE_URL", "")
    NETSUITE_REST_URL = ""
    NETSUITE_CONSUMER_KEY = os.getenv("NETSUITE_CONSUMER_KEY", "")
    NETSUITE_CONSUMER_SECRET = os.getenv("NETSUITE_CONSUMER_SECRET", "")
    NETSUITE_TOKEN_ID = os.getenv("NETSUITE_TOKEN_ID", "")
    NETSUITE_TOKEN_SECRET = os.getenv("NETSUITE_TOKEN_SECRET", "")
    
    SLACK_TOKEN = os.getenv("SLACK_TOKEN", "")
    SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "C092UMM690S")
    SLACK_NOTIFICATIONS_ENABLED = os.getenv("SLACK_NOTIFICATIONS_ENABLED", "true").lower() == "true"
    
    PRODUCTION_MODE = os.getenv("PRODUCTION_MODE", "true").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    MAX_CACHE_SIZE = int(os.getenv("MAX_CACHE_SIZE", "25"))
    MAX_FAILED_CACHE_SIZE = int(os.getenv("MAX_FAILED_CACHE_SIZE", "50"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "1"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))


# =============================================================================
# NetSuite Default Values
# =============================================================================
DEFAULT_SUBSIDIARY_ID = "2"  # Waites Sensor Technologies, Inc.
PARENT_SUBSIDIARY_ID = "1"
DEFAULT_REVENUE_SEGMENT = "1"  # Direct
DEFAULT_INDUSTRY = "47"  # Other

# =============================================================================
# Customer Status Configuration
# =============================================================================
CUSTOMER_STATUS_FIELD = "custentity_waites_customer_status"
CUSTOMER_STATUS_ACTIVE_VALUE = os.getenv("CUSTOMER_STATUS_ACTIVE_VALUE", "Active")
CUSTOMER_STATUS_ACTIVE_ID = os.getenv("CUSTOMER_STATUS_ACTIVE_ID", None)

# Sales Rep Configuration
DEFAULT_SALES_REP_ID = os.getenv("DEFAULT_SALES_REP_ID", "46109")  # Don Erb

# =============================================================================
# Subsidiary Configuration
# =============================================================================
CREATE_NEW_CUSTOMER_ON_SUBSIDIARY_MISMATCH = True
