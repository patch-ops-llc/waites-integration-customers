# netsuite_rest.py
# NetSuite REST API Client using Token-Based Authentication (TBA)
# Replaces Zapier's NetSuite integration for Railway deployment

import time
import hashlib
import hmac
import base64
import urllib.parse
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
from requests_oauthlib import OAuth1

from constants import (
    NETSUITE_ACCOUNT_ID, NETSUITE_REST_URL,
    NETSUITE_CONSUMER_KEY, NETSUITE_CONSUMER_SECRET,
    NETSUITE_TOKEN_ID, NETSUITE_TOKEN_SECRET,
    MAX_RETRIES, RETRY_DELAY
)
from logger import logger


class NetSuiteRestClient:
    """
    NetSuite REST API Client using Token-Based Authentication (OAuth 1.0)
    
    This replaces the Zapier integration with direct API calls.
    Supports all CRUD operations on NetSuite records.
    """
    
    # Record type to REST endpoint mapping
    RECORD_TYPE_MAP = {
        "customer": "customer",
        "estimate": "estimate",
        "salesOrder": "salesOrder",
        "invoice": "invoice",
        "employee": "employee",
        "inventoryItem": "inventoryItem",
        "assemblyItem": "assemblyItem",
        "kitItem": "kitItem",
        "serviceSaleItem": "serviceSaleItem",
        "serviceResaleItem": "serviceResaleItem",
        "nonInventorySaleItem": "nonInventorySaleItem",
        "nonInventoryResaleItem": "nonInventoryResaleItem",
        "nonInventoryPurchaseItem": "nonInventoryPurchaseItem"
    }
    
    def __init__(self):
        self.account_id = NETSUITE_ACCOUNT_ID
        self.base_url = NETSUITE_REST_URL
        
        # Validate credentials are set
        if not all([NETSUITE_CONSUMER_KEY, NETSUITE_CONSUMER_SECRET, 
                    NETSUITE_TOKEN_ID, NETSUITE_TOKEN_SECRET]):
            raise ValueError(
                "NetSuite TBA credentials not configured. "
                "Set NETSUITE_CONSUMER_KEY, NETSUITE_CONSUMER_SECRET, "
                "NETSUITE_TOKEN_ID, and NETSUITE_TOKEN_SECRET environment variables."
            )
        
        # Create OAuth1 session for Token-Based Authentication
        self.oauth = OAuth1(
            client_key=NETSUITE_CONSUMER_KEY,
            client_secret=NETSUITE_CONSUMER_SECRET,
            resource_owner_key=NETSUITE_TOKEN_ID,
            resource_owner_secret=NETSUITE_TOKEN_SECRET,
            realm=NETSUITE_ACCOUNT_ID.replace('-', '_').upper(),
            signature_method='HMAC-SHA256'
        )
        
        self.session = requests.Session()
        self.session.auth = self.oauth
        
        logger.info(f"NetSuite REST client initialized for account: {self.account_id}")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get default headers for API requests"""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def _handle_response(self, response: requests.Response, operation: str) -> Dict[str, Any]:
        """Handle API response and extract data or error"""
        try:
            if response.status_code in [200, 201, 204]:
                if response.status_code == 204 or not response.content:
                    return {"success": True}
                return response.json()
            else:
                error_data = response.json() if response.content else {}
                error_msg = error_data.get('o:errorDetails', [{}])
                if isinstance(error_msg, list) and error_msg:
                    error_msg = error_msg[0].get('detail', str(error_data))
                else:
                    error_msg = str(error_data)
                
                logger.error(f"NetSuite API error ({operation}): {response.status_code} - {error_msg}")
                return {
                    "success": False,
                    "error": error_msg,
                    "status_code": response.status_code
                }
        except Exception as e:
            logger.error(f"Error parsing response: {str(e)}")
            return {
                "success": False,
                "error": f"Response parsing error: {str(e)}",
                "raw_response": response.text[:500] if response.text else ""
            }
    
    def _request_with_retry(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        headers = kwargs.pop('headers', {})
        headers.update(self._get_headers())
        kwargs['headers'] = headers
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.request(method, url, **kwargs)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY * 5))
                    logger.warning(f"Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors with retry
                if response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Server error {response.status_code}, retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                
                return response
                
            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Request failed, retrying in {delay}s: {str(e)}")
                    time.sleep(delay)
                    continue
                logger.error(f"Request failed after {MAX_RETRIES} attempts: {str(e)}")
                return None
        
        return None
    
    # =========================================================================
    # CRUD Operations
    # =========================================================================
    
    def get_record(self, record_type: str, record_id: str, 
                   expand_sub_resources: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get a single record by ID
        
        Args:
            record_type: NetSuite record type (e.g., 'customer', 'estimate')
            record_id: Internal ID of the record
            expand_sub_resources: Whether to expand sublists
        
        Returns:
            Record data dict or None if not found
        """
        endpoint = self.RECORD_TYPE_MAP.get(record_type, record_type)
        url = f"{self.base_url}/{endpoint}/{record_id}"
        
        params = {}
        if expand_sub_resources:
            params['expandSubResources'] = 'true'
        
        logger.info(f"GET {record_type} {record_id}")
        
        response = self._request_with_retry('GET', url, params=params)
        if response is None:
            return None
        
        if response.status_code == 404:
            logger.info(f"Record not found: {record_type} {record_id}")
            return None
        
        result = self._handle_response(response, f"GET {record_type}")
        if 'success' in result and not result.get('success'):
            return None
        
        return result
    
    def create_record(self, record_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new record
        
        Args:
            record_type: NetSuite record type
            data: Record data to create
        
        Returns:
            Created record data with ID
        """
        endpoint = self.RECORD_TYPE_MAP.get(record_type, record_type)
        url = f"{self.base_url}/{endpoint}"
        
        logger.info(f"CREATE {record_type}")
        logger.debug(f"Create payload: {data}")
        
        response = self._request_with_retry('POST', url, json=data)
        if response is None:
            return {"success": False, "error": "Request failed"}
        
        result = self._handle_response(response, f"CREATE {record_type}")
        
        # Extract record ID - try multiple sources
        record_id = None
        
        # Try 1: Location header (most common for 201 responses)
        if response.status_code in [200, 201, 204]:
            location = response.headers.get('Location', '')
            if location:
                # Extract ID from location URL (e.g., ".../customer/12345")
                record_id = location.rstrip('/').split('/')[-1]
                logger.debug(f"Extracted ID from Location header: {record_id}")
        
        # Try 2: Response body 'id' field
        if not record_id and result.get('id'):
            record_id = str(result['id'])
            logger.debug(f"Extracted ID from response body: {record_id}")
        
        # Try 3: Response body 'internalId' field (some record types use this)
        if not record_id and result.get('internalId'):
            record_id = str(result['internalId'])
            logger.debug(f"Extracted ID from internalId: {record_id}")
        
        # Set result fields
        if record_id:
            result['id'] = record_id
            result['success'] = True
            logger.success(f"Created {record_type} with ID: {record_id}")
        elif response.status_code in [200, 201, 204]:
            # Record was created but we couldn't extract ID
            logger.warning(f"⚠️ {record_type} created (status {response.status_code}) but could not extract ID")
            logger.warning(f"Response headers: {dict(response.headers)}")
            logger.warning(f"Response body: {result}")
            result['success'] = True  # Still mark as success since it was created
        
        return result
    
    def update_record(self, record_type: str, record_id: str, 
                      data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing record
        
        Args:
            record_type: NetSuite record type
            record_id: Internal ID of the record
            data: Fields to update
        
        Returns:
            Update result
        """
        endpoint = self.RECORD_TYPE_MAP.get(record_type, record_type)
        url = f"{self.base_url}/{endpoint}/{record_id}"
        
        logger.info(f"UPDATE {record_type} {record_id}")
        logger.debug(f"Update payload: {data}")
        
        response = self._request_with_retry('PATCH', url, json=data)
        if response is None:
            return {"success": False, "error": "Request failed"}
        
        result = self._handle_response(response, f"UPDATE {record_type}")
        if response.status_code in [200, 204]:
            result['success'] = True
            result['id'] = record_id
        
        return result
    
    def upsert_record(self, record_type: str, external_id: str,
                      data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert a record using external ID
        
        Args:
            record_type: NetSuite record type
            external_id: External ID for matching
            data: Record data
        
        Returns:
            Upsert result with record ID
        """
        endpoint = self.RECORD_TYPE_MAP.get(record_type, record_type)
        url = f"{self.base_url}/{endpoint}/eid:{external_id}"
        
        logger.info(f"UPSERT {record_type} externalId={external_id}")
        
        # Try PUT for upsert
        response = self._request_with_retry('PUT', url, json=data)
        if response is None:
            return {"success": False, "error": "Request failed"}
        
        result = self._handle_response(response, f"UPSERT {record_type}")
        
        if response.status_code in [200, 201, 204]:
            result['success'] = True
            # Get ID from location header or response
            location = response.headers.get('Location', '')
            if location:
                result['id'] = location.split('/')[-1]
        
        return result
    
    def delete_record(self, record_type: str, record_id: str) -> Dict[str, Any]:
        """Delete a record"""
        endpoint = self.RECORD_TYPE_MAP.get(record_type, record_type)
        url = f"{self.base_url}/{endpoint}/{record_id}"
        
        logger.info(f"DELETE {record_type} {record_id}")
        
        response = self._request_with_retry('DELETE', url)
        if response is None:
            return {"success": False, "error": "Request failed"}
        
        if response.status_code == 204:
            return {"success": True}
        
        return self._handle_response(response, f"DELETE {record_type}")
    
    # =========================================================================
    # Search Operations
    # =========================================================================
    
    def search_records(self, record_type: str, filters: Dict[str, Any] = None,
                       limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Search for records using REST API q parameter
        
        Args:
            record_type: NetSuite record type
            filters: Search filters (field: value pairs)
            limit: Maximum records to return
            offset: Starting offset for pagination
        
        Returns:
            Search results with items list
        """
        endpoint = self.RECORD_TYPE_MAP.get(record_type, record_type)
        url = f"{self.base_url}/{endpoint}"
        
        params = {
            'limit': limit,
            'offset': offset
        }
        
        # Build q parameter for filtering (NetSuite REST API syntax)
        # Format: q=fieldName IS "value" or q=fieldName CONTAIN "value"
        if filters:
            q_parts = []
            for key, value in filters.items():
                if isinstance(value, dict) and 'operator' in value:
                    op = value['operator']
                    val = value['value']
                    q_parts.append(f'{key} {op} "{val}"')
                else:
                    # Default to IS operator for exact match
                    q_parts.append(f'{key} IS "{value}"')
            
            if q_parts:
                params['q'] = ' AND '.join(q_parts)
        
        logger.info(f"SEARCH {record_type}")
        if params.get('q'):
            logger.debug(f"Search filter: {params['q']}")
        
        response = self._request_with_retry('GET', url, params=params)
        if response is None:
            return {"success": False, "error": "Request failed", "items": []}
        
        result = self._handle_response(response, f"SEARCH {record_type}")
        
        # Normalize response structure
        if 'items' not in result and isinstance(result, list):
            result = {"items": result, "success": True}
        elif 'items' not in result:
            result['items'] = []
        
        return result
    
    def find_by_field(self, record_type: str, field: str, value: str) -> Optional[Dict[str, Any]]:
        """
        Find a record by a specific field value
        
        Args:
            record_type: NetSuite record type
            field: Field name to search
            value: Value to match
        
        Returns:
            First matching record or None
        """
        # Use SuiteQL for more flexible searching
        query = f"SELECT * FROM {record_type} WHERE {field} = '{value}'"
        
        result = self.execute_suiteql(query)
        
        if result.get('items') and len(result['items']) > 0:
            return result['items'][0]
        
        return None
    
    def execute_suiteql(self, query: str, limit: int = 1000) -> Dict[str, Any]:
        """
        Execute a SuiteQL query
        
        Args:
            query: SuiteQL query string
            limit: Maximum records to return
        
        Returns:
            Query results
        """
        # SuiteQL endpoint
        url = f"https://{self.account_id.lower().replace('_', '-')}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
        
        params = {'limit': limit}
        data = {'q': query}
        
        # CRITICAL: SuiteQL requires the Prefer: transient header
        suiteql_headers = {
            'Prefer': 'transient'
        }
        
        logger.debug(f"SuiteQL: {query}")
        
        response = self._request_with_retry('POST', url, json=data, params=params, headers=suiteql_headers)
        if response is None:
            return {"success": False, "error": "Request failed", "items": []}
        
        result = self._handle_response(response, "SuiteQL")
        
        # Normalize response
        if 'items' not in result:
            result['items'] = result.get('links', [])
        
        return result
    
    # =========================================================================
    # Convenience Methods for Common Operations
    # =========================================================================
    
    def get_customer(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Get customer by ID"""
        return self.get_record('customer', customer_id)
    
    def create_customer(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new customer"""
        return self.create_record('customer', data)
    
    def update_customer(self, customer_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a customer"""
        return self.update_record('customer', customer_id, data)
    
    def find_customer_by_name(self, company_name: str) -> Optional[Dict[str, Any]]:
        """Find customer by company name"""
        query = f"SELECT id, companyName, subsidiary, isInactive FROM customer WHERE companyName = '{company_name}'"
        result = self.execute_suiteql(query)
        
        if result.get('items') and len(result['items']) > 0:
            return result['items'][0]
        return None
    
    def find_employee_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Find employee by email - tries SuiteQL first, then REST API search"""
        # Try SuiteQL first
        try:
            query = f"SELECT id, entityId, firstName, lastName, email, isSalesRep FROM employee WHERE email = '{email}'"
            result = self.execute_suiteql(query)
            
            if result.get('items') and len(result['items']) > 0:
                return result['items'][0]
        except Exception as e:
            logger.warning(f"SuiteQL employee lookup failed: {str(e)}")
        
        # Fallback: Try REST API search
        try:
            logger.info(f"🔄 Trying REST API search for employee: {email}")
            search_result = self.search_records(
                "employee",
                filters={"email": email},
                limit=1
            )
            
            if search_result.get('items') and len(search_result['items']) > 0:
                employee = search_result['items'][0]
                logger.success(f"✅ Found employee via REST API: {employee.get('id')}")
                return employee
        except Exception as e:
            logger.warning(f"REST API employee search failed: {str(e)}")
        
        return None
    
    def get_estimate(self, estimate_id: str) -> Optional[Dict[str, Any]]:
        """Get quote/estimate by ID"""
        return self.get_record('estimate', estimate_id, expand_sub_resources=True)
    
    def create_estimate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new quote/estimate"""
        return self.create_record('estimate', data)
    
    def update_estimate(self, estimate_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a quote/estimate"""
        return self.update_record('estimate', estimate_id, data)
    
    def find_item_by_id(self, record_type: str, item_id: str) -> Optional[Dict[str, Any]]:
        """Find an item by internal ID"""
        return self.get_record(record_type, item_id)
    
    def search_items_by_name(self, item_name: str, record_type: str = None) -> List[Dict[str, Any]]:
        """Search for items by name/display name"""
        # Build search across item types
        if record_type:
            query = f"SELECT id, itemId, displayName FROM {record_type} WHERE displayName LIKE '%{item_name}%' OR itemId LIKE '%{item_name}%'"
        else:
            # Search common item types
            query = f"""
                SELECT id, itemId, displayName, 'inventoryItem' as recordType FROM inventoryItem 
                WHERE displayName LIKE '%{item_name}%' OR itemId LIKE '%{item_name}%'
                UNION ALL
                SELECT id, itemId, displayName, 'assemblyItem' as recordType FROM assemblyItem 
                WHERE displayName LIKE '%{item_name}%' OR itemId LIKE '%{item_name}%'
            """
        
        result = self.execute_suiteql(query)
        return result.get('items', [])
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the API connection and credentials"""
        try:
            # Try to get company information
            url = f"https://{self.account_id.lower().replace('_', '-')}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog"
            
            response = self._request_with_retry('GET', url)
            
            if response and response.status_code == 200:
                return {
                    "success": True,
                    "message": "Connection successful",
                    "account_id": self.account_id
                }
            else:
                return {
                    "success": False,
                    "error": f"Connection failed: {response.status_code if response else 'No response'}",
                    "account_id": self.account_id
                }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "account_id": self.account_id
            }


# Create singleton instance (optional, for convenience)
def get_netsuite_client() -> NetSuiteRestClient:
    """Get or create NetSuite REST client instance"""
    return NetSuiteRestClient()




