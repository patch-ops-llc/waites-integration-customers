# hubspot_client.py
# HubSpot API client for Customer Sync Service (simplified)

import requests
import time
from typing import Dict, List, Optional, Any
from collections import OrderedDict
from constants import (
    HUBSPOT_PAT, HUBSPOT_BASE_URL, HUBSPOT_FACILITY_OBJECT_TYPE_ID,
    MAX_RETRIES, RETRY_DELAY, MAX_CACHE_SIZE
)
from logger import logger


class HubSpotClient:
    """HubSpot API client for customer sync operations"""
    
    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {HUBSPOT_PAT}",
            "Content-Type": "application/json"
        }
        self.base_url = HUBSPOT_BASE_URL
        self.facility_object_type_id = HUBSPOT_FACILITY_OBJECT_TYPE_ID
        self._product_cache = OrderedDict()
    
    def clear_caches(self):
        """Clear all caches to free memory"""
        self._product_cache.clear()
        logger.info("Cleared HubSpot client caches")
    
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.request(method, url, headers=self.headers, **kwargs)
                
                if response.status_code >= 400:
                    logger.warning(f"HTTP {response.status_code}: {method} {url}")
                    if hasattr(response, 'text'):
                        logger.debug(f"Response body: {response.text[:500]}")
                
                response.raise_for_status()
                return response.json()
                
            except requests.HTTPError as e:
                if e.response.status_code == 400:
                    logger.error(f"❌ Bad Request (400): {method} {url}")
                    return None
                elif e.response.status_code == 429:
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY * (2 ** attempt) + 5
                        logger.warning(f"Rate limited, retrying in {delay}s")
                        time.sleep(delay)
                        continue
                
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"HTTP error {e.response.status_code}, retrying in {delay}s")
                    time.sleep(delay)
                    continue
                
                logger.error(f"Request failed after {MAX_RETRIES} attempts: {method} {url}", e)
                return None
                
            except requests.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Request failed after {MAX_RETRIES} attempts: {method} {url}", e)
                    return None
                logger.warning(f"Request failed, retrying in {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY)
        return None
    
    def get_facility(self, facility_id: str) -> Optional[Dict[str, Any]]:
        """Get facility by HubSpot internal ID with all relevant properties."""
        url = f"{self.base_url}/crm/v3/objects/{self.facility_object_type_id}/{facility_id}"
        params = {
            "properties": "name,netsuite_customer_id,phone,website,description,industry,"
                         "facility_street,facility_city,facility_state,facility_zip_code,facility_country,"
                         "shipping_street,shipping_city,shipping_state,shipping_zip_code,shipping_country,"
                         "billing_street,billing_city,billing_state,billing_zip_code,billing_country,"
                         "waites_master_id"
        }
        
        logger.info(f"Retrieving HubSpot facility {facility_id}")
        data = self._make_request("GET", url, params=params)
        if data:
            logger.success(f"Retrieved facility: {data.get('properties', {}).get('name', 'Unnamed')}")
        return data
    
    def search_facility_by_waites_id(self, waites_id: str) -> Optional[Dict[str, Any]]:
        """Search for a facility by Waites Master ID property."""
        if not waites_id:
            return None
        
        url = f"{self.base_url}/crm/v3/objects/{self.facility_object_type_id}/search"
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "waites_master_id",
                    "operator": "EQ",
                    "value": waites_id
                }]
            }],
            "properties": [
                "name", "netsuite_customer_id", "phone", "website", "description", "industry",
                "facility_street", "facility_city", "facility_state", "facility_zip_code", "facility_country",
                "shipping_street", "shipping_city", "shipping_state", "shipping_zip_code", "shipping_country",
                "billing_street", "billing_city", "billing_state", "billing_zip_code", "billing_country",
                "waites_master_id"
            ],
            "limit": 1
        }
        
        logger.info(f"🔍 Searching for facility by Waites ID: {waites_id}")
        data = self._make_request("POST", url, json=payload)
        
        if data and data.get('results'):
            facility = data['results'][0]
            facility_name = facility.get('properties', {}).get('name', 'Unnamed')
            facility_hs_id = facility.get('id')
            logger.success(f"✅ Found facility by Waites ID: {facility_name} (HubSpot ID: {facility_hs_id})")
            return facility
        
        logger.warning(f"⚠️ No facility found with Waites ID: {waites_id}")
        return None
    
    def get_company(self, company_id: str) -> Optional[Dict[str, Any]]:
        """Get company by ID with all relevant properties"""
        url = f"{self.base_url}/crm/v3/objects/companies/{company_id}"
        params = {
            "properties": "name,netsuite_record_id,phone,website,industry,"
                         "address,city,state,zip,country,description,"
                         "waites_master_id,hubspot_owner_id"
        }
        
        logger.info(f"Retrieving HubSpot company {company_id}")
        data = self._make_request("GET", url, params=params)
        if data:
            logger.success(f"Retrieved company: {data.get('properties', {}).get('name', 'Unnamed')}")
        return data
    
    def get_associated_company_for_facility(self, facility_id: str) -> Optional[str]:
        """Get company associated with a facility (for subcustomer relationship)"""
        url = f"{self.base_url}/crm/v4/objects/{self.facility_object_type_id}/{facility_id}/associations/company"
        
        logger.info(f"Getting associated company for facility {facility_id}")
        data = self._make_request("GET", url)
        if data:
            results = data.get('results', [])
            if results:
                company_id = str(results[0]['toObjectId'])
                logger.success(f"Found associated company for facility: {company_id}")
                return company_id
        
        logger.info("No company associated with facility")
        return None
    
    def update_facility(self, facility_id: str, netsuite_customer_id: str) -> bool:
        """Update HubSpot facility with NetSuite customer ID"""
        url = f"{self.base_url}/crm/v3/objects/{self.facility_object_type_id}/{facility_id}"
        property_name = "netsuite_customer_id"
        payload = {"properties": {property_name: netsuite_customer_id}}
        
        logger.info(f"Updating HubSpot facility {facility_id} with NetSuite ID {netsuite_customer_id}")
        
        data = self._make_request("PATCH", url, json=payload)
        if data:
            updated_props = data.get('properties', {})
            actual_value = updated_props.get(property_name)
            
            if actual_value == str(netsuite_customer_id):
                logger.success(f"✅ Updated HubSpot facility with NetSuite customer ID: {netsuite_customer_id}")
                return True
            elif actual_value:
                logger.warning(f"⚠️ Property set but value mismatch. Expected: {netsuite_customer_id}, Got: {actual_value}")
                return True
            else:
                logger.warning(f"⚠️ Property '{property_name}' not found in response.")
                return True
        
        logger.error(f"❌ Failed to update HubSpot facility {facility_id}")
        return False
    
    def update_company(self, company_id: str, netsuite_customer_id: str) -> bool:
        """Update HubSpot company with NetSuite customer ID"""
        url = f"{self.base_url}/crm/v3/objects/companies/{company_id}"
        property_name = "netsuite_record_id"
        payload = {"properties": {property_name: netsuite_customer_id}}
        
        logger.info(f"Updating HubSpot company {company_id} with NetSuite ID {netsuite_customer_id}")
        
        data = self._make_request("PATCH", url, json=payload)
        if data:
            updated_props = data.get('properties', {})
            actual_value = updated_props.get(property_name)
            
            if actual_value == str(netsuite_customer_id):
                logger.success(f"✅ Updated HubSpot company with NetSuite customer ID: {netsuite_customer_id}")
                return True
            elif actual_value:
                logger.warning(f"⚠️ Property set but value mismatch. Expected: {netsuite_customer_id}, Got: {actual_value}")
                return True
            else:
                logger.warning(f"⚠️ Property '{property_name}' not found in response.")
                return True
        
        logger.error(f"❌ Failed to update HubSpot company {company_id}")
        return False
    
    def get_owner_email(self, owner_id: str) -> Optional[str]:
        """Get HubSpot owner email by owner ID for NetSuite sales rep lookup"""
        owner_info = self.get_owner_info(owner_id)
        return owner_info.get('email') if owner_info else None
    
    def get_owner_info(self, owner_id: str) -> Optional[Dict[str, str]]:
        """Get HubSpot owner info (email and name) by owner ID"""
        if not owner_id:
            return None
            
        url = f"{self.base_url}/crm/v3/owners/{owner_id}"
        params = {"properties": "email,firstName,lastName"}
        
        logger.info(f"Retrieving HubSpot owner {owner_id}")
        data = self._make_request("GET", url, params=params)
        
        if data:
            email = data.get('email')
            first_name = data.get('firstName', '')
            last_name = data.get('lastName', '')
            full_name = f"{first_name} {last_name}".strip()
            
            if email:
                logger.success(f"Retrieved owner: {full_name} ({email})")
                return {
                    'email': email,
                    'name': full_name,
                    'firstName': first_name,
                    'lastName': last_name
                }
            else:
                logger.warning(f"No email found for owner: {full_name}")
        else:
            logger.warning(f"Owner {owner_id} not found")
        
        return None
