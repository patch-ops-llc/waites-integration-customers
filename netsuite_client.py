# netsuite_client.py
# NetSuite API client for Customer Sync Service (simplified for customer operations only)

import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import OrderedDict
from constants import (
    NETSUITE_ACCOUNT_ID, MAX_RETRIES, RETRY_DELAY,
    DEFAULT_SUBSIDIARY_ID, DEFAULT_REVENUE_SEGMENT,
    MAX_CACHE_SIZE, MAX_FAILED_CACHE_SIZE,
    CUSTOMER_STATUS_FIELD, CUSTOMER_STATUS_ACTIVE_VALUE, CUSTOMER_STATUS_ACTIVE_ID,
    DEFAULT_SALES_REP_ID
)
from logger import logger
from netsuite_rest import NetSuiteRestClient


class NetSuiteClient:
    """NetSuite API client for customer operations - Customer Sync Service"""
    
    def __init__(self, hubspot_client=None):
        self.account_id = NETSUITE_ACCOUNT_ID
        self.hubspot_client = hubspot_client
        
        # Initialize REST API client
        self.rest_client = NetSuiteRestClient()
        
        logger.debug(f"NetSuiteClient initialized with hubspot_client: {hubspot_client is not None}")
        
        # Caches
        self._item_cache = OrderedDict()
        self._failed_cache = set()
    
    def clear_caches(self):
        """Clear all caches to free memory"""
        self._item_cache.clear()
        self._failed_cache.clear()
        logger.info("Cleared NetSuite client caches")
    
    def _add_to_cache(self, cache_key: str, value: Any):
        """Add item to bounded cache with LRU eviction"""
        while len(self._item_cache) >= MAX_CACHE_SIZE:
            self._item_cache.popitem(last=False)
        self._item_cache[cache_key] = value
    
    def _add_to_failed_cache(self, cache_key: str):
        """Add to failed cache with size limit"""
        if len(self._failed_cache) >= MAX_FAILED_CACHE_SIZE:
            items_to_remove = list(self._failed_cache)[:MAX_FAILED_CACHE_SIZE // 2]
            for item in items_to_remove:
                self._failed_cache.discard(item)
        self._failed_cache.add(cache_key)
    
    def upsert_customer(self, entity_data: Dict[str, Any], hubspot_entity_id: str, 
                       entity_type: str = "facility",
                       parent_customer_id: str = None) -> Dict[str, Any]:
        """Handle Facility -> Company mapping"""
        props = entity_data.get('properties', {})
        entity_name = props.get('name', 'Unknown Entity')
        
        if parent_customer_id:
            logger.info(f"Upserting customer from {entity_type}: {entity_name} (as subcustomer of {parent_customer_id})")
        else:
            logger.info(f"Upserting customer from {entity_type}: {entity_name}")
        
        # Check for existing NetSuite ID
        if entity_type == "facility":
            existing_netsuite_id = props.get('netsuite_customer_id')
        else:
            existing_netsuite_id = props.get('netsuite_record_id')
        
        # If we have existing ID, verify and update
        if existing_netsuite_id:
            logger.info(f"🔍 HubSpot {entity_type} has existing NetSuite ID: {existing_netsuite_id}")
            
            existing_customer = self.get_record("customer", existing_netsuite_id)
            if existing_customer:
                customer_name = existing_customer.get('companyName', 'Unnamed')
                logger.success(f"Found existing customer: {customer_name}")
                
                # Reactivate if inactive
                is_inactive = existing_customer.get('isInactive', False)
                if is_inactive:
                    logger.warning(f"⚠️ Customer {existing_netsuite_id} is INACTIVE - reactivating")
                    self._reactivate_customer(existing_netsuite_id)
                
                return self._update_existing_customer(existing_netsuite_id, entity_data, entity_type, parent_customer_id)
        
        # Search for existing customer by name
        logger.info(f"Searching NetSuite for customer by name: {entity_name}")
        existing_by_name = self._find_customer_by_name(entity_name)
        
        if existing_by_name:
            found_customer_id = existing_by_name.get('id')
            logger.success(f"✅ Found existing NetSuite customer by name (ID: {found_customer_id})")
            
            if existing_by_name.get('isInactive', False):
                self._reactivate_customer(found_customer_id)
            
            # Update found customer with waites_master_id if present
            waites_master_id = props.get('waites_master_id')
            update_data = {}
            if waites_master_id:
                update_data["custentity_wst_customer_waitesmasterid"] = waites_master_id
            
            if parent_customer_id:
                update_data["parent"] = {"id": parent_customer_id}
            
            if update_data:
                try:
                    self.rest_client.update_customer(found_customer_id, update_data)
                    logger.success(f"✅ Updated customer {found_customer_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not update customer: {str(e)}")
            
            return {
                "success": True,
                "customer_id": found_customer_id,
                "data": existing_by_name,
                "action": "found_existing_by_name",
                "needs_hubspot_update": True
            }
        
        # Create new customer
        logger.info(f"Creating new customer: {entity_name}")
        return self._create_wst_customer_unique(entity_data, hubspot_entity_id, entity_type, parent_customer_id)
    
    def _reactivate_customer(self, customer_id: str) -> bool:
        """Reactivate an inactive customer."""
        try:
            update_data = {
                "isInactive": False,
                "cseg_revenue": {"id": DEFAULT_REVENUE_SEGMENT}
            }
            
            if CUSTOMER_STATUS_FIELD:
                if CUSTOMER_STATUS_ACTIVE_ID:
                    update_data[CUSTOMER_STATUS_FIELD] = {"id": CUSTOMER_STATUS_ACTIVE_ID}
                elif CUSTOMER_STATUS_ACTIVE_VALUE:
                    update_data[CUSTOMER_STATUS_FIELD] = CUSTOMER_STATUS_ACTIVE_VALUE
            
            result = self.rest_client.update_customer(customer_id, update_data)
            if result.get('success'):
                logger.success(f"✅ Reactivated customer {customer_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to reactivate customer: {str(e)}")
        return False
    
    def _create_wst_customer_unique(self, entity_data: Dict[str, Any], hubspot_entity_id: str, 
                                   entity_type: str = "facility",
                                   parent_customer_id: str = None) -> Dict[str, Any]:
        """Create WST customer with unique external ID"""
        props = entity_data.get('properties', {})
        original_name = props.get('name', f'Unknown {entity_type.title()}')
        
        stable_external_id = f"HS_{entity_type.upper()}_{hubspot_entity_id}"
        
        logger.info(f"Creating WST customer: {original_name}")
        
        website = props.get('website', '')
        if website and not website.startswith(('http://', 'https://')):
            website = f"https://{website.strip()}"
        
        customer_data = {
            "externalId": stable_external_id,
            "companyName": original_name,
            "phone": props.get('phone', ''),
            "url": website,
            "subsidiary": {"id": DEFAULT_SUBSIDIARY_ID},
            "cseg_revenue": {"id": DEFAULT_REVENUE_SEGMENT},
            "isInactive": False
        }
        
        if parent_customer_id:
            customer_data["parent"] = {"id": parent_customer_id}
        
        waites_master_id = props.get('waites_master_id')
        if waites_master_id:
            customer_data["custentity_wst_customer_waitesmasterid"] = waites_master_id
        
        # SALES REP: Map HubSpot company owner to NetSuite sales rep
        hubspot_owner_id = props.get('hubspot_owner_id')
        if hubspot_owner_id and self.hubspot_client:
            owner_email = self.hubspot_client.get_owner_email(hubspot_owner_id)
            if owner_email:
                netsuite_employee_id = self._find_employee_by_email(owner_email)
                if netsuite_employee_id:
                    customer_data["salesRep"] = {"id": netsuite_employee_id}
        
        # Add address
        address_parts = []
        if props.get('address'):
            address_parts.append(props['address'])
        if props.get('city') or props.get('state') or props.get('zip'):
            city_state_zip = f"{props.get('city', '')} {props.get('state', '')} {props.get('zip', '')}".strip()
            if city_state_zip:
                address_parts.append(city_state_zip)
        if props.get('country'):
            address_parts.append(props['country'])
        
        if address_parts:
            full_address = f"{original_name}\n" + "\n".join(address_parts)
            customer_data["billAddress"] = full_address
            customer_data["shipAddress"] = full_address
        
        try:
            result = self.rest_client.create_customer(customer_data)
            
            if result.get('success') or result.get('id'):
                customer_id = result.get('id')
                logger.success(f"✅ Created WST customer (ID: {customer_id})")
                return {
                    "success": True,
                    "customer_id": customer_id,
                    "data": result,
                    "action": "created_wst_customer",
                    "needs_hubspot_update": True
                }
            else:
                error_msg = result.get('error', 'Unknown error')
                
                if 'already exists' in error_msg.lower() or 'unique' in error_msg.lower():
                    logger.warning(f"⚠️ Customer already exists, attempting to find it...")
                    
                    existing = self._get_customer_by_external_id(stable_external_id)
                    if existing and existing.get('id'):
                        customer_id = str(existing['id'])
                        return {
                            "success": True,
                            "customer_id": customer_id,
                            "data": existing,
                            "action": "found_existing_by_external_id",
                            "needs_hubspot_update": True
                        }
                    
                    name_result = self._find_customer_by_name(original_name)
                    if name_result and name_result.get('id'):
                        customer_id = str(name_result['id'])
                        return {
                            "success": True,
                            "customer_id": customer_id,
                            "data": name_result,
                            "action": "found_existing_by_name_retry",
                            "needs_hubspot_update": True
                        }
                
                return {"success": False, "error": error_msg}
        
        except Exception as e:
            logger.error(f"Failed to create WST customer", e)
            return {"success": False, "error": str(e)}
    
    def _get_customer_by_external_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """Get a customer by external ID using REST API"""
        try:
            result = self.rest_client.get_record("customer", f"eid:{external_id}")
            if result and result.get('id'):
                return result
            return None
        except Exception as e:
            logger.warning(f"⚠️ External ID lookup failed: {str(e)}")
            return None
    
    def _update_existing_customer(self, customer_id: str, entity_data: Dict[str, Any], 
                                 entity_type: str = "facility",
                                 parent_customer_id: str = None) -> Dict[str, Any]:
        """Update existing customer"""
        props = entity_data.get('properties', {})
        entity_name = props.get('name', f'Unknown {entity_type.title()}')
        
        logger.info(f"Updating existing customer {customer_id}: {entity_name}")
        
        website = props.get('website', '')
        if website and not website.startswith(('http://', 'https://')):
            website = f"https://{website.strip()}"
        
        update_data = {
            "companyName": entity_name,
            "phone": props.get('phone', ''),
            "url": website,
            "cseg_revenue": {"id": DEFAULT_REVENUE_SEGMENT},
            "isInactive": False
        }
        
        if parent_customer_id:
            update_data["parent"] = {"id": parent_customer_id}
        
        waites_master_id = props.get('waites_master_id')
        if waites_master_id:
            update_data["custentity_wst_customer_waitesmasterid"] = waites_master_id
        
        # SALES REP
        hubspot_owner_id = props.get('hubspot_owner_id')
        if hubspot_owner_id and self.hubspot_client:
            owner_email = self.hubspot_client.get_owner_email(hubspot_owner_id)
            if owner_email:
                netsuite_employee_id = self._find_employee_by_email(owner_email)
                if netsuite_employee_id:
                    update_data["salesRep"] = {"id": netsuite_employee_id}
                elif DEFAULT_SALES_REP_ID:
                    update_data["salesRep"] = {"id": str(DEFAULT_SALES_REP_ID)}
        elif DEFAULT_SALES_REP_ID:
            update_data["salesRep"] = {"id": str(DEFAULT_SALES_REP_ID)}
        
        try:
            result = self.rest_client.update_customer(customer_id, update_data)
            
            if result.get('success'):
                logger.success(f"✅ Updated customer: {entity_name}")
                return {
                    "success": True,
                    "customer_id": customer_id,
                    "data": result,
                    "action": "updated_existing"
                }
            else:
                return {"success": False, "error": result.get('error', 'Unknown error')}
                
        except Exception as e:
            logger.error("Failed to update customer", e)
            return {"success": False, "error": str(e)}
    
    def _find_customer_by_name(self, customer_name: str) -> Optional[Dict[str, Any]]:
        """Search for customer by company name"""
        if not customer_name:
            return None
        
        try:
            result = self.rest_client.find_customer_by_name(customer_name)
            if result and result.get('id'):
                return result
            
            # Try REST API search
            rest_result = self.rest_client.search_records(
                "customer", 
                filters={"companyName": customer_name},
                limit=5
            )
            
            if rest_result.get('items') and len(rest_result['items']) > 0:
                return rest_result['items'][0]
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Customer search failed: {str(e)}")
            return None
    
    def get_record(self, record_type: str, record_id: str) -> Optional[Dict[str, Any]]:
        """Get any NetSuite record by type and ID"""
        cache_key = f"{record_type}:{record_id}"
        if cache_key in self._failed_cache:
            return None
        
        try:
            result = self.rest_client.get_record(record_type, record_id)
            
            if result and result.get('id'):
                return result
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve {record_type} {record_id}", e)
            self._add_to_failed_cache(cache_key)
            return None
    
    def _find_employee_by_email(self, email: str) -> Optional[str]:
        """Find NetSuite employee by email"""
        if not email:
            return None
        
        try:
            result = self.rest_client.find_employee_by_email(email)
            
            if result and result.get('id'):
                return str(result.get('id'))
            
            # Fallback mappings
            employee_mappings = {
                "don.erb@waites.net": "46109",
            }
            if email.lower() in employee_mappings:
                return employee_mappings[email.lower()]
            
            if DEFAULT_SALES_REP_ID:
                return DEFAULT_SALES_REP_ID
            
            return None
            
        except Exception as e:
            logger.error(f"Employee lookup failed: {str(e)}")
            return None
