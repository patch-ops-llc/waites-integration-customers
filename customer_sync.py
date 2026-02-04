# customer_sync.py
# Customer Sync from HubDB to NetSuite
# Replaces the Google Apps Script + Zapier workflow

import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import requests

from constants import HUBSPOT_PAT, HUBSPOT_BASE_URL, MAX_RETRIES, RETRY_DELAY, HUBSPOT_FACILITY_OBJECT_TYPE_ID
from netsuite_client import NetSuiteClient
from hubspot_client import HubSpotClient
from redis_client import get_redis_client, CustomerSyncRow, RedisClient
from logger import logger


@dataclass
class CustomerSyncConfig:
    """Configuration for customer sync"""
    hubdb_table_id: str = ""  # Set via environment variable
    rows_per_batch: int = 5
    page_size: int = 100
    auto_create_customer: bool = True


class CustomerSync:
    """
    HubDB to NetSuite Customer Sync
    
    Workflow:
    1. Fetch customers from HubDB table
    2. Store in Redis (replaces Google Sheet)
    3. Process batches of customers to NetSuite
    4. Update sync status in Redis
    
    Similar to the Google Apps Script but:
    - Uses Redis instead of Google Sheets
    - Calls NetSuite directly instead of Zapier
    - Runs on Railway with scheduled jobs
    """
    
    def __init__(self, config: Optional[CustomerSyncConfig] = None):
        import os
        
        self.config = config or CustomerSyncConfig()
        
        # Load HubDB table ID from environment
        if not self.config.hubdb_table_id:
            self.config.hubdb_table_id = os.getenv('HUBDB_TABLE_ID', '')
        
        self.hubspot_headers = {
            "Authorization": f"Bearer {HUBSPOT_PAT}",
            "Content-Type": "application/json"
        }
        
        self._processing_lock = threading.Lock()
        self._is_processing = False
    
    # =========================================================================
    # HubDB Operations
    # =========================================================================
    
    def update_hubdb_row_netsuite_id(self, hubdb_row_id: str, netsuite_id: str) -> bool:
        """
        Update a HubDB row with the NetSuite customer ID.
        This writes the NetSuite ID back to the HubDB table.
        
        Args:
            hubdb_row_id: The HubDB row ID to update
            netsuite_id: The NetSuite customer ID to write
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.hubdb_table_id or not hubdb_row_id or not netsuite_id:
            logger.warning("Missing required parameters for HubDB row update")
            return False
        
        logger.info(f"📝 Updating HubDB row {hubdb_row_id} with NetSuite ID: {netsuite_id}")
        
        url = f"{HUBSPOT_BASE_URL}/cms/v3/hubdb/tables/{self.config.hubdb_table_id}/rows/{hubdb_row_id}/draft"
        
        # Update the netsuite_id column in the HubDB row
        payload = {
            "values": {
                "netsuite_id": netsuite_id
            }
        }
        
        try:
            response = self._make_request("PATCH", url, json=payload)
            if response:
                logger.success(f"✅ Updated HubDB row {hubdb_row_id} draft with NetSuite ID: {netsuite_id}")
                
                # FIX Issue 6: Publish the draft to make changes live
                # CRITICAL: If publish fails, changes are NOT visible!
                publish_result = self._publish_hubdb_table()
                if publish_result:
                    logger.success(f"✅ Published HubDB table - changes are now live")
                    return True
                else:
                    # FIX: Return False if publish fails - changes aren't visible
                    logger.error("❌ HubDB row updated in draft, but PUBLISH FAILED")
                    logger.error("   Changes are NOT visible until table is published!")
                    return False
            else:
                logger.error(f"❌ Failed to update HubDB row {hubdb_row_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Exception updating HubDB row {hubdb_row_id}: {e}")
            return False
    
    def _publish_hubdb_table(self) -> bool:
        """
        Publish the HubDB table to make draft changes live.
        
        FIX Issue 6: Improved logging and error handling for publish failures.
        Draft changes are NOT visible until published!
        """
        if not self.config.hubdb_table_id:
            logger.error("❌ Cannot publish HubDB table: table ID not configured")
            return False
        
        url = f"{HUBSPOT_BASE_URL}/cms/v3/hubdb/tables/{self.config.hubdb_table_id}/draft/publish"
        
        logger.info(f"📤 Publishing HubDB table {self.config.hubdb_table_id}...")
        
        try:
            response = self._make_request("POST", url)
            if response is not None:
                # Check if response indicates success
                table_id = response.get('id') or response.get('tableId')
                row_count = response.get('rowCount', 'unknown')
                logger.success(f"✅ Published HubDB table (ID: {table_id}, Rows: {row_count})")
                return True
            else:
                logger.error("❌ HubDB publish failed: No response from API")
                logger.error("   CRITICAL: Draft changes are NOT visible until table is published!")
                logger.error(f"   Manual action: Go to HubSpot > Marketing > Files and Templates > HubDB")
                logger.error(f"   and publish table ID {self.config.hubdb_table_id}")
                return False
        except Exception as e:
            logger.error(f"❌ Exception publishing HubDB table: {e}")
            logger.error("   CRITICAL: Draft changes are NOT visible until table is published!")
            logger.error(f"   Manual action: Go to HubSpot > Marketing > Files and Templates > HubDB")
            logger.error(f"   and publish table ID {self.config.hubdb_table_id}")
            return False
    
    def find_and_update_hubdb_by_hubspot_id(self, hubspot_id: str, netsuite_id: str, 
                                             waites_id: Optional[str] = None) -> bool:
        """
        Find a HubDB row by HubSpot ID (or Waites ID fallback) and update it with the NetSuite ID.
        This is used by deal_sync to update HubDB when creating customers via deals.
        
        FIX: Enhanced to also search by customer_id (Waites ID) if hs_id lookup fails.
        This addresses the automation gap where NS IDs weren't making it to HubDB.
        
        Args:
            hubspot_id: The HubSpot company/entity ID to search for
            netsuite_id: The NetSuite customer ID to write
            waites_id: Optional Waites Master ID to use as fallback for lookup
            
        Returns:
            True if found and updated, False otherwise
        """
        if not self.config.hubdb_table_id or not hubspot_id or not netsuite_id:
            logger.debug("Missing required parameters for HubDB lookup")
            return False
        
        logger.info(f"🔍 Looking up HubDB row for HubSpot ID: {hubspot_id}")
        
        # Search HubDB for the row with matching hs_id or hubspot_id
        rows = self.fetch_hubdb_rows()
        
        # First pass: Try to find by hs_id/hubspot_id/hubspot_record_id
        for row in rows:
            values = row.get('values', {})
            # Check multiple possible column names for HubSpot ID
            row_hs_id = (values.get('hs_id') or 
                        values.get('hubspot_id') or 
                        values.get('hubspot_record_id') or '')
            
            if row_hs_id and str(row_hs_id) == str(hubspot_id):
                hubdb_row_id = str(row.get('id', ''))
                logger.info(f"✅ Found HubDB row {hubdb_row_id} for HubSpot ID {hubspot_id}")
                return self.update_hubdb_row_netsuite_id(hubdb_row_id, netsuite_id)
        
        # Second pass: Try to find by customer_id (Waites ID) if provided
        if waites_id:
            logger.info(f"🔍 HubSpot ID not found, trying Waites ID lookup: {waites_id}")
            for row in rows:
                values = row.get('values', {})
                row_customer_id = values.get('customer_id', values.get('waites_id', ''))
                
                if row_customer_id and str(row_customer_id) == str(waites_id):
                    hubdb_row_id = str(row.get('id', ''))
                    row_netsuite_id = values.get('netsuite_id', '')
                    
                    # Only update if the row doesn't already have a NetSuite ID
                    if not row_netsuite_id:
                        logger.info(f"✅ Found HubDB row {hubdb_row_id} by Waites ID {waites_id}")
                        return self.update_hubdb_row_netsuite_id(hubdb_row_id, netsuite_id)
                    else:
                        logger.info(f"ℹ️ HubDB row {hubdb_row_id} already has NetSuite ID: {row_netsuite_id}")
                        return True  # Already has NS ID, consider this a success
        
        logger.info(f"ℹ️ No HubDB row found for HubSpot ID: {hubspot_id}" + 
                   (f" or Waites ID: {waites_id}" if waites_id else ""))
        return False
    
    def find_and_update_hubdb_by_waites_id(self, waites_id: str, netsuite_id: str,
                                            hubspot_id: Optional[str] = None) -> bool:
        """
        Find a HubDB row by Waites Customer ID and update it with the NetSuite ID.
        Also optionally updates the hs_id field if provided.
        
        This is an alternative lookup method when the HubSpot ID isn't available
        or isn't stored in the HubDB row.
        
        Args:
            waites_id: The Waites Customer ID (customer_id column in HubDB)
            netsuite_id: The NetSuite customer ID to write
            hubspot_id: Optional HubSpot ID to also update in the row
            
        Returns:
            True if found and updated, False otherwise
        """
        if not self.config.hubdb_table_id or not waites_id or not netsuite_id:
            logger.debug("Missing required parameters for HubDB Waites ID lookup")
            return False
        
        logger.info(f"🔍 Looking up HubDB row for Waites ID: {waites_id}")
        
        rows = self.fetch_hubdb_rows()
        
        for row in rows:
            values = row.get('values', {})
            row_customer_id = values.get('customer_id') or values.get('waites_id') or ''
            
            if row_customer_id and str(row_customer_id) == str(waites_id):
                hubdb_row_id = str(row.get('id', ''))
                existing_netsuite_id = values.get('netsuite_id', '')
                
                if existing_netsuite_id:
                    logger.info(f"ℹ️ HubDB row {hubdb_row_id} already has NetSuite ID: {existing_netsuite_id}")
                    return True
                
                logger.info(f"✅ Found HubDB row {hubdb_row_id} for Waites ID {waites_id}")
                
                # Update with NetSuite ID (and optionally HubSpot ID)
                update_values = {"netsuite_id": netsuite_id}
                if hubspot_id:
                    # Check multiple possible column names for existing HS ID
                    existing_hs_id = (values.get('hs_id') or 
                                     values.get('hubspot_id') or 
                                     values.get('hubspot_record_id') or '')
                    if not existing_hs_id:
                        # Use hubspot_record_id since that's what the table uses
                        update_values["hubspot_record_id"] = hubspot_id
                        logger.info(f"   Also updating hubspot_record_id to: {hubspot_id}")
                
                return self._update_hubdb_row_values(hubdb_row_id, update_values)
        
        logger.info(f"ℹ️ No HubDB row found for Waites ID: {waites_id}")
        return False
    
    def _update_hubdb_row_values(self, hubdb_row_id: str, values: Dict[str, str]) -> bool:
        """
        Update multiple values in a HubDB row.
        
        Args:
            hubdb_row_id: The HubDB row ID to update
            values: Dict of column names to values to update
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.hubdb_table_id or not hubdb_row_id or not values:
            logger.warning("Missing required parameters for HubDB row update")
            return False
        
        logger.info(f"📝 Updating HubDB row {hubdb_row_id} with values: {values}")
        
        url = f"{HUBSPOT_BASE_URL}/cms/v3/hubdb/tables/{self.config.hubdb_table_id}/rows/{hubdb_row_id}/draft"
        
        payload = {"values": values}
        
        try:
            # Make direct request for better error visibility
            import json as json_lib
            response = requests.patch(
                url,
                headers=self.hubspot_headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code >= 400:
                logger.error(f"❌ HubDB update failed with status {response.status_code}")
                logger.error(f"   Response: {response.text[:500]}")
                return False
            
            result = response.json()
            logger.success(f"✅ Updated HubDB row {hubdb_row_id} draft")
            
            # Publish the draft to make changes live
            publish_result = self._publish_hubdb_table()
            if publish_result:
                logger.success(f"✅ Published HubDB table - changes are now live")
                return True
            else:
                logger.error("❌ HubDB row updated in draft, but PUBLISH FAILED")
                return False
                
        except requests.RequestException as e:
            logger.error(f"❌ Request exception updating HubDB row {hubdb_row_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Exception updating HubDB row {hubdb_row_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def fetch_hubdb_rows(self) -> List[Dict[str, Any]]:
        """
        Fetch all rows from HubDB table.
        Equivalent to fetchHubDBRows() in the Apps Script.
        """
        if not self.config.hubdb_table_id:
            logger.error("❌ HUBDB_TABLE_ID not configured")
            return []
        
        logger.info(f"📥 Fetching HubDB rows from table {self.config.hubdb_table_id}...")
        
        all_rows = []
        after = None
        page = 1
        
        base_url = f"{HUBSPOT_BASE_URL}/cms/v3/hubdb/tables/{self.config.hubdb_table_id}/rows/draft"
        
        try:
            while True:
                url = f"{base_url}?limit={self.config.page_size}"
                if after:
                    url += f"&after={after}"
                
                response = self._make_request("GET", url)
                if not response:
                    break
                
                results = response.get('results', [])
                all_rows.extend(results)
                
                logger.info(f"✓ Page {page}: {len(results)} rows (total: {len(all_rows)})")
                
                after = response.get('paging', {}).get('next', {}).get('after')
                if not after:
                    break
                
                page += 1
                time.sleep(0.5)  # Rate limit protection
            
            logger.success(f"✅ Fetched {len(all_rows)} rows from HubDB")
            return all_rows
            
        except Exception as e:
            logger.error(f"Failed to fetch HubDB rows: {e}")
            return []
    
    def sync_hubdb_to_redis(self, preserve_existing: bool = True) -> Dict[str, Any]:
        """
        Sync HubDB data to Redis.
        Equivalent to syncHubDBToSheet() in the Apps Script.
        
        Args:
            preserve_existing: If True, don't reset already synced rows
            
        Returns:
            Sync result with counts
        """
        logger.info("=" * 60)
        logger.info("Syncing HubDB to Redis...")
        logger.info("=" * 60)
        
        rows = self.fetch_hubdb_rows()
        if not rows:
            return {
                'success': False,
                'message': 'No rows found in HubDB',
                'rows_fetched': 0
            }
        
        try:
            redis = get_redis_client()
            stats = redis.import_hubdb_rows(rows, preserve_existing=preserve_existing)
            
            logger.success(f"✅ Synced {len(rows)} rows to Redis")
            logger.info(f"   Added: {stats['added']}, Updated: {stats['updated']}, Preserved: {stats['preserved']}")
            
            return {
                'success': True,
                'message': f"Synced {len(rows)} rows to Redis",
                'rows_fetched': len(rows),
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Failed to sync to Redis: {e}")
            return {
                'success': False,
                'error': str(e),
                'rows_fetched': len(rows)
            }
    
    # =========================================================================
    # Batch Processing
    # =========================================================================
    
    def process_next_batch(self) -> Dict[str, Any]:
        """
        Process the next batch of pending customers.
        Equivalent to processNextBatch() in the Apps Script.
        
        Returns:
            Result with batch processing details
        """
        # Prevent concurrent processing
        with self._processing_lock:
            if self._is_processing:
                return {
                    'success': False,
                    'message': 'Batch processing already in progress'
                }
            self._is_processing = True
        
        try:
            return self._do_process_batch()
        finally:
            with self._processing_lock:
                self._is_processing = False
    
    def _do_process_batch(self) -> Dict[str, Any]:
        """Internal batch processing logic"""
        logger.info("=" * 60)
        logger.info("Processing Next Customer Batch...")
        logger.info("=" * 60)
        
        try:
            redis = get_redis_client()
            
            # Get pending rows
            pending_rows = redis.get_pending_rows(limit=self.config.rows_per_batch)
            
            if not pending_rows:
                stats = redis.get_stats()
                logger.info("✓ No pending rows - all customers have been processed!")
                return {
                    'success': True,
                    'message': 'All rows synced',
                    'rows_processed': 0,
                    'rows_remaining': 0,
                    'stats': stats
                }
            
            logger.info(f"📦 Processing {len(pending_rows)} customers...")
            
            # Mark as processing
            row_ids = [row.hubdb_row_id for row in pending_rows]
            redis.mark_rows_processing(row_ids)
            
            # Process each customer
            results = []
            hubspot = HubSpotClient()
            netsuite = NetSuiteClient(hubspot_client=hubspot)  # Pass HubSpot client for owner/sales rep lookup
            
            for row in pending_rows:
                result = self._sync_single_customer(row, netsuite, hubspot)
                results.append(result)
                
                # Update Redis with result
                redis.update_row_result(
                    hubdb_row_id=row.hubdb_row_id,
                    success=result['success'],
                    error=result.get('error', ''),
                    netsuite_id=result.get('netsuite_id', '')
                )
            
            # Get updated stats
            stats = redis.get_stats()
            
            success_count = sum(1 for r in results if r['success'])
            fail_count = len(results) - success_count
            
            logger.info(f"✅ Batch complete: {success_count} success, {fail_count} failed")
            logger.info(f"   Remaining: {stats.get('pending', 0)} pending rows")
            
            return {
                'success': True,
                'message': f'Processed {len(results)} customers',
                'rows_processed': len(results),
                'success_count': success_count,
                'fail_count': fail_count,
                'rows_remaining': stats.get('pending', 0),
                'results': results,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _sync_single_customer(self, row: CustomerSyncRow, 
                              netsuite: NetSuiteClient,
                              hubspot: HubSpotClient) -> Dict[str, Any]:
        """
        Sync a single customer to NetSuite.
        
        Args:
            row: Customer sync row from Redis
            netsuite: NetSuite client instance
            hubspot: HubSpot client instance
            
        Returns:
            Sync result dict
        """
        logger.info(f"🔄 Syncing customer: HubSpot ID {row.hubspot_id}")
        
        # Track if we're replacing an invalid ID (for Issue 7 fix)
        replacing_invalid_id = False
        original_invalid_id = None
        
        try:
            # Check if already has NetSuite ID
            if row.netsuite_id:
                logger.info(f"   Already has NetSuite ID: {row.netsuite_id}")
                
                # Verify it exists in NetSuite
                existing = netsuite.get_record("customer", row.netsuite_id)
                if existing:
                    logger.success(f"✅ Verified existing customer {row.netsuite_id}")
                    
                    # Get entity data to determine type and check for updates needed
                    entity_data = None
                    entity_type = "company"
                    verified_hubspot_id = row.hubspot_id
                    
                    if row.hubspot_id:
                        entity_data = hubspot.get_company(row.hubspot_id)
                    
                    # If no company found, search for facility by Waites ID
                    if not entity_data and row.customer_id:
                        entity_data = hubspot.search_facility_by_waites_id(row.customer_id)
                        entity_type = "facility"
                        if entity_data:
                            verified_hubspot_id = entity_data.get('id')
                    
                    # FIX: Update NetSuite customer with Waites Master ID if missing
                    # Check if NetSuite customer needs Waites Master ID update
                    existing_waites_id = existing.get('custentity_wst_customer_waitesmasterid', '')
                    waites_id_from_hubdb = row.customer_id  # customer_id column is the Waites ID
                    
                    if waites_id_from_hubdb and str(existing_waites_id) != str(waites_id_from_hubdb):
                        logger.info(f"📝 NetSuite customer {row.netsuite_id} missing/mismatched Waites Master ID")
                        logger.info(f"   Current: '{existing_waites_id}' -> New: '{waites_id_from_hubdb}'")
                        
                        # Build update data for NetSuite
                        ns_update_data = {
                            "custentity_wst_customer_waitesmasterid": waites_id_from_hubdb
                        }
                        
                        # Also check and update parent customer if this is a facility
                        if entity_type == "facility" and verified_hubspot_id:
                            parent_customer_id = self._get_parent_company_netsuite_id(verified_hubspot_id, hubspot)
                            if parent_customer_id:
                                existing_parent = existing.get('parent', {})
                                existing_parent_id = existing_parent.get('id') if isinstance(existing_parent, dict) else None
                                if str(existing_parent_id) != str(parent_customer_id):
                                    ns_update_data["parent"] = {"id": parent_customer_id}
                                    logger.info(f"🔗 Also updating parent customer to: {parent_customer_id}")
                        
                        try:
                            update_result = netsuite.rest_client.update_customer(row.netsuite_id, ns_update_data)
                            if update_result.get('success'):
                                logger.success(f"✅ Updated NetSuite customer {row.netsuite_id} with Waites Master ID: {waites_id_from_hubdb}")
                            else:
                                logger.warning(f"⚠️ Failed to update NetSuite customer: {update_result.get('error', 'Unknown error')}")
                        except Exception as ns_error:
                            logger.warning(f"⚠️ Exception updating NetSuite customer: {ns_error}")
                    
                    # Also ensure HubSpot has the NetSuite ID (it may be in HubDB but not HubSpot)
                    if entity_data:
                        props = entity_data.get('properties', {})
                        hs_netsuite_id = props.get('netsuite_record_id' if entity_type == 'company' else 'netsuite_customer_id', '')
                        
                        if not hs_netsuite_id or str(hs_netsuite_id) != str(row.netsuite_id):
                            logger.info(f"📝 HubSpot {entity_type} {verified_hubspot_id} missing NetSuite ID, updating...")
                            if entity_type == "facility":
                                hubspot.update_facility(verified_hubspot_id, row.netsuite_id)
                            else:
                                hubspot.update_company(verified_hubspot_id, row.netsuite_id)
                    
                    return {
                        'success': True,
                        'hubspot_id': verified_hubspot_id or row.hubspot_id,
                        'netsuite_id': row.netsuite_id,
                        'action': 'verified_existing'
                    }
                else:
                    # FIX Issue 7: Track that we're replacing an invalid ID
                    original_invalid_id = row.netsuite_id
                    replacing_invalid_id = True
                    logger.warning(f"⚠️ NetSuite ID {row.netsuite_id} not found in NetSuite, will create new customer")
                    logger.warning(f"   This stale ID will be replaced in both HubDB and HubSpot")
            
            # Get HubSpot company/facility data
            entity_data = None
            entity_type = "company"
            actual_hubspot_id = row.hubspot_id  # Track the actual HubSpot ID for updates
            
            # Try to get as company first (using HubSpot ID)
            if row.hubspot_id:
                entity_data = hubspot.get_company(row.hubspot_id)
            
            # If no company found and customer_id (Waites ID) is set, search for facility
            # IMPORTANT: row.customer_id contains Waites ID (e.g., "E7846"), not HubSpot ID
            # So we must SEARCH by waites_master_id property, not GET by ID
            if not entity_data and row.customer_id:
                logger.info(f"🔍 Searching for facility by Waites ID: {row.customer_id}")
                entity_data = hubspot.search_facility_by_waites_id(row.customer_id)
                entity_type = "facility"
                
                # Update actual_hubspot_id with the found facility's HubSpot ID
                if entity_data:
                    actual_hubspot_id = entity_data.get('id')
                    logger.info(f"✅ Found facility HubSpot ID: {actual_hubspot_id}")
            
            # Last resort: Try to get facility directly by HubSpot ID if we have one
            if not entity_data and row.hubspot_id:
                entity_data = hubspot.get_facility(row.hubspot_id)
                entity_type = "facility"
                if entity_data:
                    actual_hubspot_id = row.hubspot_id
            
            if not entity_data:
                error_msg = f"HubSpot entity not found: HubSpot ID={row.hubspot_id}, Waites ID={row.customer_id}"
                logger.error(f"❌ {error_msg}")
                return {
                    'success': False,
                    'hubspot_id': row.hubspot_id,
                    'error': error_msg
                }
            
            # Get parent company NetSuite ID for subcustomer support
            # IMPORTANT: Use actual_hubspot_id (the HubSpot internal ID), not row.customer_id (Waites ID)
            parent_customer_id = None
            if entity_type == "facility" and actual_hubspot_id:
                parent_customer_id = self._get_parent_company_netsuite_id(actual_hubspot_id, hubspot)
                if parent_customer_id:
                    logger.info(f"🔗 Facility will be created as subcustomer of company (NS ID: {parent_customer_id})")
            
            # Upsert to NetSuite
            # Use actual_hubspot_id which is the verified HubSpot internal ID
            result = netsuite.upsert_customer(
                entity_data=entity_data,
                hubspot_entity_id=actual_hubspot_id or row.hubspot_id or row.customer_id,
                entity_type=entity_type,
                parent_customer_id=parent_customer_id
            )
            
            if result.get('success'):
                netsuite_id = result.get('customer_id', '')
                action = result.get('action', 'synced')
                
                # FIX Issue 7: Log when we're replacing an invalid ID
                if replacing_invalid_id:
                    logger.warning(f"🔄 REPLACING invalid NetSuite ID: {original_invalid_id} → {netsuite_id}")
                    action = 'replaced_invalid_id'
                
                logger.success(f"✅ Synced customer to NetSuite: {netsuite_id} ({action})")
                
                # Update HubSpot with NetSuite ID if needed
                # FIX: Check return values and use actual HubSpot ID (not Waites ID)
                # FIX Issue 7: Always update when replacing invalid ID
                hubspot_update_success = True
                needs_update = result.get('needs_hubspot_update') or replacing_invalid_id
                if needs_update and netsuite_id and actual_hubspot_id:
                    if entity_type == "facility":
                        # Use actual_hubspot_id which is the verified HubSpot internal ID
                        hubspot_update_success = hubspot.update_facility(actual_hubspot_id, netsuite_id)
                    else:
                        hubspot_update_success = hubspot.update_company(actual_hubspot_id, netsuite_id)
                    
                    # FIX Issue 1: Check return value and fail if update failed
                    if hubspot_update_success:
                        logger.success(f"✅ Updated HubSpot {entity_type} {actual_hubspot_id} with NetSuite ID")
                    else:
                        logger.error(f"❌ Failed to update HubSpot {entity_type} {actual_hubspot_id} with NetSuite ID: {netsuite_id}")
                        return {
                            'success': False,
                            'hubspot_id': actual_hubspot_id,
                            'netsuite_id': netsuite_id,
                            'error': f'Failed to update HubSpot {entity_type} with NetSuite ID'
                        }
                
                # CRITICAL: Update HubDB row with NetSuite ID
                # FIX Issue 2: Fail sync if HubDB update fails (not just warning)
                if netsuite_id and row.hubdb_row_id:
                    hubdb_updated = self.update_hubdb_row_netsuite_id(row.hubdb_row_id, netsuite_id)
                    if hubdb_updated:
                        logger.success(f"✅ Updated HubDB row {row.hubdb_row_id} with NetSuite ID: {netsuite_id}")
                    else:
                        logger.error(f"❌ Failed to update HubDB row {row.hubdb_row_id} with NetSuite ID: {netsuite_id}")
                        return {
                            'success': False,
                            'hubspot_id': row.hubspot_id,
                            'netsuite_id': netsuite_id,
                            'error': f'Failed to update HubDB row with NetSuite ID'
                        }
                
                return {
                    'success': True,
                    'hubspot_id': actual_hubspot_id or row.hubspot_id,
                    'netsuite_id': netsuite_id,
                    'action': action
                }
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"❌ Failed to sync customer: {error_msg}")
                return {
                    'success': False,
                    'hubspot_id': actual_hubspot_id or row.hubspot_id,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.error(f"❌ Exception syncing customer {row.hubspot_id}: {e}")
            return {
                'success': False,
                'hubspot_id': row.hubspot_id,
                'error': str(e)
            }
    
    # =========================================================================
    # Status & Management
    # =========================================================================
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current sync status.
        Equivalent to checkStatus() in the Apps Script.
        """
        try:
            redis = get_redis_client()
            stats = redis.get_stats()
            
            logger.info("=" * 60)
            logger.info("CUSTOMER SYNC STATUS")
            logger.info("=" * 60)
            logger.info(f"Total rows: {stats.get('total', 0)}")
            logger.info(f"Pending: {stats.get('pending', 0)}")
            logger.info(f"Processing: {stats.get('processing', 0)}")
            logger.info(f"Completed: {stats.get('completed', 0)}")
            logger.info(f"Failed: {stats.get('failed', 0)}")
            logger.info(f"Last HubDB sync: {stats.get('last_hubdb_sync', 'Never')}")
            logger.info("=" * 60)
            
            return {
                'success': True,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Failed to get status: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def reset_all(self) -> Dict[str, Any]:
        """Reset all sync data"""
        try:
            redis = get_redis_client()
            redis.reset_all()
            return {
                'success': True,
                'message': 'All customer sync data has been reset'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def reset_failed(self) -> Dict[str, Any]:
        """Reset failed rows back to pending"""
        try:
            redis = get_redis_client()
            count = redis.reset_failed_rows()
            return {
                'success': True,
                'message': f'Reset {count} failed rows to pending',
                'count': count
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _get_parent_company_netsuite_id(self, facility_id: str, hubspot: HubSpotClient) -> Optional[str]:
        """Get the NetSuite customer ID of the company associated with a facility.
        
        This enables subcustomer support: when a facility has an associated company
        that already exists in NetSuite, the facility will be created as a subcustomer
        of that company.
        
        Args:
            facility_id: HubSpot facility ID
            hubspot: HubSpot client instance
            
        Returns:
            NetSuite customer ID of the parent company, or None if not found
        """
        # Get the company associated with this facility
        company_id = hubspot.get_associated_company_for_facility(facility_id)
        if not company_id:
            logger.info("No company associated with facility - will create as top-level customer")
            return None
        
        # Get the company data to check for NetSuite ID
        company_data = hubspot.get_company(company_id)
        if not company_data:
            logger.warning(f"Could not retrieve company {company_id}")
            return None
        
        # Check if the company has a NetSuite ID
        company_props = company_data.get('properties', {})
        netsuite_record_id = company_props.get('netsuite_record_id')
        
        if netsuite_record_id:
            # Verify the customer exists in NetSuite
            netsuite = NetSuiteClient()
            existing_customer = netsuite.get_record("customer", netsuite_record_id)
            if existing_customer:
                company_name = company_props.get('name', 'Unknown Company')
                logger.success(f"✅ Found parent company in NetSuite: {company_name} (ID: {netsuite_record_id})")
                return netsuite_record_id
            else:
                logger.warning(f"⚠️ Company has NetSuite ID {netsuite_record_id} but customer not found in NetSuite")
                return None
        else:
            company_name = company_props.get('name', 'Unknown Company')
            logger.info(f"Associated company '{company_name}' does not have a NetSuite ID yet - will create facility as top-level customer")
            return None
    
    def process_all(self, max_batches: int = 100) -> Dict[str, Any]:
        """
        Process all pending customers in multiple batches.
        Useful for initial sync or catch-up.
        
        Args:
            max_batches: Maximum number of batches to process
            
        Returns:
            Overall result
        """
        logger.info("=" * 60)
        logger.info("Processing ALL pending customers...")
        logger.info("=" * 60)
        
        total_processed = 0
        total_success = 0
        total_failed = 0
        batches_run = 0
        
        for batch_num in range(max_batches):
            result = self.process_next_batch()
            
            if not result.get('success'):
                if 'already in progress' in result.get('message', ''):
                    continue
                break
            
            rows_processed = result.get('rows_processed', 0)
            if rows_processed == 0:
                logger.info(f"✅ All customers processed after {batches_run} batches")
                break
            
            total_processed += rows_processed
            total_success += result.get('success_count', 0)
            total_failed += result.get('fail_count', 0)
            batches_run += 1
            
            logger.info(f"   Batch {batches_run}: {rows_processed} processed")
            
            # Small delay between batches
            time.sleep(1)
        
        return {
            'success': True,
            'message': f'Processed {total_processed} customers in {batches_run} batches',
            'total_processed': total_processed,
            'total_success': total_success,
            'total_failed': total_failed,
            'batches_run': batches_run
        }
    
    # =========================================================================
    # Reconciliation: Backfill missing NetSuite IDs from HubSpot to HubDB
    # =========================================================================
    
    def reconcile_hubdb_netsuite_ids(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        Reconcile HubDB table with HubSpot records to backfill missing NetSuite IDs.
        
        This addresses the automation gap where:
        - A HubSpot facility/company has both a Waites ID and NetSuite ID
        - But the corresponding HubDB row only has the Waites ID (missing the NS ID)
        
        The method:
        1. Fetches all HubDB rows
        2. For each row with a Waites ID or HubSpot ID but NO NetSuite ID:
           a. Looks up the corresponding HubSpot facility/company record
           b. If the HubSpot record has a NetSuite ID, updates the HubDB row
        
        Args:
            dry_run: If True, only report gaps without making changes
            
        Returns:
            Dict with reconciliation results
        """
        logger.info("=" * 60)
        logger.info(f"RECONCILING HubDB NetSuite IDs {'(DRY RUN)' if dry_run else '(LIVE)'}")
        logger.info("=" * 60)
        
        hubspot = HubSpotClient()
        
        # Fetch all HubDB rows
        rows = self.fetch_hubdb_rows()
        if not rows:
            return {
                'success': False,
                'message': 'No HubDB rows found or failed to fetch',
                'gaps_found': 0,
                'gaps_fixed': 0
            }
        
        logger.info(f"📊 Checking {len(rows)} HubDB rows for missing NetSuite IDs...")
        
        gaps_found = []
        gaps_fixed = 0
        gaps_failed = 0
        already_has_ns_id = 0
        no_hubspot_record = 0
        hubspot_missing_ns_id = 0
        
        for row in rows:
            row_id = str(row.get('id', ''))
            values = row.get('values', {})
            
            # Check multiple possible column names
            waites_id = values.get('customer_id') or values.get('waites_id') or ''
            hs_id = (values.get('hs_id') or 
                    values.get('hubspot_id') or 
                    values.get('hubspot_record_id') or '')
            netsuite_id = values.get('netsuite_id', '')
            
            # Skip if already has NetSuite ID
            if netsuite_id:
                already_has_ns_id += 1
                continue
            
            # Skip if no identifying info
            if not waites_id and not hs_id:
                continue
            
            # Try to find the HubSpot record and get its NetSuite ID
            hubspot_netsuite_id = None
            entity_type = None
            entity_name = None
            
            # First try by HubSpot ID (facility)
            if hs_id:
                facility_data = hubspot.get_facility(hs_id)
                if facility_data:
                    props = facility_data.get('properties', {})
                    hubspot_netsuite_id = props.get('netsuite_customer_id', '')
                    entity_type = 'facility'
                    entity_name = props.get('name', 'Unknown')
                else:
                    # Try as company
                    company_data = hubspot.get_company(hs_id)
                    if company_data:
                        props = company_data.get('properties', {})
                        hubspot_netsuite_id = props.get('netsuite_record_id', '')
                        entity_type = 'company'
                        entity_name = props.get('name', 'Unknown')
            
            # If no HubSpot record found by hs_id, we'd need to search by Waites ID
            # This is more complex as it requires searching HubSpot by custom property
            if not entity_type and waites_id:
                # Try to find facility by waites_master_id property
                facility_data = self._find_hubspot_entity_by_waites_id(hubspot, waites_id, 'facility')
                if facility_data:
                    props = facility_data.get('properties', {})
                    hubspot_netsuite_id = props.get('netsuite_customer_id', '')
                    entity_type = 'facility'
                    entity_name = props.get('name', 'Unknown')
                    hs_id = facility_data.get('id', '')
                else:
                    # Try company
                    company_data = self._find_hubspot_entity_by_waites_id(hubspot, waites_id, 'company')
                    if company_data:
                        props = company_data.get('properties', {})
                        hubspot_netsuite_id = props.get('netsuite_record_id', '')
                        entity_type = 'company'
                        entity_name = props.get('name', 'Unknown')
                        hs_id = company_data.get('id', '')
            
            if not entity_type:
                no_hubspot_record += 1
                continue
            
            if not hubspot_netsuite_id:
                hubspot_missing_ns_id += 1
                continue
            
            # Found a gap! HubSpot has NS ID but HubDB doesn't
            gap_info = {
                'hubdb_row_id': row_id,
                'waites_id': waites_id,
                'hs_id': hs_id,
                'entity_type': entity_type,
                'entity_name': entity_name,
                'netsuite_id_from_hubspot': hubspot_netsuite_id
            }
            gaps_found.append(gap_info)
            
            logger.info(f"🔍 Gap found: {entity_name} (Waites: {waites_id}, HS: {hs_id}) - NS ID: {hubspot_netsuite_id}")
            
            if not dry_run:
                # Update the HubDB row with the NetSuite ID
                update_values = {'netsuite_id': hubspot_netsuite_id}
                
                # Also update hs_id if it's missing
                if not values.get('hs_id') and hs_id:
                    update_values['hs_id'] = hs_id
                
                if self._update_hubdb_row_values(row_id, update_values):
                    gaps_fixed += 1
                    logger.success(f"   ✅ Fixed: Updated HubDB row {row_id}")
                else:
                    gaps_failed += 1
                    logger.error(f"   ❌ Failed to update HubDB row {row_id}")
                
                # Rate limit protection
                time.sleep(0.5)
        
        # Summary
        logger.info("=" * 60)
        logger.info("RECONCILIATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total rows checked: {len(rows)}")
        logger.info(f"Already have NetSuite ID: {already_has_ns_id}")
        logger.info(f"Gaps found: {len(gaps_found)}")
        if not dry_run:
            logger.info(f"Gaps fixed: {gaps_fixed}")
            logger.info(f"Fix failures: {gaps_failed}")
        logger.info(f"No HubSpot record found: {no_hubspot_record}")
        logger.info(f"HubSpot record missing NS ID: {hubspot_missing_ns_id}")
        
        return {
            'success': True,
            'dry_run': dry_run,
            'total_rows': len(rows),
            'already_has_ns_id': already_has_ns_id,
            'gaps_found': len(gaps_found),
            'gaps_fixed': gaps_fixed if not dry_run else 0,
            'gaps_failed': gaps_failed if not dry_run else 0,
            'no_hubspot_record': no_hubspot_record,
            'hubspot_missing_ns_id': hubspot_missing_ns_id,
            'gap_details': gaps_found[:50]  # Limit to first 50 for response size
        }
    
    def _find_hubspot_entity_by_waites_id(self, hubspot: HubSpotClient, waites_id: str, 
                                         entity_type: str) -> Optional[Dict[str, Any]]:
        """
        Find a HubSpot entity (facility or company) by Waites Master ID.
        
        Args:
            hubspot: HubSpot client instance
            waites_id: The Waites Master ID to search for
            entity_type: 'facility' or 'company'
            
        Returns:
            Entity data dict or None if not found
        """
        if not waites_id:
            return None
        
        try:
            if entity_type == 'facility':
                object_type = HUBSPOT_FACILITY_OBJECT_TYPE_ID
                property_name = 'waites_master_id'
                properties = 'name,netsuite_customer_id,waites_master_id'
            else:
                object_type = 'companies'
                property_name = 'waites_master_id'
                properties = 'name,netsuite_record_id,waites_master_id'
            
            url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/{object_type}/search"
            payload = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": property_name,
                        "operator": "EQ",
                        "value": waites_id
                    }]
                }],
                "properties": properties.split(','),
                "limit": 1
            }
            
            response = self._make_request("POST", url, json=payload)
            if response and response.get('results'):
                return response['results'][0]
            
        except Exception as e:
            logger.debug(f"Error searching for {entity_type} by Waites ID: {e}")
        
        return None
    
    # =========================================================================
    # HTTP Helpers
    # =========================================================================
    
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.request(
                    method, 
                    url, 
                    headers=self.hubspot_headers,
                    **kwargs
                )
                
                if response.status_code == 429:
                    # Rate limited
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY * 5))
                    logger.warning(f"Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Request failed, retrying in {delay}s: {e}")
                    time.sleep(delay)
                    continue
                logger.error(f"Request failed after {MAX_RETRIES} attempts: {e}")
                return None
        
        return None


# Background processing support
_background_thread: Optional[threading.Thread] = None
_stop_background = False


def start_background_sync(interval_seconds: int = 300):
    """
    Start background sync processing.
    Processes batches on a schedule (similar to Apps Script trigger).
    
    Args:
        interval_seconds: Seconds between batch processing (default 5 minutes)
    """
    global _background_thread, _stop_background
    
    if _background_thread and _background_thread.is_alive():
        logger.warning("Background sync already running")
        return
    
    _stop_background = False
    
    def background_worker():
        logger.info(f"🔄 Background customer sync started (interval: {interval_seconds}s)")
        sync = CustomerSync()
        
        while not _stop_background:
            try:
                result = sync.process_next_batch()
                if result.get('rows_processed', 0) > 0:
                    logger.info(f"Background sync: {result.get('rows_processed')} processed")
            except Exception as e:
                logger.error(f"Background sync error: {e}")
            
            # Sleep in small chunks to allow quick shutdown
            for _ in range(interval_seconds):
                if _stop_background:
                    break
                time.sleep(1)
        
        logger.info("🛑 Background customer sync stopped")
    
    _background_thread = threading.Thread(target=background_worker, daemon=True)
    _background_thread.start()


def stop_background_sync():
    """Stop background sync processing"""
    global _stop_background
    _stop_background = True
    logger.info("Stopping background customer sync...")



