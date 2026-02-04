# redis_client.py
# Redis client for customer sync state management

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from logger import logger


@dataclass
class CustomerSyncRow:
    """Represents a row in the customer sync queue"""
    hubdb_row_id: str
    hubspot_id: str
    netsuite_id: str = ""
    customer_id: str = ""
    synced_date: str = ""
    status: str = ""  # pending, processing, Success, Failed
    sync_completed: str = ""
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CustomerSyncRow':
        return cls(
            hubdb_row_id=data.get('hubdb_row_id', ''),
            hubspot_id=data.get('hubspot_id', ''),
            netsuite_id=data.get('netsuite_id', ''),
            customer_id=data.get('customer_id', ''),
            synced_date=data.get('synced_date', ''),
            status=data.get('status', ''),
            sync_completed=data.get('sync_completed', ''),
            notes=data.get('notes', ''),
            created_at=data.get('created_at', ''),
            updated_at=data.get('updated_at', '')
        )


class RedisClient:
    """Redis client for customer sync state management."""
    
    ROWS_KEY_PREFIX = "customer_sync:rows:"
    PENDING_SET = "customer_sync:pending"
    PROCESSING_SET = "customer_sync:processing"
    COMPLETED_SET = "customer_sync:completed"
    FAILED_SET = "customer_sync:failed"
    STATS_KEY = "customer_sync:stats"
    LAST_SYNC_KEY = "customer_sync:last_hubdb_sync"
    
    def __init__(self, redis_url: Optional[str] = None):
        if not REDIS_AVAILABLE:
            raise ImportError("Redis package not installed. Run: pip install redis")
        
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379')
        
        try:
            self.client = redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            self.client.ping()
            logger.info("✅ Redis client connected")
        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise
    
    def upsert_row(self, row: CustomerSyncRow) -> bool:
        """Insert or update a customer sync row"""
        try:
            key = f"{self.ROWS_KEY_PREFIX}{row.hubdb_row_id}"
            row.updated_at = datetime.utcnow().isoformat()
            if not row.created_at:
                row.created_at = row.updated_at
            
            self.client.hset(key, mapping=row.to_dict())
            self._update_status_sets(row.hubdb_row_id, row.status)
            
            return True
        except Exception as e:
            logger.error(f"Failed to upsert row {row.hubdb_row_id}: {e}")
            return False
    
    def get_row(self, hubdb_row_id: str) -> Optional[CustomerSyncRow]:
        """Get a single row by HubDB row ID"""
        try:
            key = f"{self.ROWS_KEY_PREFIX}{hubdb_row_id}"
            data = self.client.hgetall(key)
            if data:
                return CustomerSyncRow.from_dict(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get row {hubdb_row_id}: {e}")
            return None
    
    def get_all_rows(self) -> List[CustomerSyncRow]:
        """Get all customer sync rows"""
        rows = []
        try:
            pattern = f"{self.ROWS_KEY_PREFIX}*"
            for key in self.client.scan_iter(pattern):
                data = self.client.hgetall(key)
                if data:
                    rows.append(CustomerSyncRow.from_dict(data))
        except Exception as e:
            logger.error(f"Failed to get all rows: {e}")
        return rows
    
    def _update_status_sets(self, hubdb_row_id: str, status: str):
        """Update the status sets based on row status"""
        self.client.srem(self.PENDING_SET, hubdb_row_id)
        self.client.srem(self.PROCESSING_SET, hubdb_row_id)
        self.client.srem(self.COMPLETED_SET, hubdb_row_id)
        self.client.srem(self.FAILED_SET, hubdb_row_id)
        
        if status == 'pending':
            self.client.sadd(self.PENDING_SET, hubdb_row_id)
        elif status == 'processing':
            self.client.sadd(self.PROCESSING_SET, hubdb_row_id)
        elif status == 'Success':
            self.client.sadd(self.COMPLETED_SET, hubdb_row_id)
        elif status == 'Failed':
            self.client.sadd(self.FAILED_SET, hubdb_row_id)
    
    def get_pending_rows(self, limit: int = 5) -> List[CustomerSyncRow]:
        """Get pending rows for processing"""
        rows = []
        try:
            pending_ids = list(self.client.smembers(self.PENDING_SET))[:limit]
            
            for hubdb_row_id in pending_ids:
                row = self.get_row(hubdb_row_id)
                if row:
                    rows.append(row)
        except Exception as e:
            logger.error(f"Failed to get pending rows: {e}")
        return rows
    
    def mark_rows_processing(self, hubdb_row_ids: List[str]) -> bool:
        """Mark rows as currently processing"""
        timestamp = datetime.utcnow().isoformat()
        try:
            for row_id in hubdb_row_ids:
                row = self.get_row(row_id)
                if row:
                    row.status = 'processing'
                    row.synced_date = timestamp
                    self.upsert_row(row)
            return True
        except Exception as e:
            logger.error(f"Failed to mark rows as processing: {e}")
            return False
    
    def update_row_result(self, hubdb_row_id: str, success: bool, 
                          error: str = "", netsuite_id: str = "") -> bool:
        """Update row with sync result"""
        try:
            row = self.get_row(hubdb_row_id)
            if not row:
                return False
            
            timestamp = datetime.utcnow().isoformat()
            row.status = 'Success' if success else 'Failed'
            row.sync_completed = timestamp
            row.notes = 'Synced successfully' if success else (error or 'Unknown error')
            
            if netsuite_id:
                row.netsuite_id = netsuite_id
            
            return self.upsert_row(row)
        except Exception as e:
            logger.error(f"Failed to update row result: {e}")
            return False
    
    def import_hubdb_rows(self, hubdb_rows: List[Dict[str, Any]], 
                          preserve_existing: bool = True) -> Dict[str, int]:
        """Import rows from HubDB, preserving existing sync state if requested."""
        stats = {'added': 0, 'updated': 0, 'preserved': 0}
        
        for hubdb_row in hubdb_rows:
            row_id = str(hubdb_row.get('id', ''))
            values = hubdb_row.get('values', {})
            
            existing = self.get_row(row_id) if preserve_existing else None
            
            if existing and existing.status in ['Success', 'processing']:
                stats['preserved'] += 1
                continue
            
            hs_id = (values.get('hs_id') or 
                    values.get('hubspot_id') or 
                    values.get('hubspot_record_id') or '')
            
            row = CustomerSyncRow(
                hubdb_row_id=row_id,
                hubspot_id=hs_id,
                netsuite_id=existing.netsuite_id if existing else values.get('netsuite_id', ''),
                customer_id=values.get('customer_id', ''),
                synced_date=existing.synced_date if existing else '',
                status=existing.status if existing else 'pending',
                sync_completed=existing.sync_completed if existing else '',
                notes=existing.notes if existing else ''
            )
            
            if not existing or existing.status not in ['Success']:
                row.status = 'pending'
            
            if self.upsert_row(row):
                if existing:
                    stats['updated'] += 1
                else:
                    stats['added'] += 1
        
        self.client.set(self.LAST_SYNC_KEY, datetime.utcnow().isoformat())
        
        return stats
    
    def get_stats(self) -> Dict[str, Any]:
        """Get sync statistics"""
        try:
            total = len(list(self.client.scan_iter(f"{self.ROWS_KEY_PREFIX}*")))
            pending = self.client.scard(self.PENDING_SET)
            processing = self.client.scard(self.PROCESSING_SET)
            completed = self.client.scard(self.COMPLETED_SET)
            failed = self.client.scard(self.FAILED_SET)
            last_sync = self.client.get(self.LAST_SYNC_KEY) or "Never"
            
            return {
                'total': total,
                'pending': pending,
                'processing': processing,
                'completed': completed,
                'failed': failed,
                'last_hubdb_sync': last_sync,
                'completion_rate': f"{(completed / total * 100):.1f}%" if total > 0 else "0%"
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {'error': str(e)}
    
    def reset_all(self) -> bool:
        """Reset all sync data (DANGEROUS)"""
        try:
            for key in self.client.scan_iter(f"{self.ROWS_KEY_PREFIX}*"):
                self.client.delete(key)
            
            self.client.delete(self.PENDING_SET)
            self.client.delete(self.PROCESSING_SET)
            self.client.delete(self.COMPLETED_SET)
            self.client.delete(self.FAILED_SET)
            self.client.delete(self.STATS_KEY)
            self.client.delete(self.LAST_SYNC_KEY)
            
            logger.warning("⚠️ All customer sync data has been reset")
            return True
        except Exception as e:
            logger.error(f"Failed to reset: {e}")
            return False
    
    def reset_failed_rows(self) -> int:
        """Reset failed rows back to pending status"""
        count = 0
        try:
            failed_ids = list(self.client.smembers(self.FAILED_SET))
            
            for row_id in failed_ids:
                row = self.get_row(row_id)
                if row:
                    row.status = 'pending'
                    row.synced_date = ''
                    row.sync_completed = ''
                    row.notes = ''
                    self.upsert_row(row)
                    count += 1
            
            logger.info(f"♻️ Reset {count} failed rows to pending")
        except Exception as e:
            logger.error(f"Failed to reset failed rows: {e}")
        return count
    
    def test_connection(self) -> Dict[str, Any]:
        """Test Redis connection"""
        try:
            self.client.ping()
            info = self.client.info('server')
            return {
                'success': True,
                'redis_version': info.get('redis_version', 'unknown'),
                'connected_clients': self.client.info('clients').get('connected_clients', 0)
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}


# Singleton instance
_redis_client: Optional[RedisClient] = None


def get_redis_client() -> RedisClient:
    """Get or create Redis client singleton"""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
    return _redis_client
