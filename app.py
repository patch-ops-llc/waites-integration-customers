# app.py
# Waites Customer Sync Service
# Syncs customers from HubDB to NetSuite (separate from Deal-to-Quote sync)

import os
import threading
from datetime import datetime
from flask import Flask, jsonify, request

from customer_sync import CustomerSync, start_background_sync, stop_background_sync
from logger import logger

# APScheduler for in-app cron
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'customer-sync-secret-key')

# =============================================================================
# APScheduler Setup for Customer Sync Cron
# =============================================================================
scheduler = BackgroundScheduler()
cron_enabled = True  # Can be toggled via API
last_cron_result = None


def _run_customer_sync_cron():
    """Background worker for customer sync cron
    
    This cron job:
    1. Refreshes Redis from HubDB (adds new rows, updates pending/failed rows)
    2. Processes all pending rows to NetSuite
    """
    try:
        logger.info("🕐 Cron worker started: Customer Sync")
        
        sync = CustomerSync()
        
        # Step 1: Refresh Redis from HubDB
        logger.info("📥 Step 1: Refreshing Redis from HubDB...")
        refresh_result = sync.sync_hubdb_to_redis(preserve_existing=True)
        
        if not refresh_result.get('success'):
            logger.error(f"❌ HubDB refresh failed: {refresh_result.get('error', 'Unknown error')}")
            return
        
        stats = refresh_result.get('stats', {})
        logger.info(f"   Added: {stats.get('added', 0)}, Updated: {stats.get('updated', 0)}, Preserved: {stats.get('preserved', 0)}")
        
        # Step 2: Process pending rows to NetSuite
        logger.info("🔄 Step 2: Processing pending rows to NetSuite...")
        
        from redis_client import get_redis_client
        redis = get_redis_client()
        redis_stats = redis.get_stats()
        pending_count = redis_stats.get('pending', 0)
        
        if pending_count == 0:
            logger.info("✅ No pending rows to sync - all customers are up to date")
            return
        
        logger.info(f"   Found {pending_count} pending rows to process")
        
        # Process all pending rows in batches
        total_success = 0
        total_failed = 0
        batches_run = 0
        max_batches = 50  # Safety limit
        
        while batches_run < max_batches:
            batch_result = sync.process_next_batch()
            
            if not batch_result.get('success'):
                if 'already in progress' in batch_result.get('message', ''):
                    continue
                break
            
            rows_processed = batch_result.get('rows_processed', 0)
            if rows_processed == 0:
                break
            
            total_success += batch_result.get('success_count', 0)
            total_failed += batch_result.get('fail_count', 0)
            batches_run += 1
        
        logger.info(f"✅ Cron complete: Processed {total_success + total_failed} customers in {batches_run} batches")
        logger.info(f"   Success: {total_success}, Failed: {total_failed}")
        
    except Exception as e:
        logger.error(f"Cron worker failed: {e}")
        import traceback
        logger.error(traceback.format_exc())


def scheduled_customer_sync():
    """Scheduled task to sync new customers from HubDB to NetSuite"""
    global cron_enabled, last_cron_result
    
    if not cron_enabled:
        logger.info("⏸️ Scheduled customer sync skipped - cron is disabled")
        return
    
    logger.info("⏰ Scheduled customer sync starting...")
    
    try:
        _run_customer_sync_cron()
        last_cron_result = {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Completed successfully"
        }
        # Also store in Redis for persistence
        try:
            from redis_client import get_redis_client
            import json
            redis = get_redis_client()
            redis.client.set('customer_sync:last_cron_result', json.dumps(last_cron_result))
        except:
            pass
    except Exception as e:
        logger.error(f"Scheduled customer sync failed: {e}")
        last_cron_result = {
            "success": False,
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }
        try:
            from redis_client import get_redis_client
            import json
            redis = get_redis_client()
            redis.client.set('customer_sync:last_cron_result', json.dumps(last_cron_result))
        except:
            pass


# =============================================================================
# Health & Status Endpoints
# =============================================================================

@app.route('/', methods=['GET'])
def home():
    """Health check endpoint"""
    return jsonify({
        "service": "Waites Customer Sync",
        "status": "healthy",
        "version": "1.0.0",
        "cron_enabled": cron_enabled,
        "timestamp": datetime.utcnow().isoformat()
    })


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint for Railway"""
    try:
        from redis_client import get_redis_client
        redis = get_redis_client()
        redis_status = "connected" if redis.test_connection().get('success') else "error"
    except Exception as e:
        redis_status = f"error: {str(e)}"
    
    return jsonify({
        "status": "healthy",
        "redis_status": redis_status,
        "cron_enabled": cron_enabled,
        "timestamp": datetime.utcnow().isoformat()
    })


# =============================================================================
# Customer Sync Endpoints
# =============================================================================

@app.route('/api/customer-sync/status', methods=['GET'])
def customer_sync_status():
    """Get customer sync status and statistics"""
    try:
        sync = CustomerSync()
        result = sync.get_status()
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/customer-sync/refresh', methods=['POST'])
def customer_sync_refresh():
    """
    Refresh customer data from HubDB to Redis.
    This fetches the latest data from HubDB and stores it in Redis.
    Existing sync state is preserved by default.
    """
    try:
        preserve = request.args.get('preserve', 'true').lower() == 'true'
        
        sync = CustomerSync()
        result = sync.sync_hubdb_to_redis(preserve_existing=preserve)
        
        status_code = 200 if result.get('success') else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Customer sync refresh failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/customer-sync/process', methods=['POST'])
def customer_sync_process():
    """
    Process the next batch of pending customers.
    This syncs customers from Redis to NetSuite.
    """
    try:
        sync = CustomerSync()
        result = sync.process_next_batch()
        
        status_code = 200 if result.get('success') else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Customer sync process failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/customer-sync/process-all', methods=['POST'])
def customer_sync_process_all():
    """
    Process ALL pending customers in batches.
    Use with caution - may take a while for large datasets.
    """
    try:
        max_batches = int(request.args.get('max_batches', 100))
        
        sync = CustomerSync()
        result = sync.process_all(max_batches=max_batches)
        
        status_code = 200 if result.get('success') else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Customer sync process-all failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/customer-sync/reset', methods=['POST'])
def customer_sync_reset():
    """
    Reset customer sync data.
    
    Query params:
    - failed_only=true: Only reset failed rows back to pending
    - all=true: Reset everything (DANGEROUS)
    """
    try:
        sync = CustomerSync()
        
        if request.args.get('failed_only', '').lower() == 'true':
            result = sync.reset_failed()
        elif request.args.get('all', '').lower() == 'true':
            result = sync.reset_all()
        else:
            return jsonify({
                "success": False,
                "error": "Specify ?failed_only=true or ?all=true"
            }), 400
        
        status_code = 200 if result.get('success') else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Customer sync reset failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# Store reconciliation results for async retrieval
_reconciliation_result = None
_reconciliation_running = False


def _run_reconciliation_async(dry_run: bool):
    """Background worker for reconciliation"""
    global _reconciliation_result, _reconciliation_running
    
    try:
        _reconciliation_running = True
        logger.info(f"🔄 Starting async reconciliation (dry_run={dry_run})...")
        
        sync = CustomerSync()
        result = sync.reconcile_hubdb_netsuite_ids(dry_run=dry_run)
        
        _reconciliation_result = {
            "completed_at": datetime.utcnow().isoformat(),
            **result
        }
        
        # Store in Redis for persistence
        try:
            from redis_client import get_redis_client
            import json
            redis = get_redis_client()
            redis.client.set('customer_sync:reconciliation_result', json.dumps(_reconciliation_result))
        except:
            pass
        
        logger.success(f"✅ Reconciliation complete: {result.get('gaps_found', 0)} gaps found, {result.get('gaps_fixed', 0)} fixed")
        
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        _reconciliation_result = {
            "success": False,
            "error": str(e),
            "completed_at": datetime.utcnow().isoformat()
        }
    finally:
        _reconciliation_running = False


@app.route('/api/customer-sync/reconcile', methods=['POST', 'GET'])
def customer_sync_reconcile():
    """
    Reconcile HubDB table with HubSpot records to backfill missing NetSuite IDs.
    """
    global _reconciliation_running
    
    try:
        if _reconciliation_running:
            return jsonify({
                "success": True,
                "status": "running",
                "message": "Reconciliation is already in progress."
            }), 202
        
        dry_run = request.args.get('dry_run', 'true').lower() != 'false'
        
        thread = threading.Thread(target=_run_reconciliation_async, args=(dry_run,), daemon=True)
        thread.start()
        
        return jsonify({
            "success": True,
            "status": "started",
            "dry_run": dry_run,
            "message": f"Reconciliation started (dry_run={dry_run}).",
            "started_at": datetime.utcnow().isoformat()
        }), 202
        
    except Exception as e:
        logger.error(f"Customer sync reconciliation failed to start: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/customer-sync/reconcile/status', methods=['GET'])
def customer_sync_reconcile_status():
    """Check the status/results of the last reconciliation run"""
    global _reconciliation_result, _reconciliation_running
    
    result = _reconciliation_result
    if result is None:
        try:
            from redis_client import get_redis_client
            import json
            redis = get_redis_client()
            stored = redis.client.get('customer_sync:reconciliation_result')
            if stored:
                result = json.loads(stored)
        except:
            pass
    
    if _reconciliation_running:
        return jsonify({
            "status": "running",
            "message": "Reconciliation is currently in progress..."
        })
    elif result:
        return jsonify({
            "status": "completed",
            **result
        })
    else:
        return jsonify({
            "status": "not_run",
            "message": "No reconciliation has been run yet."
        })


@app.route('/api/customer-sync/mark-all-synced', methods=['POST'])
def customer_sync_mark_all_synced():
    """
    Mark all pending rows as already synced (Success).
    """
    try:
        from redis_client import get_redis_client
        
        redis = get_redis_client()
        
        pending_ids = list(redis.client.smembers(redis.PENDING_SET))
        count = len(pending_ids)
        
        if count == 0:
            return jsonify({
                "success": True,
                "message": "No pending rows to mark",
                "count": 0
            })
        
        timestamp = datetime.utcnow().isoformat()
        
        for row_id in pending_ids:
            row = redis.get_row(row_id)
            if row:
                row.status = 'Success'
                row.sync_completed = timestamp
                row.notes = 'Marked as synced (migrated from old system)'
                redis.upsert_row(row)
        
        logger.info(f"✅ Marked {count} pending rows as synced")
        
        return jsonify({
            "success": True,
            "message": f"Marked {count} rows as already synced",
            "count": count
        })
        
    except Exception as e:
        logger.error(f"Mark all synced failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# =============================================================================
# Cron Management Endpoints
# =============================================================================

@app.route('/api/customer-sync/cron', methods=['POST', 'GET'])
def customer_sync_cron():
    """Manually trigger customer sync cron (runs async)."""
    try:
        logger.info("🕐 Manual cron trigger: Customer Sync")
        
        thread = threading.Thread(target=_run_customer_sync_cron, daemon=True)
        thread.start()
        
        return jsonify({
            "success": True,
            "message": "Cron job started in background",
            "status": "processing",
            "timestamp": datetime.utcnow().isoformat()
        }), 202
        
    except Exception as e:
        logger.error(f"Cron job failed to start: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/customer-sync/cron/enable', methods=['POST'])
def customer_sync_cron_enable():
    """Enable the scheduled customer sync cron job"""
    global cron_enabled
    cron_enabled = True
    logger.info("✅ Customer sync cron ENABLED")
    return jsonify({
        "success": True,
        "cron_enabled": True,
        "message": "Cron job enabled"
    })


@app.route('/api/customer-sync/cron/disable', methods=['POST'])
def customer_sync_cron_disable():
    """Disable the scheduled customer sync cron job"""
    global cron_enabled
    cron_enabled = False
    logger.info("⏸️ Customer sync cron DISABLED")
    return jsonify({
        "success": True,
        "cron_enabled": False,
        "message": "Cron job disabled"
    })


@app.route('/api/customer-sync/cron/toggle', methods=['POST'])
def customer_sync_cron_toggle():
    """Toggle the scheduled customer sync cron job on/off"""
    global cron_enabled
    cron_enabled = not cron_enabled
    status = "ENABLED" if cron_enabled else "DISABLED"
    logger.info(f"🔄 Customer sync cron {status}")
    return jsonify({
        "success": True,
        "cron_enabled": cron_enabled,
        "message": f"Cron job {status.lower()}"
    })


@app.route('/api/customer-sync/cron/status', methods=['GET'])
def customer_sync_cron_status():
    """Get cron job status and last run result"""
    from config import config
    
    run_result = last_cron_result
    if run_result is None:
        try:
            from redis_client import get_redis_client
            import json
            redis = get_redis_client()
            stored = redis.client.get('customer_sync:last_cron_result')
            if stored:
                run_result = json.loads(stored)
        except:
            pass
    
    job = scheduler.get_job('customer_sync_cron')
    
    return jsonify({
        "success": True,
        "cron_enabled": cron_enabled,
        "interval_minutes": config.CRON_INTERVAL_MINUTES,
        "scheduler_running": scheduler.running,
        "last_run": run_result,
        "next_run": str(job.next_run_time) if job else None
    })


# =============================================================================
# Redis Test Endpoint
# =============================================================================

@app.route('/api/test-redis', methods=['GET'])
def test_redis():
    """Test Redis connection"""
    try:
        from redis_client import get_redis_client
        
        client = get_redis_client()
        result = client.test_connection()
        
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# =============================================================================
# Scheduler Initialization
# =============================================================================

def start_scheduler():
    """Start the APScheduler for customer sync cron"""
    global cron_enabled
    
    from config import config
    
    # Set initial state based on config
    cron_enabled = config.CRON_ENABLED_ON_START
    
    # Add the customer sync job
    scheduler.add_job(
        scheduled_customer_sync,
        trigger=IntervalTrigger(minutes=config.CRON_INTERVAL_MINUTES),
        id='customer_sync_cron',
        name=f'Customer sync every {config.CRON_INTERVAL_MINUTES} minutes',
        replace_existing=True
    )
    
    # Start the scheduler
    if not scheduler.running:
        scheduler.start()
        logger.info(f"📅 Scheduler started: Customer sync every {config.CRON_INTERVAL_MINUTES} minutes")
        logger.info(f"   Cron enabled: {cron_enabled}")


# Start scheduler for both development and production (gunicorn)
try:
    start_scheduler()
except Exception as e:
    logger.error(f"Failed to start scheduler: {e}")


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    debug = os.getenv('ENVIRONMENT', 'production') != 'production'
    
    logger.info(f"🚀 Starting Waites Customer Sync on port {port}")
    logger.info(f"   Environment: {os.getenv('ENVIRONMENT', 'production')}")
    logger.info(f"   Debug mode: {debug}")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
