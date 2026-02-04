# Waites Customer Sync Service

Syncs customer data from HubDB to NetSuite. This is a standalone service separated from the Deal-to-Quote sync.

## Overview

This service:
1. Fetches customer data from HubDB (scheduled via cron)
2. Stores sync state in Redis
3. Creates/updates customers in NetSuite
4. Updates HubSpot facilities/companies with NetSuite IDs

## Architecture

```
HubDB Table → Redis Queue → NetSuite Customer Records
                  ↓
         HubSpot Facility/Company
         (NetSuite ID updates)
```

## API Endpoints

### Health & Status
- `GET /` - Service health check
- `GET /health` - Health check for Railway

### Customer Sync Operations
- `GET /api/customer-sync/status` - Get sync statistics
- `POST /api/customer-sync/refresh` - Refresh HubDB data to Redis
- `POST /api/customer-sync/process` - Process next batch of pending customers
- `POST /api/customer-sync/process-all` - Process all pending customers
- `POST /api/customer-sync/reset?failed_only=true` - Reset failed rows
- `POST /api/customer-sync/reset?all=true` - Reset all sync data

### Reconciliation
- `POST /api/customer-sync/reconcile` - Backfill missing NetSuite IDs
- `GET /api/customer-sync/reconcile/status` - Check reconciliation status

### Cron Management
- `POST /api/customer-sync/cron` - Manually trigger sync
- `POST /api/customer-sync/cron/enable` - Enable scheduled sync
- `POST /api/customer-sync/cron/disable` - Disable scheduled sync
- `GET /api/customer-sync/cron/status` - Get cron job status

## Environment Variables

See `env.example` for all required environment variables.

### Required:
- `HUBSPOT_PAT` - HubSpot Personal Access Token
- `NETSUITE_ACCOUNT_ID` - NetSuite account ID
- `NETSUITE_CONSUMER_KEY` - NetSuite TBA consumer key
- `NETSUITE_CONSUMER_SECRET` - NetSuite TBA consumer secret
- `NETSUITE_TOKEN_ID` - NetSuite TBA token ID
- `NETSUITE_TOKEN_SECRET` - NetSuite TBA token secret
- `REDIS_URL` - Redis connection URL
- `HUBDB_TABLE_ID` - HubDB table ID for customer data

## Railway Deployment

1. Create a new service in Railway
2. Connect to this GitHub repository
3. Add Redis service (or connect to existing)
4. Set environment variables
5. Deploy

## Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp env.example .env
# Edit .env with your credentials

# Run the service
python app.py
```

## Related Services

- **waites-integration-deals** - Deal-to-Quote sync (HubSpot deals → NetSuite quotes)
