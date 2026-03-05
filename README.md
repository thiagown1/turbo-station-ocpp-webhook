# turbo-station-ocpp-webhook

Webhook Worker Service for Turbo Station OCPP infrastructure.

Background service that processes webhook queues (via Redis) and delivers events to the external Turbo Station API with retry logic, priority queues, and dead-letter queue (DLQ) support.

## Architecture

```
Redis Queues (webhooks:high, webhooks:normal, webhooks:low)
        │
        ▼
┌─────────────────────┐
│  WebhookWorkerService│  (N concurrent workers)
│  ┌─────────────────┐ │
│  │ WebhookWorker 1 │ │──► HTTP POST to API
│  │ WebhookWorker 2 │ │──► HTTP POST to API
│  │ WebhookWorker N │ │──► HTTP POST to API
│  └─────────────────┘ │
└─────────────────────┘
        │ (failures)
        ▼
   webhooks:dlq (Dead Letter Queue)
```

## Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
# Edit .env with your configuration
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WEBHOOK_WORKER_ENABLED` | `true` | Enable/disable worker |
| `WEBHOOK_WORKER_CONCURRENCY` | `5` | Number of concurrent workers |
| `API_ENDPOINT` | `http://localhost:3000` | External API URL |
| `API_KEY` | `` | API authentication key |
| `WEBHOOK_ENABLED` | `true` | Enable webhook delivery |
| `WEBHOOK_QUEUE_ENABLED` | `true` | Enable Redis queue |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database |
| `ENVIRONMENT` | `production` | Environment (production/development) |

## Running

### Development
```bash
python webhook_worker.py
```

### Production (PM2)
```bash
pm2 start ecosystem.config.js --only ocpp-webhook
```

## Deployment

```bash
./scripts/deploy_prod.sh          # Deploy current version
./scripts/deploy_prod.sh --force  # Force redeploy
./scripts/rollback_prod.sh        # Rollback to previous
```
