#!/usr/bin/env python3
"""
Webhook Worker Service - turbo-station-ocpp-webhook

Background service that processes webhook queue and delivers events to external API.
Runs continuously, processing messages from Redis queues with retry logic.

Usage:
    python webhook_worker.py

Environment Variables:
    WEBHOOK_WORKER_ENABLED - Enable/disable worker (default: true)
    WEBHOOK_WORKER_CONCURRENCY - Number of concurrent workers (default: 5)
    API_URL - External API URL
    API_KEY - API authentication key
    REDIS_HOST - Redis host (default: localhost)
    REDIS_PORT - Redis port (default: 6379)
"""

import asyncio
import signal
import sys
import logging
from logging.handlers import RotatingFileHandler
import os
import json
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from pathlib import Path
from managers.webhook_queue_manager import get_webhook_queue_manager
from managers.webhook_manager import get_webhook_manager

# Load environment variables from .env file (explicit path)
env_path = Path(__file__).parent / '.env'
if os.getenv('ENVIRONMENT') == 'development':
    dev_env = Path(__file__).parent / '.env.dev'
    if dev_env.exists():
        env_path = dev_env
load_dotenv(dotenv_path=env_path)

# Setup logger
def setup_webhook_logger():
    """Setup webhook worker logger"""
    logger = logging.getLogger('webhook_worker')
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        env = os.getenv("ENVIRONMENT", "production")
        log_dir = os.path.join("logs", env)
        os.makedirs(log_dir, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, 'webhook_worker.log'),
            maxBytes=50 * 1024 * 1024,  # 50 MB
            backupCount=7
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

logger = setup_webhook_logger()


class WebhookWorker:
    """
    Worker that processes webhook queue and delivers to external API.
    """
    
    def __init__(self, worker_id: int = 1):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique worker identifier
        """
        self.worker_id = worker_id
        self.queue_manager = get_webhook_queue_manager()
        self.webhook_manager = get_webhook_manager()
        self.running = False
        self.processed_count = 0
        self.failed_count = 0
        
        logger.info(f"Worker-{self.worker_id}: Initialized")
    
    async def process_message(self, message: dict) -> bool:
        """
        Process a single webhook message.
        
        Args:
            message: Message from queue
        
        Returns:
            True if processed successfully
        """
        endpoint = message.get('endpoint')
        payload = message.get('payload', {})
        attempts = message.get('attempts', 0)
        max_attempts = message.get('max_attempts', 5)
        
        logger.info(
            f"Worker-{self.worker_id}: Processing {endpoint} "
            f"(attempt={attempts+1}/{max_attempts})"
        )
        
        # Increment attempts
        message['attempts'] = attempts + 1
        message['last_attempt_at'] = datetime.now().isoformat()
        
        # Send webhook
        result = await self.webhook_manager.send_webhook(
            endpoint, 
            payload, 
            priority=message.get('priority', 'normal')
        )
        
        if result['success']:
            # Success! Mark as processed
            await self.queue_manager.mark_processed(message)
            self.processed_count += 1
            
            logger.info(
                f"Worker-{self.worker_id}: ✅ Successfully processed {endpoint} "
                f"(elapsed={result.get('elapsed', 0):.3f}s)"
            )
            return True
        
        else:
            # Failed - check if should retry
            if message['attempts'] >= max_attempts:
                # Max retries exceeded, move to DLQ
                error = result.get('error', 'unknown')
                await self.queue_manager.move_to_dlq(message, error)
                self.failed_count += 1
                
                logger.error(
                    f"Worker-{self.worker_id}: ❌ FAILED after {message['attempts']} attempts: {endpoint} "
                    f"(error={error})"
                )
                return False
            
            else:
                # Re-queue for retry with exponential backoff
                backoff_seconds = 2 ** attempts  # 1s, 2s, 4s, 8s, 16s...
                
                logger.warning(
                    f"Worker-{self.worker_id}: ⚠️ Retry needed for {endpoint} "
                    f"(attempt={message['attempts']}/{max_attempts}, "
                    f"backoff={backoff_seconds}s, "
                    f"error={result.get('error')})"
                )
                
                # Wait before re-queuing
                await asyncio.sleep(backoff_seconds)
                
                # Re-enqueue with preserved attempt count
                redis = await self.queue_manager._get_redis()
                queue_name = self.queue_manager._get_queue_name(message.get('priority', 'normal'))
                message_json = json.dumps(message)
                await redis.lpush(queue_name, message_json)
                
                return False
    
    async def run(self):
        """
        Main worker loop - processes messages from queue.
        """
        self.running = True
        logger.info(f"Worker-{self.worker_id}: Starting main loop")
        
        # Process high priority first, then normal, then low
        priorities = ['high', 'normal', 'low']
        current_priority_index = 0
        
        while self.running:
            try:
                # Round-robin through priorities (with bias toward high priority)
                priority = priorities[current_priority_index]
                
                # Dequeue message (blocks for 1 second if queue empty)
                message = await self.queue_manager.dequeue(priority, timeout=1)
                
                if message:
                    # Process message
                    await self.process_message(message)
                    
                    # Reset to high priority after processing
                    current_priority_index = 0
                else:
                    # No message in this queue, try next priority
                    current_priority_index = (current_priority_index + 1) % len(priorities)
                
            except asyncio.CancelledError:
                logger.info(f"Worker-{self.worker_id}: Cancelled, shutting down")
                break
            
            except Exception as e:
                logger.exception(f"Worker-{self.worker_id}: Unexpected error: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on errors
        
        logger.info(
            f"Worker-{self.worker_id}: Stopped "
            f"(processed={self.processed_count}, failed={self.failed_count})"
        )
    
    async def stop(self):
        """Stop the worker gracefully."""
        logger.info(f"Worker-{self.worker_id}: Stopping...")
        self.running = False


class WebhookWorkerService:
    """
    Service that manages multiple webhook workers.
    """
    
    def __init__(self, concurrency: int = 5):
        """
        Initialize service.
        
        Args:
            concurrency: Number of concurrent workers
        """
        self.concurrency = concurrency
        self.workers = []
        self.tasks = []
        self.running = False
        
        logger.info(f"WebhookWorkerService: Initialized with {concurrency} workers")
    
    async def start(self):
        """Start all workers."""
        self.running = True
        
        logger.info(f"WebhookWorkerService: Starting {self.concurrency} workers...")
        
        # Create workers
        for i in range(self.concurrency):
            worker = WebhookWorker(worker_id=i+1)
            self.workers.append(worker)
            
            # Start worker task
            task = asyncio.create_task(worker.run())
            self.tasks.append(task)
        
        logger.info(f"WebhookWorkerService: All workers started")
        
        # Wait for all tasks
        await asyncio.gather(*self.tasks, return_exceptions=True)
    
    async def stop(self):
        """Stop all workers gracefully."""
        logger.info("WebhookWorkerService: Stopping all workers...")
        self.running = False
        
        # Stop all workers
        for worker in self.workers:
            await worker.stop()
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        logger.info("WebhookWorkerService: All workers stopped")
    
    async def print_stats(self):
        """Print periodic stats."""
        queue_manager = get_webhook_queue_manager()
        
        while self.running:
            await asyncio.sleep(60)  # Every minute
            
            try:
                stats = await queue_manager.get_stats()
                depths = stats.get('queue_depths', {})
                
                logger.info(
                    f"📊 STATS: "
                    f"High={depths.get('webhooks:high', 0)} "
                    f"Normal={depths.get('webhooks:normal', 0)} "
                    f"Low={depths.get('webhooks:low', 0)} "
                    f"DLQ={depths.get('webhooks:dlq', 0)} | "
                    f"Enqueued={stats.get('total_enqueued', 0)} "
                    f"Processed={stats.get('total_processed', 0)} "
                    f"Failed={stats.get('total_failed', 0)}"
                )
                
                # Alert if DLQ is growing
                dlq_depth = depths.get('webhooks:dlq', 0)
                if dlq_depth > 10:
                    logger.warning(f"⚠️ DLQ has {dlq_depth} messages! Manual intervention may be needed.")
                
            except Exception as e:
                logger.exception(f"Error printing stats: {e}")


async def main():
    """Main entry point."""
    import os
    
    # Check if worker is enabled
    enabled = os.getenv('WEBHOOK_WORKER_ENABLED', 'true').lower() in ['true', '1', 'yes']
    if not enabled:
        logger.info("Webhook worker is disabled (WEBHOOK_WORKER_ENABLED=false)")
        return
    
    # Get concurrency
    concurrency = int(os.getenv('WEBHOOK_WORKER_CONCURRENCY', '5'))
    
    # Create service
    service = WebhookWorkerService(concurrency=concurrency)
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    
    def signal_handler(sig):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(service.stop())
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
    
    # Start stats printer
    stats_task = asyncio.create_task(service.print_stats())
    
    try:
        logger.info("🚀 Webhook Worker Service starting...")
        await service.start()
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    
    finally:
        stats_task.cancel()
        await service.stop()
        logger.info("👋 Webhook Worker Service stopped")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting...")
        sys.exit(0)
