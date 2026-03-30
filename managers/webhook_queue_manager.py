import json
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from redis import asyncio as aioredis
import os
from log_config import root_logger


class WebhookQueueManager:
    """
    Redis-based queue manager for webhook delivery.
    Provides priority queues and dead-letter queue (DLQ) support.
    """
    
    # Base queue names (will be prefixed with environment)
    QUEUE_HIGH = "webhooks:high"
    QUEUE_NORMAL = "webhooks:normal"
    QUEUE_LOW = "webhooks:low"
    QUEUE_DLQ = "webhooks:dlq"
    
    # Base stats keys (will be prefixed with environment)
    STATS_ENQUEUED = "webhooks:stats:enqueued"
    STATS_PROCESSED = "webhooks:stats:processed"
    STATS_FAILED = "webhooks:stats:failed"
    
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        """
        Initialize queue manager.
        
        Args:
            redis_client: Optional Redis client. If not provided, creates new connection.
        """
        self.redis = redis_client
        self._should_close_redis = redis_client is None
        self.enabled = os.getenv('WEBHOOK_QUEUE_ENABLED', 'true').lower() in ['true', '1', 'yes']
        
        # Determine queue prefix based on environment
        # Production uses no prefix (backward compatible), dev uses "dev:" prefix
        environment = os.getenv('ENVIRONMENT', 'production').lower()
        if environment == 'development':
            self.queue_prefix = "dev:"
            root_logger.info(f"WebhookQueueManager: Using DEV queue prefix (dev:*) - separate from production")
        else:
            self.queue_prefix = ""
            root_logger.info(f"WebhookQueueManager: Using PRODUCTION queues (no prefix)")
        
        root_logger.info(f"WebhookQueueManager initialized: queue_enabled={self.enabled}, prefix={self.queue_prefix or 'none'}")
    
    async def _get_redis(self) -> aioredis.Redis:
        """Get or create Redis connection"""
        if self.redis is None:
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = int(os.getenv('REDIS_PORT', '6379'))
            redis_db = int(os.getenv('REDIS_DB', '0'))
            
            self.redis = await aioredis.from_url(
                f"redis://{redis_host}:{redis_port}/{redis_db}",
                encoding="utf-8",
                decode_responses=True
            )
            root_logger.info(f"WebhookQueueManager: Created Redis connection to {redis_host}:{redis_port}/{redis_db}")
        
        return self.redis
    
    async def close(self):
        """Close Redis connection if we created it"""
        if self._should_close_redis and self.redis:
            await self.redis.close()
            root_logger.info("WebhookQueueManager: Closed Redis connection")
    
    def _get_queue_name(self, priority: str) -> str:
        """Get queue name for priority level with environment prefix"""
        priority_map = {
            'high': self.QUEUE_HIGH,
            'normal': self.QUEUE_NORMAL,
            'low': self.QUEUE_LOW,
        }
        base_name = priority_map.get(priority, self.QUEUE_NORMAL)
        return f"{self.queue_prefix}{base_name}" if self.queue_prefix else base_name
    
    def _get_dlq_name(self) -> str:
        """Get dead-letter queue name with environment prefix"""
        return f"{self.queue_prefix}{self.QUEUE_DLQ}" if self.queue_prefix else self.QUEUE_DLQ
    
    def _get_stats_key(self, stats_type: str) -> str:
        """Get stats key name with environment prefix"""
        stats_map = {
            'enqueued': self.STATS_ENQUEUED,
            'processed': self.STATS_PROCESSED,
            'failed': self.STATS_FAILED,
        }
        base_name = stats_map.get(stats_type, self.STATS_ENQUEUED)
        return f"{self.queue_prefix}{base_name}" if self.queue_prefix else base_name
    
    async def enqueue(self, endpoint: str, payload: Dict[str, Any], 
                     priority: str = 'normal', metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Add webhook to queue.
        
        Args:
            endpoint: API endpoint (e.g., '/webhook/stop-transaction')
            payload: Data to send
            priority: 'high', 'normal', or 'low'
            metadata: Optional metadata (transaction_id, user_id, etc.)
        
        Returns:
            True if enqueued successfully
        """
        if not self.enabled:
            root_logger.debug(f"Queue disabled, skipping enqueue: {endpoint}")
            return False
        
        try:
            redis = await self._get_redis()
            
            # Create message
            message = {
                'endpoint': endpoint,
                'payload': payload,
                'priority': priority,
                'metadata': metadata or {},
                'attempts': 0,
                'max_attempts': int(os.getenv('WEBHOOK_MAX_RETRIES', '5')),
                'created_at': datetime.now().isoformat(),
                'enqueued_at': datetime.now().isoformat(),
            }
            
            # Push to appropriate queue (LPUSH = left push, RPOP = right pop for FIFO)
            queue_name = self._get_queue_name(priority)
            message_json = json.dumps(message)
            
            await redis.lpush(queue_name, message_json)
            await redis.hincrby(self._get_stats_key('enqueued'), queue_name, 1)
            
            root_logger.info(
                f"📥 Enqueued webhook: {endpoint} "
                f"(queue={queue_name}, priority={priority}, "
                f"metadata={metadata})"
            )
            
            return True
            
        except Exception as e:
            root_logger.exception(f"❌ Failed to enqueue webhook {endpoint}: {e}")
            return False
    
    async def enqueue_stop_transaction(self, transaction_data: Dict[str, Any], 
                                       meter_stop: int, timestamp: str, reason: str = 'Local') -> bool:
        """
        Enqueue StopTransaction webhook.
        
        Args:
            transaction_data: Transaction data
            meter_stop: Final meter reading
            timestamp: Stop timestamp
            reason: Stop reason (Remote, Local, etc.)
        
        Returns:
            True if enqueued successfully
        """
        # Determine previous_energy using the fix logic
        meter_start = transaction_data.get('meter_start')
        
        if transaction_data.get('previous_energy'):
            previous_energy = transaction_data['previous_energy']
            source = 'cache'
        elif transaction_data.get('meter_value'):
            previous_energy = transaction_data['meter_value']
            source = 'firebase_meter_value'
        else:
            previous_energy = meter_start
            source = 'meter_start_full_session'
        
        # Build transaction_data payload, excluding None/undefined values
        clean_transaction_data = {}
        for key, value in transaction_data.items():
            if value is not None:
                clean_transaction_data[key] = value
        
        payload = {
            'transaction_id': transaction_data.get('transaction_id'),
            'charge_point_id': transaction_data.get('charge_point_id'),
            'connector_id': transaction_data.get('connector_id'),
            'user_id': transaction_data.get('user_id'),
            'meter_start': meter_start,
            'meter_stop': meter_stop,
            'timestamp': timestamp,  # Changed from timestamp_stop
            'reason': reason,  # Stop reason (Remote, Local, etc.)
            'kwh_price': transaction_data.get('kwh_price'),
            'previous_energy': previous_energy,
            'previous_energy_source': source,
            'transaction_data': clean_transaction_data  # Clean data without None values
        }
        
        metadata = {
            'transaction_id': transaction_data.get('transaction_id'),
            'user_id': transaction_data.get('user_id'),
            'charge_point_id': transaction_data.get('charge_point_id'),
            'event_type': 'stop_transaction'
        }
        
        return await self.enqueue('/api/webhook/stop-transaction', payload, priority='high', metadata=metadata)
    
    async def enqueue_status_notification(self, charge_point_id: str, connector_id: int,
                                          status: str, error_code: str, 
                                          timestamp: str, **kwargs) -> bool:
        """
        Enqueue StatusNotification webhook.
        
        Args:
            charge_point_id: Charger ID
            connector_id: Connector ID
            status: Connector status
            error_code: Error code
            timestamp: Notification timestamp
            **kwargs: Additional data
        
        Returns:
            True if enqueued successfully
        """
        payload = {
            'charge_point_id': charge_point_id,
            'connector_id': connector_id,
            'status': status,
            'error_code': error_code,
            'timestamp': timestamp,
            **kwargs
        }
        
        metadata = {
            'charge_point_id': charge_point_id,
            'event_type': 'status_notification'
        }
        
        return await self.enqueue('/api/webhook/status-notification', payload, priority='normal', metadata=metadata)
    
    async def enqueue_meter_values(self, transaction_id: int, charge_point_id: str,
                                   current_energy: float, timestamp: str,
                                   **kwargs) -> bool:
        """
        Enqueue MeterValues webhook.
        
        Args:
            transaction_id: Transaction ID
            charge_point_id: Charger ID
            current_energy: Current energy reading
            timestamp: Reading timestamp
            **kwargs: Additional data
        
        Returns:
            True if enqueued successfully
        """
        payload = {
            'transaction_id': transaction_id,
            'charge_point_id': charge_point_id,
            'current_energy': current_energy,
            'timestamp': timestamp,
            **kwargs
        }
        
        metadata = {
            'transaction_id': transaction_id,
            'charge_point_id': charge_point_id,
            'event_type': 'meter_values'
        }
        
        return await self.enqueue('/api/webhook/meter-values', payload, priority='normal', metadata=metadata)
    
    async def enqueue_heartbeat(self, charge_point_id: str,
                                timestamp: str) -> bool:
        """
        Enqueue Heartbeat webhook for idle fee billing.
        
        Args:
            charge_point_id: Charger ID
            timestamp: Heartbeat timestamp
        
        Returns:
            True if enqueued successfully
        """
        payload = {
            'charge_point_id': charge_point_id,
            'timestamp': timestamp,
        }
        
        metadata = {
            'charge_point_id': charge_point_id,
            'event_type': 'heartbeat'
        }
        
        return await self.enqueue('/api/webhook/heartbeat', payload, priority='low', metadata=metadata)
    
    async def dequeue(self, priority: str = 'high', timeout: int = 1) -> Optional[Dict[str, Any]]:
        """
        Dequeue a webhook message (used by worker).
        
        Args:
            priority: Queue priority to dequeue from
            timeout: Blocking timeout in seconds
        
        Returns:
            Message dict or None if queue is empty
        """
        try:
            redis = await self._get_redis()
            queue_name = self._get_queue_name(priority)
            
            # BRPOP blocks until item available or timeout
            result = await redis.brpop(queue_name, timeout=timeout)
            
            if result:
                _, message_json = result
                message = json.loads(message_json)
                return message
            
            return None
            
        except Exception as e:
            root_logger.exception(f"❌ Failed to dequeue from {priority}: {e}")
            return None
    
    async def move_to_dlq(self, message: Dict[str, Any], error: str) -> bool:
        """
        Move failed message to Dead Letter Queue.
        
        Args:
            message: Original message
            error: Error reason
        
        Returns:
            True if moved successfully
        """
        try:
            redis = await self._get_redis()
            
            # Add error info
            dlq_message = {
                **message,
                'failed_at': datetime.now().isoformat(),
                'error': error,
                'final_attempts': message.get('attempts', 0)
            }
            
            dlq_name = self._get_dlq_name()
            await redis.lpush(dlq_name, json.dumps(dlq_message))
            await redis.hincrby(self._get_stats_key('failed'), message.get('endpoint', 'unknown'), 1)
            
            root_logger.warning(
                f"💀 Moved to DLQ: {message.get('endpoint')} "
                f"(attempts={message.get('attempts')}, error={error})"
            )
            
            return True
            
        except Exception as e:
            root_logger.exception(f"❌ Failed to move to DLQ: {e}")
            return False
    
    async def get_queue_depth(self, priority: str = 'all') -> Dict[str, int]:
        """
        Get current queue depths.
        
        Args:
            priority: 'high', 'normal', 'low', 'dlq', or 'all'
        
        Returns:
            Dict with queue depths
        """
        try:
            redis = await self._get_redis()
            
            if priority == 'all':
                queues = [
                    self._get_queue_name('high'),
                    self._get_queue_name('normal'),
                    self._get_queue_name('low'),
                    self._get_dlq_name()
                ]
            else:
                queues = [self._get_queue_name(priority)]
            
            depths = {}
            for queue in queues:
                depth = await redis.llen(queue)
                depths[queue] = depth
            
            return depths
            
        except Exception as e:
            root_logger.exception(f"❌ Failed to get queue depth: {e}")
            return {}
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get webhook queue statistics.
        
        Returns:
            Dict with stats
        """
        try:
            redis = await self._get_redis()
            
            # Get queue depths
            depths = await self.get_queue_depth('all')
            
            # Get counters
            enqueued = await redis.hgetall(self._get_stats_key('enqueued')) or {}
            processed = await redis.hgetall(self._get_stats_key('processed')) or {}
            failed = await redis.hgetall(self._get_stats_key('failed')) or {}
            
            return {
                'queue_depths': depths,
                'total_enqueued': sum(int(v) for v in enqueued.values()),
                'total_processed': sum(int(v) for v in processed.values()),
                'total_failed': sum(int(v) for v in failed.values()),
                'enqueued_by_queue': enqueued,
                'processed_by_endpoint': processed,
                'failed_by_endpoint': failed,
            }
            
        except Exception as e:
            root_logger.exception(f"❌ Failed to get stats: {e}")
            return {}
    
    async def mark_processed(self, message: Dict[str, Any]) -> bool:
        """
        Mark message as successfully processed.
        
        Args:
            message: Message that was processed
        
        Returns:
            True if marked successfully
        """
        try:
            redis = await self._get_redis()
            endpoint = message.get('endpoint', 'unknown')
            await redis.hincrby(self._get_stats_key('processed'), endpoint, 1)
            return True
        except Exception as e:
            root_logger.exception(f"❌ Failed to mark processed: {e}")
            return False


# Global singleton
_webhook_queue_manager = None


def get_webhook_queue_manager(redis_client: Optional[aioredis.Redis] = None) -> WebhookQueueManager:
    """Get or create the global webhook queue manager instance"""
    global _webhook_queue_manager
    if _webhook_queue_manager is None:
        _webhook_queue_manager = WebhookQueueManager(redis_client)
    return _webhook_queue_manager

