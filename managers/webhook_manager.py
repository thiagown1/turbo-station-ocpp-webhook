import asyncio
import aiohttp
import os
from typing import Optional, Dict, Any
from datetime import datetime
from log_config import root_logger


class WebhookManager:
    """
    Simple webhook manager for sending events to external API with retry logic.
    """
    
    def __init__(self):
        self.api_url = os.getenv('API_ENDPOINT', 'http://localhost:3000')
        self.api_key = os.getenv('API_KEY', '')
        self.timeout = int(os.getenv('WEBHOOK_TIMEOUT', '10'))
        self.max_retries = int(os.getenv('WEBHOOK_MAX_RETRIES', '3'))
        self.enabled = os.getenv('WEBHOOK_ENABLED', 'true').lower() in ['true', '1', 'yes']
        
        # Create a session that will be reused
        self._session = None
        
        root_logger.info(f"WebhookManager initialized: enabled={self.enabled}, url={self.api_url}")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def close(self):
        """Close the aiohttp session"""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def send_webhook(self, endpoint: str, payload: Dict[str, Any], 
                          priority: str = 'normal') -> Dict[str, Any]:
        """
        Send webhook to external API with retry logic.
        
        Args:
            endpoint: API endpoint (e.g., '/webhook/stop-transaction')
            payload: Data to send
            priority: 'high', 'normal', or 'low' (for future use)
        
        Returns:
            Dict with success status and details
        """
        if not self.enabled:
            root_logger.debug(f"Webhook disabled, skipping: {endpoint}")
            return {
                'success': False,
                'skipped': True,
                'reason': 'webhooks_disabled'
            }
        
        url = f"{self.api_url}{endpoint}"
        root_logger.info(f"Sending webhook to {url} with payload: {payload}")
        headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.api_key,
            'X-Webhook-Priority': priority
        }
        
        # Add retry logic
        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                
                start_time = asyncio.get_event_loop().time()
                
                async with session.post(url, json=payload, headers=headers) as response:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    
                    if response.status == 200:
                        response_data = await response.json()
                        root_logger.info(
                            f"✅ Webhook success: {endpoint} "
                            f"(attempt={attempt+1}, elapsed={elapsed:.3f}s)"
                        )
                        return {
                            'success': True,
                            'attempt': attempt + 1,
                            'elapsed': elapsed,
                            'response': response_data
                        }
                    else:
                        error_text = await response.text()
                        root_logger.warning(
                            f"⚠️ Webhook failed: {endpoint} "
                            f"(attempt={attempt+1}, status={response.status}, error={error_text})"
                        )
                        
                        # Don't retry on 4xx errors (client errors)
                        if 400 <= response.status < 500:
                            return {
                                'success': False,
                                'attempt': attempt + 1,
                                'error': f'Client error: {response.status}',
                                'error_text': error_text
                            }
                        
                        # Retry on 5xx errors
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        
                        return {
                            'success': False,
                            'attempt': attempt + 1,
                            'error': f'Server error: {response.status}',
                            'error_text': error_text
                        }
            
            except asyncio.TimeoutError:
                root_logger.warning(
                    f"⏱️ Webhook timeout: {endpoint} "
                    f"(attempt={attempt+1}, timeout={self.timeout}s)"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                return {
                    'success': False,
                    'attempt': attempt + 1,
                    'error': 'timeout',
                    'timeout': self.timeout
                }
            
            except aiohttp.ClientError as e:
                root_logger.warning(
                    f"🔌 Webhook connection error: {endpoint} "
                    f"(attempt={attempt+1}, error={str(e)})"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                return {
                    'success': False,
                    'attempt': attempt + 1,
                    'error': 'connection_error',
                    'error_message': str(e)
                }
            
            except Exception as e:
                root_logger.exception(
                    f"❌ Webhook unexpected error: {endpoint} "
                    f"(attempt={attempt+1})"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                return {
                    'success': False,
                    'attempt': attempt + 1,
                    'error': 'unexpected_error',
                    'error_message': str(e)
                }
        
        # Should not reach here, but just in case
        return {
            'success': False,
            'error': 'max_retries_exceeded'
        }
    
    async def send_stop_transaction(self, transaction_data: Dict[str, Any], 
                                    meter_stop: int, timestamp: str) -> Dict[str, Any]:
        """
        Send StopTransaction webhook with proper payload structure.
        
        Args:
            transaction_data: Transaction data from cache/Firebase
            meter_stop: Final meter reading
            timestamp: Stop timestamp
        
        Returns:
            Webhook result
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
        
        payload = {
            'transaction_id': transaction_data.get('transaction_id'),
            'charge_point_id': transaction_data.get('charge_point_id'),
            'connector_id': transaction_data.get('connector_id'),
            'user_id': transaction_data.get('user_id'),
            'meter_start': meter_start,
            'meter_stop': meter_stop,
            'timestamp_stop': timestamp,
            'kwh_price': transaction_data.get('kwh_price'),
            'previous_energy': previous_energy,
            'previous_energy_source': source,
            'transaction_data': transaction_data  # Send full transaction data for context
        }
        
        root_logger.info(
            f"📤 Sending StopTransaction webhook: "
            f"transaction_id={payload['transaction_id']}, "
            f"previous_energy={previous_energy} (source={source})"
        )
        
        return await self.send_webhook('/api/webhook/stop-transaction', payload, priority='high')
    
    async def send_status_notification(self, charge_point_id: str, connector_id: int,
                                       status: str, error_code: str, 
                                       timestamp: str, **kwargs) -> Dict[str, Any]:
        """
        Send StatusNotification webhook.
        
        Args:
            charge_point_id: Charger ID
            connector_id: Connector ID
            status: Connector status
            error_code: Error code
            timestamp: Notification timestamp
            **kwargs: Additional data
        
        Returns:
            Webhook result
        """
        payload = {
            'charge_point_id': charge_point_id,
            'connector_id': connector_id,
            'status': status,
            'error_code': error_code,
            'timestamp': timestamp,
            **kwargs
        }
        
        root_logger.debug(
            f"📤 Sending StatusNotification webhook: "
            f"charger={charge_point_id}, connector={connector_id}, status={status}"
        )
        
        return await self.send_webhook('/api/webhook/status-notification', payload, priority='normal')
    
    async def send_meter_values(self, transaction_id: int, charge_point_id: str,
                                current_energy: float, timestamp: str,
                                **kwargs) -> Dict[str, Any]:
        """
        Send MeterValues webhook for incremental billing.
        
        Args:
            transaction_id: Transaction ID
            charge_point_id: Charger ID
            current_energy: Current energy reading
            timestamp: Reading timestamp
            **kwargs: Additional data (battery, power, etc.)
        
        Returns:
            Webhook result
        """
        payload = {
            'transaction_id': transaction_id,
            'charge_point_id': charge_point_id,
            'current_energy': current_energy,
            'timestamp': timestamp,
            **kwargs
        }
        
        root_logger.debug(
            f"📤 Sending MeterValues webhook: "
            f"transaction_id={transaction_id}, energy={current_energy}Wh"
        )
        
        return await self.send_webhook('/api/webhook/meter-values', payload, priority='normal')


# Global singleton instance
_webhook_manager = None


def get_webhook_manager() -> WebhookManager:
    """Get or create the global webhook manager instance"""
    global _webhook_manager
    if _webhook_manager is None:
        _webhook_manager = WebhookManager()
    return _webhook_manager

