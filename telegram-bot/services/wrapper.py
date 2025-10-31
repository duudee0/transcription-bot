from typing import Dict, List, Optional, Any

import httpx

from config import config

class WrapperService:
    """Сервис для работы с Wrapper API."""
    
    def __init__(self):
        self.base_url = config.WRAPPER_URL
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def create_task(
        self,
        task_type: str,
        input_data: Dict[str, Any],
        service_chain: List[str],
        parameters: Optional[Dict[str, Any]] = None,
        callback_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Создает задачу в wrapper."""
        payload = {
            "task_type": task_type,
            "input_data": input_data,
            "service_chain": service_chain,
            "parameters": parameters or {},
            "timeout": config.DEFAULT_TIMEOUT
        }
        
        if callback_url:
            payload["callback_url"] = callback_url
            
        response = await self.client.post(
            f"{self.base_url}/api/v1/tasks",
            json=payload
        )
        response.raise_for_status()
        
        return response.json()
    
    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Получает статус задачи."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/tasks/{task_id}"
        )
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        """Закрывает HTTP клиент."""
        await self.client.aclose()
