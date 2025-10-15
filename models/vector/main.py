from common.base_service import BaseService
from common.models import TaskMessage, ResultData, VectorOperationType
from fastapi import HTTPException
from vector_manager import VectorManager
import os
from typing import Dict, Any

class VectorService(BaseService):
    """Сервис для работы с векторной базой Qdrant"""
    
    def __init__(self):
        super().__init__("vector-service", "1.0")
        self.vector_manager = VectorManager(
            host=os.getenv("QDRANT_HOST", "qdrant"),
            port=int(os.getenv("QDRANT_PORT", "6333"))
        )
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация векторных задач"""
        supported_operations = [
            "create_collection", "store_vectors", "search_vectors",
            "get_collection_info", "delete_vectors"
        ]
        
        if task_message.data.task_type not in supported_operations:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported vector operation: {task_message.data.task_type}"
            )
    
    async def _process_task_logic(self, task_message: TaskMessage) -> ResultData:
        """Логика обработки векторных операций"""
        task_type = task_message.data.task_type
        input_data = task_message.data.input_data
        
        try:
            if task_type == "create_collection":
                return await self._create_collection(input_data)
            elif task_type == "store_vectors":
                return await self._store_vectors(input_data)
            elif task_type == "search_vectors":
                return await self._search_vectors(input_data)
            elif task_type == "get_collection_info":
                return await self._get_collection_info(input_data)
            elif task_type == "delete_vectors":
                return await self._delete_vectors(input_data)
                
        except Exception as e:
            return ResultData(
                success=False,
                error_message=str(e),
                execution_metadata={"service": "vector-service", "error": True}
            )
    
    async def _create_collection(self, input_data: Dict[str, Any]) -> ResultData:
        """Создание коллекции"""
        collection_name = input_data["collection_name"]
        vector_size = input_data["vector_size"]
        
        success = self.vector_manager.create_collection(collection_name, vector_size)
        
        return ResultData(
            success=success,
            result={"collection_name": collection_name, "created": success},
            execution_metadata={"service": "vector-service", "operation": "create_collection"}
        )
    
    async def _store_vectors(self, input_data: Dict[str, Any]) -> ResultData:
        """Сохранение векторов"""
        collection_name = input_data["collection_name"]
        vectors = input_data["vectors"]
        payloads = input_data["payloads"]
        ids = input_data.get("ids")
        
        success = self.vector_manager.store_vectors(collection_name, vectors, payloads, ids)
        
        return ResultData(
            success=success,
            result={
                "collection_name": collection_name,
                "stored_count": len(vectors) if success else 0,
                "vector_ids": ids
            },
            execution_metadata={"service": "vector-service", "operation": "store_vectors"}
        )
    
    async def _search_vectors(self, input_data: Dict[str, Any]) -> ResultData:
        """Поиск похожих векторов"""
        collection_name = input_data["collection_name"]
        query_vector = input_data["query_vector"]
        limit = input_data.get("limit", 10)
        score_threshold = input_data.get("score_threshold")
        filter_conditions = input_data.get("filter_conditions")
        
        results = self.vector_manager.search_vectors(
            collection_name, query_vector, limit, score_threshold, filter_conditions
        )
        
        return ResultData(
            success=True,
            result={
                "collection_name": collection_name,
                "results": results,
                "count": len(results)
            },
            execution_metadata={"service": "vector-service", "operation": "search_vectors"}
        )
    
    async def _get_collection_info(self, input_data: Dict[str, Any]) -> ResultData:
        """Получение информации о коллекции"""
        collection_name = input_data["collection_name"]
        info = self.vector_manager.get_collection_info(collection_name)
        
        return ResultData(
            success=info is not None,
            result={"collection_info": info},
            execution_metadata={"service": "vector-service", "operation": "get_collection_info"}
        )
    
    async def _delete_vectors(self, input_data: Dict[str, Any]) -> ResultData:
        """Удаление векторов"""
        collection_name = input_data["collection_name"]
        vector_ids = input_data["vector_ids"]
        
        success = self.vector_manager.delete_vectors(collection_name, vector_ids)
        
        return ResultData(
            success=success,
            result={
                "collection_name": collection_name,
                "deleted_count": len(vector_ids) if success else 0
            },
            execution_metadata={"service": "vector-service", "operation": "delete_vectors"}
        )

if __name__ == "__main__":
    service = VectorService()
    service.run(port=int(os.getenv("PORT", 8005)))