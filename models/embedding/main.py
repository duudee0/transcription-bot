from common.base_service import BaseService
from common.models import TaskMessage, ResultData
from fastapi import HTTPException
import numpy as np
from typing import Dict, Any, List
import os

# Для эмбеддингов можно использовать sentence-transformers
try:
    from sentence_transformers import SentenceTransformer
    import torch
except ImportError:
    # Fallback для тестирования
    pass

class EmbeddingService(BaseService):
    """Сервис для создания векторных представлений текста"""
    
    def __init__(self):
        super().__init__("embedding-service", "1.0")
        self.model = None
        self.model_name = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
        self._load_model()
    
    def _load_model(self):
        """Загрузка модели для эмбеддингов"""
        try:
            # В продакшене используем реальную модель
            self.model = SentenceTransformer(self.model_name)
            print(f"✅ Model loaded: {self.model_name}")
        except Exception as e:
            print(f"⚠️ Could not load model {self.model_name}: {e}")
            print("🔧 Using dummy embeddings for testing")
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задач для embedding service"""
        if task_message.data.task_type not in ["generate_embeddings", "get_embedding_dim"]:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported task type: {task_message.data.task_type}"
            )
        
        if task_message.data.task_type == "generate_embeddings":
            texts = task_message.data.input_data.get("texts", [])
            if not texts or not isinstance(texts, list):
                raise ValueError("Texts list is required for generate_embeddings task")
    
    async def _process_task_logic(self, task_message: TaskMessage) -> ResultData:
        """Логика обработки задач эмбеддингов"""
        task_type = task_message.data.task_type
        
        if task_type == "generate_embeddings":
            return await self._generate_embeddings(task_message.data.input_data)
        elif task_type == "get_embedding_dim":
            return await self._get_embedding_dim()
    
    async def _generate_embeddings(self, input_data: Dict[str, Any]) -> ResultData:
        """Генерация эмбеддингов для текстов"""
        texts = input_data.get("texts", [])
        normalize = input_data.get("normalize", True)
        
        try:
            if self.model:
                # Реальные эмбеддинги
                embeddings = self.model.encode(texts, normalize_embeddings=normalize)
                embeddings_list = embeddings.tolist()
            else:
                # Тестовые эмбеддинги (заглушка)
                embeddings_list = self._generate_dummy_embeddings(texts, 384)
            
            return ResultData(
                success=True,
                result={
                    "embeddings": embeddings_list,
                    "texts": texts,
                    "dimension": len(embeddings_list[0]) if embeddings_list else 0,
                    "count": len(embeddings_list)
                },
                execution_metadata={
                    "service": "embedding-service",
                    "model": self.model_name if self.model else "dummy",
                    "normalized": normalize
                }
            )
            
        except Exception as e:
            return ResultData(
                success=False,
                error_message=f"Embedding generation failed: {str(e)}",
                execution_metadata={"service": "embedding-service", "error": True}
            )
    
    async def _get_embedding_dim(self) -> ResultData:
        """Получение размерности эмбеддингов"""
        if self.model:
            dim = self.model.get_sentence_embedding_dimension()
        else:
            dim = 384  # Размерность для тестовой модели
        
        return ResultData(
            success=True,
            result={"dimension": dim},
            execution_metadata={"service": "embedding-service"}
        )
    
    def _generate_dummy_embeddings(self, texts: List[str], dimension: int) -> List[List[float]]:
        """Генерация тестовых эмбеддингов"""
        embeddings = []
        for i, text in enumerate(texts):
            # Простая детерминированная "эмбеддинг" для тестирования
            embedding = [float((hash(text + str(i)) % 1000) / 1000) for _ in range(dimension)]
            # Нормализация
            norm = sum(x*x for x in embedding) ** 0.5
            normalized = [x/norm for x in embedding]
            embeddings.append(normalized)
        return embeddings

# Запуск сервиса
if __name__ == "__main__":
    service = EmbeddingService()
    service.run(port=int(os.getenv("PORT", 8004)))