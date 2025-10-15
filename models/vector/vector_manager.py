from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, 
    Filter, FieldCondition, MatchValue,
    SearchRequest
)
from typing import List, Dict, Any, Optional
import uuid
from datetime import datetime
import logging

logger = logging.getLogger("vector-manager")

class VectorManager:
    """Менеджер для работы с Qdrant"""
    
    def __init__(self, host: str = "localhost", port: int = 6333):
        self.client = QdrantClient(host=host, port=port)
        logger.info(f"🔗 Connected to Qdrant at {host}:{port}")
    
    def create_collection(self, collection_name: str, vector_size: int, distance: Distance = Distance.COSINE):
        """Создание коллекции"""
        try:
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=vector_size, distance=distance),
            )
            logger.info(f"✅ Collection created: {collection_name}")
            return True
        except Exception as e:
            logger.warning(f"Collection {collection_name} might already exist: {e}")
            return False
    
    def store_vectors(
        self, 
        collection_name: str,
        vectors: List[List[float]],
        payloads: List[Dict[str, Any]],
        ids: Optional[List[str]] = None
    ) -> bool:
        """Сохранение векторов с метаданными"""
        try:
            if not ids:
                ids = [str(uuid.uuid4()) for _ in vectors]
            
            points = [
                PointStruct(
                    id=idx,
                    vector=vector,
                    payload={**payload, "timestamp": datetime.now().isoformat()}
                )
                for idx, vector, payload in zip(ids, vectors, payloads)
            ]
            
            operation_info = self.client.upsert(
                collection_name=collection_name,
                wait=True,
                points=points
            )
            
            logger.info(f"✅ Stored {len(vectors)} vectors in {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to store vectors: {e}")
            return False
    
    def search_vectors(
        self,
        collection_name: str,
        query_vector: List[float],
        limit: int = 10,
        score_threshold: Optional[float] = None,
        filter_conditions: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Поиск похожих векторов"""
        try:
            search_filters = None
            if filter_conditions:
                search_filters = Filter(
                    must=[
                        FieldCondition(key=key, match=MatchValue(value=value))
                        for key, value in filter_conditions.items()
                    ]
                )
            
            search_result = self.client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                limit=limit,
                score_threshold=score_threshold,
                query_filter=search_filters
            )
            
            results = []
            for hit in search_result:
                results.append({
                    "id": hit.id,
                    "score": hit.score,
                    "payload": hit.payload,
                    "vector": hit.vector
                })
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Vector search failed: {e}")
            return []
    
    def get_collection_info(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """Получение информации о коллекции"""
        try:
            collections = self.client.get_collections().collections
            for collection in collections:
                if collection.name == collection_name:
                    return {
                        "name": collection.name,
                        "status": "exists"
                    }
            return None
        except Exception as e:
            logger.error(f"❌ Failed to get collection info: {e}")
            return None
    
    def delete_vectors(self, collection_name: str, vector_ids: List[str]) -> bool:
        """Удаление векторов по ID"""
        try:
            self.client.delete(
                collection_name=collection_name,
                points_selector=vector_ids
            )
            logger.info(f"✅ Deleted {len(vector_ids)} vectors from {collection_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to delete vectors: {e}")
            return False