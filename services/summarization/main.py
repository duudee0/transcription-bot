from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data
from fastapi import HTTPException
import os
import torch
from transformers import MBartTokenizer, MBartForConditionalGeneration
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import asyncio

class SummarizationService(BaseService):
    """Сервис для суммаризации текста на русском языке"""
    
    def __init__(self):
        super().__init__("summarization-service", "1.0")
        
        # Конфигурация модели
        self.model_name = os.getenv("SUMMARIZATION_MODEL", "IlyaGusev/mbart_ru_sum_gazeta")
        self.max_input_length = int(os.getenv("MAX_INPUT_LENGTH", "600"))
        self.device = os.getenv("DEVICE", "cuda" if torch.cuda.is_available() else "cpu")
        
        print(f"🔄 Loading summarization model: {self.model_name}")
        print(f"📱 Using device: {self.device}")
        
        # Загружаем модель и токенизатор
        try:
            # Сначала загружаем токенизатор
            self.tokenizer = MBartTokenizer.from_pretrained(self.model_name)
            
            # Затем загружаем модель с игнорированием несовпадающих весов
            self.model = MBartForConditionalGeneration.from_pretrained(
                self.model_name,
                ignore_mismatched_sizes=True  # Игнорировать несовпадающие размеры
            )
            
            # Принудительно изменяем размер эмбеддингов
            self.model.resize_token_embeddings(len(self.tokenizer))
            
            # Перемещаем модель на GPU если доступно
            if self.device == "cuda" and torch.cuda.is_available():
                self.model = self.model.cuda()
                print("✅ Model moved to GPU")
            else:
                print("ℹ️  Using CPU for inference")
                
            print(f"✅ Summarization model {self.model_name} loaded successfully")
            
        except Exception as e:
            print(f"❌ Failed to load model: {str(e)}")
            raise

    def _can_handle_task_type(self, task_type: str) -> bool:
        """Определяет, может ли сервис обработать тип задачи"""
        supported_task_types = [
            "summarize_text",
            "text_summarization", 
            "generate_summary",
            "abstractive_summarization"
        ]
        return task_type in supported_task_types

    def _health_handler(self):
        """Проверка здоровья сервиса"""
        try:
            cuda_available = torch.cuda.is_available()
            current_device = "cuda" if next(self.model.parameters()).is_cuda else "cpu"
            
            return {
                "status": "ok",
                "service": self.service_name,
                "model": self.model_name,
                "device": current_device,
                "cuda_available": cuda_available,
                "model_loaded": self.model is not None,
                "tokenizer_loaded": self.tokenizer is not None,
                "max_input_length": self.max_input_length
            }
        except Exception as e:
            return {
                "status": "error",
                "service": self.service_name,
                "error": str(e)
            }

    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для суммаризации"""
        if task_message.data.payload_type != PayloadType.TEXT:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported payload type: {task_message.data.payload_type}. Expected TEXT"
            )
        
        text = task_message.data.payload.get("text", "")
        if not text or not text.strip():
            raise HTTPException(status_code=400, detail="Text is required for summarization")

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Логика обработки задачи суммаризации"""
        text = task_message.data.payload.get("text", "")
        
        # Генерируем суммаризацию
        summary = await self._generate_summary(text)
        
        return Data(
            payload_type=PayloadType.TEXT,
            payload={
                "task": "text_summarization",
                "original_text": text,
                "summary": summary,
                "model_used": self.model_name,
                "input_length": len(text),
                "summary_length": len(summary)
            },
            execution_metadata={
                "task_type": "summarize_text",
                "service": "summarization-service",
                "model": self.model_name,
                "device": self.device
            }
        )
    
    async def _generate_summary(self, text: str) -> str:
        """Генерирует суммаризацию текста"""
        try:
            # Запускаем в thread pool, так как это блокирующая операция
            loop = asyncio.get_event_loop()
            summary = await loop.run_in_executor(
                None, 
                self._sync_generate_summary, 
                text
            )
            return summary
            
        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail=f"Summarization failed: {str(e)}"
            )
    
    def _sync_generate_summary(self, text: str) -> str:
        """Синхронная генерация суммаризации"""
        try:
            # Токенизация входного текста
            input_ids = self.tokenizer(
                [text],
                max_length=self.max_input_length,
                truncation=True,
                return_tensors="pt",
            )["input_ids"]
            
            # Перемещаем на GPU если используется
            if self.device == "cuda" and torch.cuda.is_available():
                input_ids = input_ids.cuda()
            
            # Генерация суммаризации
            output_ids = self.model.generate(
                input_ids=input_ids,
                no_repeat_ngram_size=4,
                num_beams=5,           # Улучшает качество генерации
                length_penalty=2.0,     # Поощряет более длинные summary
                min_length=30,          # Минимальная длина summary
                max_length=100,         # Максимальная длина summary
                early_stopping=True
            )[0]
            
            # Декодируем результат
            summary = self.tokenizer.decode(output_ids, skip_special_tokens=True)
            return summary.strip()
            
        except Exception as e:
            print(f"🚨 Summarization error: {str(e)}")
            raise RuntimeError(f"Summarization failed: {str(e)}") from e


# Создаем и запускаем сервис
service = SummarizationService()

if __name__ == "__main__":
    service.run()