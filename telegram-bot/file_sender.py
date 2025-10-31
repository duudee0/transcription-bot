"""Сервис для отправки файлов и результатов пользователям."""
import os
import json
import tempfile
import aiofiles
import httpx
from typing import Any, Optional
from aiogram import Bot
from aiogram.types import FSInputFile
from aiogram.enums import ParseMode
from html import escape

from config import config


class FileSender:
    """Сервис для отправки файлов и результатов пользователям."""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def send_task_result(
        self, 
        chat_id: int, 
        task_id: str, 
        status: str, 
        result: Any, 
        error: Any = None
    ) -> None:
        """
        Универсальная логика отправки результата в чат.
        
        Args:
            chat_id: ID чата для отправки
            task_id: ID задачи
            status: Статус задачи
            result: Результат задачи (может содержать URL файла)
            error: Ошибка (если есть)
        """
        try:
            # Пытаемся отправить как аудио файл, если есть URL
            if await self._try_send_audio_file(chat_id, task_id, status, result):
                return
            
            # Если не аудио, отправляем как текст с красивым форматированием
            await self._send_beautiful_result(chat_id, task_id, status, result, error)
            
        except Exception as e:
            print(f"❌ Unexpected error while sending result to chat {chat_id}: {e}")
            # Fallback: простая текстовая отправка при ошибке
            await self._send_fallback_result(chat_id, task_id, status, result, error)
    
    async def _try_send_audio_file(
        self, 
        chat_id: int, 
        task_id: str, 
        status: str, 
        result: Any
    ) -> bool:
        """Пытается отправить результат как аудио файл. Возвращает True если успешно."""
        audio_url = self._extract_audio_url(result)
        if not audio_url:
            return False
        
        try:
            # Скачиваем файл во временную директорию
            temp_path = await self._download_file_to_temp(audio_url)
            
            try:
                # Пытаемся отправить как аудио
                caption = self._format_audio_caption(task_id, status)
                await self.bot.send_audio(
                    chat_id, 
                    FSInputFile(temp_path), 
                    caption=caption
                )
                return True
                
            except Exception:
                # Если не получилось как аудио, пробуем как документ
                caption = self._format_document_caption(task_id, status)
                await self.bot.send_document(
                    chat_id, 
                    FSInputFile(temp_path), 
                    caption=caption
                )
                return True
                
        except Exception as e:
            # Если загрузка/отправка не удалась
            print(f"⚠️ Failed to download/send audio from {audio_url}: {e}")
            error_msg = self._format_download_error(task_id, audio_url, e)
            await self.bot.send_message(chat_id, error_msg)
            return False
        
        finally:
            # Всегда удаляем временный файл
            self._cleanup_temp_file(temp_path)
    
    def _format_audio_caption(self, task_id: str, status: str) -> str:
        """Форматирует заголовок для аудио файла."""
        task_short = task_id[:8]
        status_icon = self._get_status_icon(status)
        return f"{status_icon} Результат задачи #{task_short}\n\n🎧 Аудио готово к прослушиванию!"
    
    def _format_document_caption(self, task_id: str, status: str) -> str:
        """Форматирует заголовок для документа."""
        task_short = task_id[:8]
        status_icon = self._get_status_icon(status)
        return f"{status_icon} Результат задачи #{task_short}\n\n📎 Файл готов к скачиванию!"
    
    def _format_download_error(self, task_id: str, audio_url: str, error: Exception) -> str:
        """Форматирует сообщение об ошибке загрузки."""
        task_short = task_id[:8]
        return (
            f"⚠️ <b>Внимание!</b>\n\n"
            f"Задача <code>#{task_short}</code> выполнена, но возникла проблема с загрузкой аудио.\n\n"
            f"<b>URL:</b> <code>{escape(audio_url)}</code>\n"
            f"<b>Ошибка:</b> <code>{escape(str(error))}</code>\n\n"
            f"Результат будет отправлен текстовым сообщением."
        )
    
    async def _send_beautiful_result(
        self, 
        chat_id: int, 
        task_id: str, 
        status: str, 
        result: Any, 
        error: Any
    ) -> None:
        """Отправляет красивый текстовый результат."""
        try:
            message = self._format_beautiful_message(task_id, status, result, error)
            await self.bot.send_message(
                chat_id, 
                message, 
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"❌ Error sending beautiful result: {e}")
            await self._send_fallback_result(chat_id, task_id, status, result, error)
    
    def _format_beautiful_message(self, task_id: str, status: str, result: Any, error: Any) -> str:
        """Форматирует красивое сообщение с результатом."""
        task_short = task_id[:8]
        status_icon = self._get_status_icon(status)
        status_text = self._get_status_text(status)
        
        # Основной заголовок
        header = f"{status_icon} <b>Задача #{task_short}</b> - {status_text}\n\n"
        
        # Тело сообщения
        body = ""
        
        if status == "completed" and result:
            if isinstance(result, dict) and "text" in result:
                # Текстовый результат
                text_content = result["text"]
                truncated_text = self._safe_truncate(text_content, 3000)
                body = (
                    f"📝 <b>Результат:</b>\n"
                    f"<blockquote expandable>{escape(truncated_text)}</blockquote>"
                )
            else:
                # Структурированный результат
                body = self._format_structured_result(result)
        
        elif status == "error":
            body = self._format_error_section(error, result)
        
        elif status == "timeout":
            body = (
                "⏳ <b>Время выполнения истекло</b>\n\n"
                "Задача заняла больше времени, чем ожидалось. "
                "Попробуйте повторить запрос или обратитесь к администратору."
            )
        
        # Добавляем разделитель если есть и заголовок и тело
        if body:
            return header + body
        else:
            return header + "ℹ️ <i>Нет дополнительной информации</i>"
    
    def _format_structured_result(self, result: Any) -> str:
        """Форматирует структурированный результат."""
        if isinstance(result, dict):
            # Пытаемся извлечь полезные поля
            sections = []
            
            if "answer" in result:
                answer = self._safe_truncate(str(result["answer"]), 2000)
                sections.append(f"💬 <b>Ответ:</b>\n<blockquote expandable>{escape(answer)}</blockquote>")
            
            if "summary" in result:
                summary = self._safe_truncate(str(result["summary"]), 1500)
                sections.append(f"📊 <b>Сводка:</b>\n<blockquote expandable>{escape(summary)}</blockquote>")
            
            # Добавляем остальные поля как JSON
            remaining_fields = {k: v for k, v in result.items() 
                              if k not in ["answer", "summary", "text", "audio_url", "file_url"]}
            
            if remaining_fields:
                json_output = json.dumps(remaining_fields, ensure_ascii=False, indent=2)
                truncated_json = self._safe_truncate(json_output, 1000)
                sections.append(f"🔧 <b>Детали:</b>\n<code>{escape(truncated_json)}</code>")
            
            return "\n\n".join(sections) if sections else "📄 <i>Структурированные данные</i>"
        
        else:
            # Простой объект
            text = self._safe_truncate(str(result), 3000)
            return f"📄 <b>Результат:</b>\n<blockquote expandable>{escape(text)}</blockquote>"
    
    def _format_error_section(self, error: Any, result: Any) -> str:
        """Форматирует раздел с ошибкой."""
        error_text = ""
        
        if error:
            error_str = self._safe_truncate(str(error), 1500)
            error_text = f"🚫 <b>Ошибка:</b>\n<code>{escape(error_str)}</code>"
        
        result_text = ""
        if result:
            result_str = self._safe_truncate(str(result), 1000)
            result_text = f"\n\n📋 <b>Частичный результат:</b>\n<code>{escape(result_str)}</code>"
        
        return error_text + result_text
    
    def _get_status_icon(self, status: str) -> str:
        """Возвращает иконку для статуса."""
        icons = {
            "completed": "✅",
            "error": "❌", 
            "timeout": "⏰",
            "processing": "🔄",
            "pending": "⏳"
        }
        return icons.get(status, "📋")
    
    def _get_status_text(self, status: str) -> str:
        """Возвращает текстовое описание статуса."""
        texts = {
            "completed": "Завершена",
            "error": "Ошибка",
            "timeout": "Таймаут",
            "processing": "В процессе",
            "pending": "Ожидает"
        }
        return texts.get(status, status)
    
    def _extract_audio_url(self, result: Any) -> Optional[str]:
        """Извлекает URL аудио из результата."""
        if not isinstance(result, dict):
            return None
        
        # Проверяем возможные поля с URL
        for key in ("audio_url", "file_url", "url", "audio"):
            if key in result and result[key]:
                url = result[key]
                if isinstance(url, str) and url.startswith(('http://', 'https://')):
                    return url
        return None
    
    async def _download_file_to_temp(self, url: str) -> str:
        """Скачивает файл по URL во временный файл."""
        temp_dir = tempfile.gettempdir()
        filename = f"tg_audio_{hash(url)}.tmp"
        temp_path = os.path.join(temp_dir, filename)
        
        async with self.client.stream("GET", url) as response:
            response.raise_for_status()
            async with aiofiles.open(temp_path, "wb") as file:
                async for chunk in response.aiter_bytes():
                    if chunk:
                        await file.write(chunk)
        
        return temp_path
    
    def _cleanup_temp_file(self, file_path: str) -> None:
        """Удаляет временный файл."""
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"⚠️ Failed to cleanup temp file {file_path}: {e}")
    
    async def _send_fallback_result(
        self, 
        chat_id: int, 
        task_id: str, 
        status: str, 
        result: Any, 
        error: Any
    ) -> None:
        """Fallback: простая текстовая отправка при ошибках."""
        try:
            task_short = task_id[:8]
            message = f"📋 Задача #{task_short}\nСтатус: {status}"
            
            if result:
                result_str = self._safe_truncate(str(result), 2000)
                message += f"\n\nРезультат: {escape(result_str)}"
            
            if error:
                error_str = self._safe_truncate(str(error), 1000)
                message += f"\n\nОшибка: {escape(error_str)}"
            
            await self.bot.send_message(chat_id, message)
        except Exception as e:
            print(f"❌ Even fallback failed: {e}")
    
    def _safe_truncate(self, text: str, limit: int = 3500) -> str:
        """Безопасно обрезает текст до лимита."""
        if len(text) <= limit:
            return text
        return text[:limit - 100] + "\n\n... (текст обрезан)"