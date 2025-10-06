import asyncpg
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from typing import List, Optional

MAX_TEXTS = 10

class Database:
    def __init__(self):
        self.pool = None

    async def create_pool(self):
        self.pool = await asyncpg.create_pool(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            min_size=1,
            max_size=10
        )

    async def save_text(self, user_id: int, diarized_text: str, raw_text: str, 
                       username: Optional[str] = None, full_name: Optional[str] = None):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('''
                    INSERT INTO users (user_id, username, full_name)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id) DO UPDATE
                    SET username = EXCLUDED.username,
                        full_name = EXCLUDED.full_name
                ''', user_id, username, full_name)
                
                await conn.execute('''
                    INSERT INTO transcriptions (user_id, diarized_text, raw_text)
                    VALUES ($1, $2, $3)
                ''', user_id, diarized_text, raw_text)
                
                await conn.execute('''
                    DELETE FROM transcriptions
                    WHERE ctid NOT IN (
                        SELECT ctid FROM transcriptions
                        WHERE user_id = $1
                        ORDER BY created_at DESC
                        LIMIT $2
                    ) AND user_id = $1
                ''', user_id, MAX_TEXTS)

    async def get_texts(self, user_id: int, with_speakers: bool = True) -> List[str]:
        async with self.pool.acquire() as conn:
            column = 'diarized_text' if with_speakers else 'raw_text'
            records = await conn.fetch(f'''
                SELECT {column} FROM transcriptions
                WHERE user_id = $1
                ORDER BY created_at DESC
            ''', user_id)
            return [record[column] for record in records]

    async def get_current_index(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.fetchval('''
                SELECT current_text_index FROM users WHERE user_id = $1
            ''', user_id)
            return result if result is not None else 0

    async def set_current_index(self, user_id: int, index: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, current_text_index)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE
                SET current_text_index = EXCLUDED.current_text_index
            ''', user_id, index)

db = Database()