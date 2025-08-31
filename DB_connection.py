import asyncpg
from asyncpg.pool import Pool
import logging

logger = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class DB_connection(metaclass=Singleton):
    def __init__(self):
        self.pool: Pool = None
        self._is_connected = False

    async def connect(self, **kwargs):
        """Подключение к базе данных с возможностью кастомизации параметров"""
        if self._is_connected:
            return
            
        connection_params = {
            "user": "postgres",
            "password": "postgres",
            "database": "postgres",
            "host": "localhost",
            "port": 5432,
            **kwargs
        }
        
        self.pool = await asyncpg.create_pool(**connection_params)
        self._is_connected = True
        logger.info("Database connection established")

    async def close(self):
        """Закрытие соединения с базой данных"""
        if self.pool:
            await self.pool.close()
            self._is_connected = False
            self.pool = None
            logger.info("Database connection closed")

    async def execute(self, query, *args, **kwargs):
        """Выполнение запроса без возврата результатов"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args, **kwargs)

    @property
    def is_connected(self):
        """Проверка статуса подключения"""
        return self._is_connected
        
