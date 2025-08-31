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


class Settings:
    def __init__(self, user: str, 
                  password: str, 
                  dbname: str, 
                  host: str, 
                  port: int) -> None:
        if not self.validator(user, password, dbname, host, port):
            raise ValueError("Invalid settings arguments")
        self.user = user
        self.password = password
        self.dbname = dbname
        self.host = host
        self.port = port

    def asdict(self) -> dict[str, str | int]:
        result = {
            "user": self.user,
            "password": self.password,
            "database": self.dbname,
            "host": self.host,
            "port": self.port
        }
        return result
    
    def validator(self, user: str, 
                  password: str, 
                  dbname: str, 
                  host: str, 
                  port: int
            ) -> bool:
        boolean_attrs = [isinstance(attr, str) for attr in (user, password, dbname, host)]
        if all(boolean_attrs) and isinstance(port, int):
            return True
        return False
         


class DB_connection(metaclass=Singleton):
    def __init__(self, settings):
        self.pool: Pool = None
        self.settings: Settings = settings
        self._is_connected = False

    async def connect(self):
        """Подключение к базе данных с возможностью кастомизации параметров"""
        if self._is_connected:
            return
        
        self.pool = await asyncpg.create_pool(**self.settings.asdict())
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
        
