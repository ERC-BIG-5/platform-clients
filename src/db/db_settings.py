from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.const import ENV_FILE_PATH, BASE_DATA_PATH


class PostgresCredentials(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    DB_NAME: str = "big5"

    @property
    def connection_str(self) -> str:
        return (f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.DB_NAME}")


class SqliteSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    SQLITE_FILE_PATH: str = Field((BASE_DATA_PATH / "db.sqlite").absolute().as_posix())
