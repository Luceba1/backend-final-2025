from pydantic import BaseSettings

class Settings(BaseSettings):
    REDIS_URL: str | None = None
    CACHE_ENABLED: bool = True

    class Config:
        env_file = ".env"


# instancia global
settings = Settings()
