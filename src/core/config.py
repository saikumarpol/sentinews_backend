import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    PROJECT_NAME: str = "Sentinews API"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/api/v1"
    
    # API Keys
    NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY", "")
    TWELVEDATA_API_KEY: str = os.getenv("TWELVEDATA_API_KEY", "")
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./sentinews.db")
    
    # Auth
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-super-secret-key-change-me")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days
    
    # Cache
    CACHE_DURATION: int = int(os.getenv("CACHE_DURATION", "300"))

settings = Settings()
