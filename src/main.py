import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.core.config import settings
from src.api.v1.api import api_router
from src.api.deps import engine
from src.models.models import SQLModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentinews")

app = FastAPI(title=settings.PROJECT_NAME, version=settings.VERSION, openapi_url=f"{settings.API_V1_STR}/openapi.json")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)
    logger.info("Database initialized")

@app.get("/")
def root():
    return {"message": "Welcome to Sentinews API", "docs": "/docs"}
