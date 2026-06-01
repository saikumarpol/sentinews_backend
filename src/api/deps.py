from typing import Generator, Optional
from fastapi import Depends, HTTPException, status
from jose import jwt, JWTError
from sqlmodel import Session, create_engine
from src.core.config import settings
from src.core.security import oauth2_scheme
from src.models.models import User

engine = create_engine(settings.DATABASE_URL, echo=False)

def get_db() -> Generator:
    with Session(engine) as session:
        yield session

def get_current_user(db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id: str = payload.get("user_id")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = db.get(User, int(user_id))
    if not user:
        raise credentials_exception
    return user
