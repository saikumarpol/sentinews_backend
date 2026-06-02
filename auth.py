# auth.py
import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlmodel import select

from db import get_session, User

# FIX: Use environment variable so secret never changes on restart
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "CHANGE_ME_TO_RANDOM_HEX_IN_PRODUCTION")
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")
ALGORITHM = "HS256"
# FIX: 7 days for development - users won't get logged out constantly
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("TOKEN_EXPIRE_MINUTES", 60 * 24 * 7))

# FastAPI security scheme – used to read Bearer token from Authorization header
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")
# Optional scheme — doesn't raise 401 if no token present (for public routes)
oauth2_optional = OAuth2PasswordBearer(tokenUrl="login", auto_error=False)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    # Bcrypt has a 72-byte limit; truncate if necessary
    password = password[:72]
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None,
) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


class TokenData(BaseModel):
    user_id: Optional[int] = None


def _decode_token(token: str) -> dict:
    """Try to decode token with Supabase secret first, then local secret."""
    if SUPABASE_JWT_SECRET:
        try:
            return jwt.decode(
                token, 
                SUPABASE_JWT_SECRET, 
                algorithms=["HS256"], 
                options={"verify_aud": False}
            )
        except JWTError:
            pass
            
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """
    Decode JWT from Authorization: Bearer <token> and return the User.
    Raises 401 if token is missing or invalid.
    Supports both local JWTs and Supabase JWTs.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = _decode_token(token)
        user_id = payload.get("user_id")
        email = payload.get("email")
        if user_id is None and email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    with get_session() as session:
        if user_id is not None:
            user = session.get(User, user_id)
        else:
            # Supabase token: get/create local user mapping by email
            user = session.exec(select(User).where(User.email == email)).first()
            if not user:
                user = User(email=email, hashed_password="")
                session.add(user)
                session.commit()
                session.refresh(user)
                
        if not user:
            raise credentials_exception
        return user


def get_optional_user(token: Optional[str] = Depends(oauth2_optional)) -> Optional[User]:
    """
    Optional auth — returns None if no/bad token (for public routes that can personalize).
    """
    if not token:
        return None
    try:
        payload = _decode_token(token)
        user_id = payload.get("user_id")
        email = payload.get("email")
        if user_id is None and email is None:
            return None
            
        with get_session() as session:
            if user_id is not None:
                return session.get(User, user_id)
            else:
                user = session.exec(select(User).where(User.email == email)).first()
                if not user:
                    user = User(email=email, hashed_password="")
                    session.add(user)
                    session.commit()
                    session.refresh(user)
                return user
    except JWTError:
        return None
