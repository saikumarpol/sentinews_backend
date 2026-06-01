from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlmodel import select, Session
from src.core.security import create_access_token, hash_password, verify_password
from src.schemas.schemas import Token, SignupBody, ForgotPasswordBody
from src.models.models import User
from src.api.deps import get_db

router = APIRouter()

@router.post("/signup", response_model=Token)
def signup(body: SignupBody, db: Session = Depends(get_db)):
    email_clean = body.email.lower().strip()
    existing = db.exec(select(User).where(User.email == email_clean)).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user = User(email=email_clean, hashed_password=hash_password(body.password))
    db.add(user)
    db.commit()
    db.refresh(user)
    
    token = create_access_token(user.id)
    return {"access_token": token, "token_type": "bearer"}

@router.post("/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    email_clean = form_data.username.lower().strip()
    user = db.exec(select(User).where(User.email == email_clean)).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password")
    
    token = create_access_token(user.id)
    return {"access_token": token, "token_type": "bearer"}

@router.post("/forgot-password")
def forgot_password(body: ForgotPasswordBody, db: Session = Depends(get_db)):
    # In a real app, send reset email.
    return {"message": "If an account exists, a reset link has been sent."}
