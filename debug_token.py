#!/usr/bin/env python3
# backend/debug_token.py
import os
import sys
from datetime import datetime
from jose import jwt, JWTError
from dotenv import load_dotenv

load_dotenv()

TOKEN = "PASTE_YOUR_TOKEN_HERE"  # Change this or pass via arg
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "CHANGE_ME_TO_RANDOM_HEX_IN_PRODUCTION")

if len(sys.argv) > 1:
    TOKEN = sys.argv[1]

print("=" * 50)
print("TOKEN DEBUG")
print("=" * 50)

if TOKEN == "PASTE_YOUR_TOKEN_HERE":
    print("Please paste your token into the script or pass it as an argument.")
    sys.exit(0)

# Try without verifying expiry first
try:
    payload = jwt.decode(TOKEN, SECRET_KEY, algorithms=["HS256"], options={"verify_exp": False})
    print(f"✓ Token decoded successfully")
    print(f"  Payload: {payload}")
    exp = payload.get("exp")
    if exp:
        exp_dt = datetime.fromtimestamp(exp)
        now = datetime.utcnow()
        expired = now.timestamp() > exp
        print(f"  Expires at: {exp_dt} (UTC)")
        print(f"  Current time: {now} (UTC)")
        print(f"  {'✗ EXPIRED' if expired else '✓ Still valid'}")
        if expired:
            print(f"\n  → Fix: increase ACCESS_TOKEN_EXPIRE_MINUTES in auth.py")
            print(f"         or re-login to get a fresh token")
except Exception as e:
    print(f"✗ Could not decode token at all: {e}")
    print(f"\n  → This usually means SECRET_KEY changed after token was issued.")
    print(f"     Check your .env file and make sure JWT_SECRET_KEY is consistent.")
    print(f"     Current SECRET_KEY used: {SECRET_KEY[:10]}...")

# Now try full validation
print()
try:
    jwt.decode(TOKEN, SECRET_KEY, algorithms=["HS256"])
    print("✓ Full validation passed — token is valid!")
except JWTError as e:
    print(f"✗ Full validation failed: {e}")
    print(f"\n  → User needs to log in again to get a fresh token.")

print("=" * 50)
