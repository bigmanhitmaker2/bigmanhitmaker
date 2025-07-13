from jose import jwt
from fastapi import HTTPException, Header
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

def verify_jwt(authorization: str = Header(...)):
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=403, detail="Invalid auth scheme")

        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"])
        return payload["sub"]  # user_id
    except Exception as e:
        raise HTTPException(status_code=403, detail="Invalid token")
