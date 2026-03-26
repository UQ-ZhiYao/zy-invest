"""
Auth router  v1.0.0
POST /api/auth/login
POST /api/auth/logout
POST /api/auth/change-password
GET  /api/auth/me
"""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta, timezone
from typing import Optional
import bcrypt
import jwt
import os

from database import Database, get_db

router = APIRouter()

JWT_SECRET   = os.getenv("JWT_SECRET", "change-this-secret-in-production")
JWT_ALGO     = "HS256"
JWT_EXPIRE_H = 24  # hours


# ── Schemas ───────────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: str
    password: str

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    name: str
    investor_id: Optional[str]


# ── Helpers ───────────────────────────────────────────────────
def create_jwt(user_id: str, investor_id: Optional[str], role: str) -> str:
    payload = {
        "sub":         user_id,
        "investor_id": investor_id,
        "role":        role,
        "exp":         datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_H),
        "iat":         datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def decode_jwt(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def get_token_from_header(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization header")
    return auth[7:]


async def get_current_user(request: Request, db: Database = Depends(get_db)) -> dict:
    token  = get_token_from_header(request)
    claims = decode_jwt(token)
    user   = await db.fetchrow(
        "SELECT id, name, email, role, investor_id, is_active FROM users WHERE id = $1",
        claims["sub"]
    )
    if not user or not user["is_active"]:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return dict(user)


async def require_admin(request: Request, db: Database = Depends(get_db)) -> dict:
    user = await get_current_user(request, db)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# ── Routes ────────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, request: Request, db: Database = Depends(get_db)):
    user = await db.fetchrow(
        "SELECT id, name, email, password_hash, role, investor_id, is_active "
        "FROM users WHERE email = $1",
        body.email.lower().strip()
    )
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not user["is_active"]:
        raise HTTPException(status_code=403, detail="Account suspended")
    if not bcrypt.checkpw(body.password.encode(), user["password_hash"].encode()):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_jwt(
        str(user["id"]),
        str(user["investor_id"]) if user["investor_id"] else None,
        user["role"]
    )

    # Log the login
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name, ip_address) VALUES ($1,$2,$3,$4)",
        str(user["id"]), "LOGIN", "users", request.client.host
    )

    return TokenResponse(
        access_token=token,
        role=user["role"],
        name=user["name"],
        investor_id=str(user["investor_id"]) if user["investor_id"] else None,
    )


@router.get("/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    return {
        "id":          str(current_user["id"]),
        "name":        current_user["name"],
        "email":       current_user["email"],
        "role":        current_user["role"],
        "investor_id": str(current_user["investor_id"]) if current_user["investor_id"] else None,
    }


@router.post("/change-password")
async def change_password(
    body: ChangePasswordRequest,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    user = await db.fetchrow(
        "SELECT password_hash FROM users WHERE id = $1", str(current_user["id"])
    )
    if not bcrypt.checkpw(body.current_password.encode(), user["password_hash"].encode()):
        raise HTTPException(status_code=400, detail="Current password incorrect")

    if len(body.new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    new_hash = bcrypt.hashpw(body.new_password.encode(), bcrypt.gensalt()).decode()
    await db.execute(
        "UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2",
        new_hash, str(current_user["id"])
    )
    return {"message": "Password updated successfully"}


@router.post("/logout")
async def logout(current_user: dict = Depends(get_current_user), db: Database = Depends(get_db)):
    # JWT is stateless — client deletes the token
    # Log the logout for audit trail
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name) VALUES ($1,$2,$3)",
        str(current_user["id"]), "LOGOUT", "users"
    )
    return {"message": "Logged out successfully"}
