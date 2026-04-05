"""
Auth router v2.1 — individual & joint account support
POST /api/auth/login
POST /api/auth/logout
POST /api/auth/change-password
GET  /api/auth/me
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from typing import Optional
import bcrypt, jwt, os

from database import Database, get_db

router = APIRouter()

JWT_SECRET   = os.getenv("JWT_SECRET", "change-this-secret-in-production")
JWT_ALGO     = "HS256"
JWT_EXPIRE_H = 24


# ── Schemas ───────────────────────────────────────────────────
class LoginRequest(BaseModel):
    email:    str
    password: str

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password:     str

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    role:         str
    name:         str
    investor_id:  Optional[str]
    holder_role:  Optional[str]   # primary | secondary


# ── JWT helpers ───────────────────────────────────────────────
def create_jwt(user_id: str, investor_id: Optional[str],
               role: str, holder_role: Optional[str] = None) -> str:
    payload = {
        "sub":         user_id,
        "investor_id": investor_id,
        "role":        role,
        "holder_role": holder_role,
        "exp":         datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_H),
        "iat":         datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def decode_jwt(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")


def get_token_from_header(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing authorization header")
    return auth[7:]


async def _resolve_investor(user_id: str, direct_investor_id, db: Database):
    """
    Resolve investor_id and holder_role for a user.
    1. users.investor_id (primary link, kept for backward compat)
    2. investor_holders junction table (joint account secondary holders)
    Returns (investor_id_str, holder_role_str)
    """
    if direct_investor_id:
        return str(direct_investor_id), "primary"

    row = await db.fetchrow("""
        SELECT investor_id, role FROM investor_holders
        WHERE user_id = $1
        ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END, created_at ASC
        LIMIT 1
    """, user_id)
    if row:
        return str(row["investor_id"]), row["role"]
    return None, None


async def get_current_user(request: Request,
                           db: Database = Depends(get_db)) -> dict:
    token  = get_token_from_header(request)
    claims = decode_jwt(token)
    user   = await db.fetchrow(
        "SELECT id, name, email, role, investor_id, is_active FROM users WHERE id=$1",
        claims["sub"])
    if not user or not user["is_active"]:
        raise HTTPException(401, "User not found or inactive")

    u = dict(user)
    inv_id, holder_role = await _resolve_investor(
        str(u["id"]), u.get("investor_id"), db)
    u["investor_id"]  = inv_id
    u["holder_role"]  = holder_role
    return u


async def require_admin(request: Request,
                        db: Database = Depends(get_db)) -> dict:
    user = await get_current_user(request, db)
    if user["role"] != "admin":
        raise HTTPException(403, "Admin access required")
    return user


# ── Routes ────────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, request: Request,
                db: Database = Depends(get_db)):
    user = await db.fetchrow(
        "SELECT id, name, email, password_hash, role, investor_id, is_active "
        "FROM users WHERE email = $1",
        body.email.lower().strip())
    if not user:
        raise HTTPException(401, "Invalid credentials")
    if not user["is_active"]:
        raise HTTPException(403, "Account suspended")
    if not bcrypt.checkpw(body.password.encode(), user["password_hash"].encode()):
        raise HTTPException(401, "Invalid credentials")

    inv_id, holder_role = await _resolve_investor(
        str(user["id"]), user["investor_id"], db)

    token = create_jwt(str(user["id"]), inv_id, user["role"], holder_role)

    await db.execute(
        "INSERT INTO audit_log (user_id,action,table_name,ip_address) "
        "VALUES ($1,$2,$3,$4)",
        str(user["id"]), "LOGIN", "users", request.client.host)

    return TokenResponse(
        access_token=token,
        role=user["role"],
        name=user["name"],
        investor_id=inv_id,
        holder_role=holder_role,
    )


@router.get("/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    return {
        "id":          str(current_user["id"]),
        "name":        current_user["name"],
        "email":       current_user["email"],
        "role":        current_user["role"],
        "investor_id": current_user.get("investor_id"),
        "holder_role": current_user.get("holder_role"),
    }


@router.post("/change-password")
async def change_password(
    body: ChangePasswordRequest,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    user = await db.fetchrow(
        "SELECT password_hash FROM users WHERE id=$1", str(current_user["id"]))
    if not bcrypt.checkpw(body.current_password.encode(),
                          user["password_hash"].encode()):
        raise HTTPException(400, "Current password incorrect")
    if len(body.new_password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    new_hash = bcrypt.hashpw(
        body.new_password.encode(), bcrypt.gensalt()).decode()
    await db.execute(
        "UPDATE users SET password_hash=$1, updated_at=NOW() WHERE id=$2",
        new_hash, str(current_user["id"]))
    return {"message": "Password updated successfully"}


@router.post("/logout")
async def logout(current_user: dict = Depends(get_current_user),
                 db: Database = Depends(get_db)):
    await db.execute(
        "INSERT INTO audit_log (user_id,action,table_name) VALUES ($1,$2,$3)",
        str(current_user["id"]), "LOGOUT", "users")
    return {"message": "Logged out successfully"}
