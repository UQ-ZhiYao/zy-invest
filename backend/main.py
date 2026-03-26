"""
ZY-Invest Backend API  v1.0.0
FastAPI + Supabase PostgreSQL
Deploy to: Render.com (free tier)
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os

from routers import auth, member, admin, public
from database import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("ZY-Invest API starting up...")
    yield
    print("ZY-Invest API shutting down...")


app = FastAPI(
    title="ZY-Invest API",
    version="1.0.0",
    description="Private fund portal backend",
    lifespan=lifespan,
    docs_url="/docs" if os.getenv("ENV") != "production" else None,
    redoc_url=None,
)

# ── CORS ─────────────────────────────────────────────────────
# Allow GitHub Pages domain + localhost for development
ALLOWED_ORIGINS = [
    os.getenv("FRONTEND_URL", "http://localhost:3000"),
    "https://*.github.io",          # GitHub Pages wildcard
    "http://127.0.0.1:5500",        # VS Code Live Server
    "http://localhost:5500",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["Authorization", "Content-Type"],
)

# ── Routers ───────────────────────────────────────────────────
app.include_router(public.router,  prefix="/api/public",  tags=["Public"])
app.include_router(auth.router,    prefix="/api/auth",    tags=["Auth"])
app.include_router(member.router,  prefix="/api/member",  tags=["Member"])
app.include_router(admin.router,   prefix="/api/admin",   tags=["Admin"])


@app.get("/")
async def root():
    return {"status": "ok", "service": "ZY-Invest API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
