from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os

from routers import auth, member, admin, public
from database import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect with retry — don't crash if DB is slow to connect
    try:
        await engine.connect()
        print("Database connected successfully")
    except Exception as e:
        print(f"Database connection warning: {e}")
        print("Will retry on first request...")
    yield
    try:
        await engine.disconnect()
    except Exception:
        pass


app = FastAPI(
    title="ZY-Invest API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(public.router,  prefix="/api/public",  tags=["Public"])
app.include_router(auth.router,    prefix="/api/auth",    tags=["Auth"])
app.include_router(member.router,  prefix="/api/member",  tags=["Member"])
app.include_router(admin.router,   prefix="/api/admin",   tags=["Admin"])


@app.get("/")
@app.head("/")
async def root():
    return {"status": "ok", "service": "ZY-Invest API", "version": "1.0.0"}


@app.head("/health")
@app.get("/health-check")
async def health_head():
    return {"status": "ok"}


@app.get("/health")
@app.head("/health")
async def health():
    return {"status": "healthy", "service": "ZY-Invest API"}
