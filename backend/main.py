from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os

from routers import auth, member, admin, public
from database import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.connect()
    yield
    await engine.disconnect()


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
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["Authorization", "Content-Type"],
)

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
