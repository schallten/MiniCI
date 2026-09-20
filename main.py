from typing import *
from datetime import datetime
from enum import Enum
from celery.states import SUCCESS
from fastapi import FastAPI, APIRouter, status
from pydantic import BaseModel
import os
from contextlib import asynccontextmanager
from pydantic.mypy import from_attributes_callback
from requests import session
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker, declarative_base

DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/myapp"

engine: Engine = create_engine(DATABASE_URL)
SessionLocal: sessionmaker[Session] = sessionmaker(autocommit=False,autoflush=False,bind=engine)
Base = declarative_base()

def get_db() -> Generator[Session, Any, None]:
    db: Session=SessionLocal()
    try:
        yield db
    finally:
        db.close()

class RunStatus(str,Enum):
    PENDING="pending"
    RUNNING="running"
    SUCCESS="success"
    FAILED="failed"

class ProjectCreate(BaseModel):
    name:str
    repo_url:Optional[str]=None

class RunRequest(BaseModel):
    steps:List[str]
    timeout:int=300

#response schema
class ProjectResponse(BaseModel):
    id:str
    name:str
    repo_url:Optional[str]
    created_at:datetime

    class Config:
        from_attributes = True

class RunResponse(BaseModel):
    id:str
    project_id:str
    status:RunStatus
    started_at:Optional[datetime]
    ended_at: Optional[datetime]
    steps:List[StepResponse]

    class Config:
        from_attributes=True

class PipelineRun(Base):
    id:str
    status:RunStatus
    started_at:Optional[datetime]
    steps: List[PipelineStep]

app = FastAPI()

@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "Hello World"}
