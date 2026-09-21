from typing import *
from datetime import datetime
from enum import Enum
import uuid
from celery.states import SUCCESS
from click import Option
from fastapi import FastAPI, APIRouter, status
from pydantic import BaseModel
import os
from contextlib import asynccontextmanager
from pydantic.mypy import from_attributes_callback
from requests import session
from sqlalchemy import Engine, create_engine, null
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from sqlalchemy import String, DateTime, ForeignKey, Text, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

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

def generate_uuid()->str:
    return str(uuid.uuid4())

class Project(Base):
    __tablename__: str="projects"
    id:Mapped[str]=mapped_column(String(36),primary_key=True,default=generate_uuid)
    name:Mapped[str]=mapped_column(String(255),nullable=False)
    repo_url:Mapped[Optional[str]]=mapped_column(String(500),nullable=True)
    created_at:Mapped[datetime]=mapped_column(DateTime,default=datetime.utcnow)

    runs:Mapped[List["PipelineRun"]]=relationship(back_populates="project")

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=generate_uuid)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id"))
    status: Mapped[RunStatus] = mapped_column(default=RunStatus.PENDING)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    project: Mapped["Project"] = relationship(back_populates="runs")
    steps: Mapped[List["PipelineStep"]] = relationship(back_populates="run")

class PipelineStep(Base):
    __tablename__:str="pipeline_steps"
    id:Mapped[str]=mapped_column(String(36),primary_key=True,default=generate_uuid)
    run_id:Mapped[str]=mapped_column(String(36),ForeignKey("pipeline_runs.id"))
    command:Mapped[str]=mapped_column(Text,nullable=False)
    stdout:Mapped[Optional[str]]=mapped_column(Text,nullable=True)
    stderr:Mapped[Optional[str]]=mapped_column(Text,nullable=True)
    exit_code:Mapped[Optional[int]]=mapped_column(Integer,nullable=True)

    run:Mapped["PipelineRun"]=relationship(back_populates="steps")

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
