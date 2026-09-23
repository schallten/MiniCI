from typing import *
from datetime import datetime
from enum import Enum
from typing import Any, Generator, List, Optional
from uuid import uuid4
import uuid
from docker.models.containers import Container
WaitContainerResponse = Dict[str, Any]
from fastapi import FastAPI, APIRouter, HTTPException, Depends, status, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import os
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from sqlalchemy import String, DateTime, ForeignKey, Text, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
import docker
from docker.errors import ContainerError,ImageNotFound
from typing import Tuple,List,Dict,Set
import asyncio

from tasks import execute_pipeline, redis_client

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://user:password@localhost:5432/myapp",
)

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

class RunStatus(str,Enum):
    PENDING="pending"
    RUNNING="running"
    SUCCESS="success"
    FAILED="failed"

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
    id:Mapped[str]=mapped_column(String(50),primary_key=True,default=generate_uuid)
    run_id:Mapped[str]=mapped_column(String(36),ForeignKey("pipeline_runs.id"))
    command:Mapped[str]=mapped_column(Text,nullable=False)
    stdout:Mapped[Optional[str]]=mapped_column(Text,nullable=True)
    stderr:Mapped[Optional[str]]=mapped_column(Text,nullable=True)
    exit_code:Mapped[Optional[int]]=mapped_column(Integer,nullable=True)

    run:Mapped["PipelineRun"]=relationship(back_populates="steps")

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

class StepResponse(BaseModel):
    id: str
    command: str
    stdout: Optional[str]
    stderr: Optional[str]
    exit_code: Optional[int]

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

Base.metadata.create_all(bind=engine)

app = FastAPI(title="MiniCI API",version="0.0.1")

@app.post("/projects", response_model=ProjectResponse, status_code=201)
def create_project(project: ProjectCreate, db: Session = Depends(get_db)):
    db_project = Project(
        id=str(uuid4()),
        name=project.name,
        repo_url=project.repo_url
    )
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    return db_project

@app.get("/projects/{project_id}",response_model=ProjectResponse)
def get_project(project_id:str,db:Session=Depends(get_db)) -> Project:
    project: Project | None=db.query(Project).filter(Project.id==project_id).first()
    if not project:
        raise HTTPException(status_code=404,detail="Project not found")
    return project

@app.get("/projects",response_model=List[ProjectResponse])
def list_projects(db:Session=Depends(get_db)) -> List[Project]:
    return db.query(Project).all()

# run endpoints
@app.post(
    "/projects/{project_id}/runs",
    response_model=RunResponse,
    status_code=status.HTTP_202_ACCEPTED
)
def create_run(
    project_id: str,
    run_request: RunRequest,
    db: Session = Depends(get_db)
) -> PipelineRun:
    # Verify project exists
    project = db.query(Project).filter(Project.id == project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Create run
    run_id = str(uuid4())
    run = PipelineRun(
        id=run_id,
        project_id=project_id,
        status="pending"
    )
    db.add(run)
    
    # Create steps
    for i, command in enumerate(run_request.steps):
        step = PipelineStep(
            id=f"{run_id}-step-{i}",
            run_id=run_id,
            command=command
        )
        db.add(step)
    
    db.commit()
    db.refresh(run)
    
    execute_pipeline.delay(run_id, run_request.steps)
    
    return run

@app.get("/runs/{run_id}",response_model=RunResponse)
def get_run(run_id:str,db:Session=Depends(get_db)) -> PipelineRun:
    run: PipelineRun | None=db.query(PipelineRun).filter(PipelineRun.id==run_id).first()
    if not run:
        raise HTTPException(status_code=404,detail="Run not found")
    return run

@app.get("/projects/{project_id}/runs",response_model=List[RunResponse])
def list_runs(project_id:str,db:Session=Depends(get_db)) -> List[PipelineRun]:
    return db.query(PipelineRun).filter(PipelineRun.project_id==project_id).all()


class DockerRunner:
    """Execute pipeline steps in isolated docker containers"""
    def __init__(self)->None:
        self.client: docker.DockerClient = docker.from_env()
        self.image:str = "python:3.11-slim"

    def _volume_name(self, run_id: str) -> str:
        return f"minici-ws-{run_id}"

    def cleanup_workspace(self, run_id: str) -> None:
        """Remove shared workspace volume for a run."""
        try:
            vol = self.client.volumes.get(self._volume_name(run_id))
            vol.remove(force=True)
        except Exception:
            pass

    def run_steps(
            self,
            steps:List[str],
            run_id:str,
            timeout:int=300
    )->Tuple[bool,str]:
        """execute steps in docker container (shared workspace per run)"""
        combined_output = []
        volume = self._volume_name(run_id)
        for i,step in enumerate(steps):
            try:
                container: Container=self.client.containers.run(
                    image=self.image,
                    command=["/bin/bash", "-c", step],
                    detach=True,
                    # resource limits
                    nano_cpus=1_000_000_000, #1 cpu core
                    mem_limit="512m",
                    network_disabled=True, # no network accesss
                    remove=False,
                    working_dir="/workspace",
                    # shared across steps of the same run (CI-style workspace)
                    volumes={volume: {"bind": "/workspace", "mode": "rw"}},
                )

                # waith with timeout
                result: WaitContainerResponse = container.wait(timeout=timeout)
                stdout: str=container.logs().decode("utf-8")

                #cleanup
                container.remove(force=True)

                if result["StatusCode"]!=0:
                    return False,f"Step {i+1} failed: {stdout}"

                combined_output.append(stdout)

            except Exception as e:
                return False, f"Step {i+1} error: {str(e)}"

        return True, "\n".join(combined_output)

class ConnectionManager:
    """manage websocket connections for run logs"""
    def __init__(self) -> None:
        self.active_connections:Dict[str,Set[WebSocket]]={}

    async def connect(self,websocket:WebSocket,run_id:str)-> None:
        await websocket.accept()
        if run_id not in self.active_connections:
            self.active_connections[run_id]=set()
        self.active_connections[run_id].add(websocket)

    def disconnect(self,websocket:WebSocket,run_id:str)->None:
        if run_id in self.active_connections:
            self.active_connections[run_id].discard(websocket)

    async def broadcast(self,run_id:str,message:str)->None:
        if run_id in self.active_connections:
            for connection in list(self.active_connections[run_id]):
                await connection.send_text(message)

manager = ConnectionManager()

@app.websocket("/ws/runs/{run_id}")
async def websocket_logs(websocket:WebSocket,run_id:str)->None:
    """stream logs for specific run"""
    await manager.connect(websocket,run_id)

    pubsub = redis_client.pubsub()
    await asyncio.to_thread(pubsub.subscribe, f"logs:{run_id}")

    try:
        while True:
            message = await asyncio.to_thread(pubsub.get_message, timeout=1.0)
            if message and message["type"]=="message":
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)

    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket,run_id)
        await asyncio.to_thread(pubsub.unsubscribe, f"logs:{run_id}")
        pubsub.close()

@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "Hello World"}
