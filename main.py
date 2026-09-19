from fastapi import FastAPI, APIRouter, status
from pydantic import BaseModel
import os
from contextlib import asynccontextmanager

class PipelineRun(Base):
    id:str
    status:RunStatus
    started_at:Optional[datetime]
    steps: List[PipelineStep]

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello World"}
