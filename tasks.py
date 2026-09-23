from celery import Celery
from datetime import datetime
from typing import Dict, Any, List
import json
import redis

celery_app = Celery(
    "minici",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

redis_client = redis.Redis(host="localhost", port=6379, db=0)


@celery_app.task(bind=True, name="execute_pipeline")
def execute_pipeline(
    self,
    run_id: str,
    steps: List[str]
) -> Dict[str, Any]:
    """execute pipeline in background worker"""
    from main import SessionLocal, PipelineRun, PipelineStep, RunStatus, DockerRunner

    db = SessionLocal()
    run = None
    runner = DockerRunner()

    try:
        run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
        if run is None:
            return {"status": "error", "message": "run not found"}

        run.status = RunStatus.RUNNING
        run.started_at = datetime.utcnow()
        db.commit()

        for i, command in enumerate(steps):
            step = db.query(PipelineStep).filter(
                PipelineStep.id == f"{run_id}-step-{i}"
            ).first()
            if step is None:
                step = PipelineStep(
                    id=f"{run_id}-step-{i}",
                    run_id=run_id,
                    command=command,
                )
                db.add(step)
                db.commit()

            redis_client.publish(
                f"logs:{run_id}",
                json.dumps({
                    "type": "step_start",
                    "step": i,
                    "command": command,
                    "timestamp": datetime.utcnow().isoformat()
                })
            )

            success, output = runner.run_steps(
                steps=[command],
                run_id=run_id
            )

            step.stdout = output
            step.exit_code = 0 if success else 1
            db.commit()

            redis_client.publish(
                f"logs:{run_id}",
                json.dumps({
                    "type": "step_complete",
                    "step": i,
                    "stdout": output,
                    "exit_code": 0 if success else 1,
                    "timestamp": datetime.utcnow().isoformat()
                })
            )
            if not success:
                run.status = RunStatus.FAILED
                run.ended_at = datetime.utcnow()
                db.commit()
                return {"status": "failed", "step": i}

        run.status = RunStatus.SUCCESS
        run.ended_at = datetime.utcnow()
        db.commit()

        redis_client.publish(
            f"logs:{run_id}",
            json.dumps({
                "type": "run_complete",
                "status": run.status.value,
                "timestamp": datetime.utcnow().isoformat()
            })
        )

        return {"status": "success"}

    except Exception as e:
        if run is not None:
            run.status = RunStatus.FAILED
            run.ended_at = datetime.utcnow()
            db.commit()
        return {"status": "error", "message": str(e)}
    finally:
        db.close()
