# MiniCI

Self-hosted CI/CD backend: FastAPI → Redis/Celery → Docker → PostgreSQL, with WebSocket log streaming.

Headless private Actions: queue sandboxed shell steps over REST, watch logs live over WebSocket.

## Architecture

```
Client → FastAPI (202) → Redis queue → Celery worker → Docker steps
                ↓                              ↓
           PostgreSQL                    Redis Pub/Sub → WebSocket
```

## What you can do with it

Assume you’re **Max**, you just cloned this repo.

### 1. Bring the stack up

```bash
docker compose up -d --build
```

Four services: `postgres`, `redis`, `api` (:8000), `worker`.

### 2. Create a project (your namespace for runs)

```bash
curl -X POST http://localhost:8000/projects \
  -H "Content-Type: application/json" \
  -d '{"name": "my-api"}'
# → { "id": "...", "name": "my-api", ... }
```

### 3. Queue a pipeline run (returns immediately)

```bash
curl -X POST http://localhost:8000/projects/{project_id}/runs \
  -H "Content-Type: application/json" \
  -d '{"steps": ["pip install -r requirements.txt", "pytest", "echo ship it"]}'
# → 202 Accepted, status: pending, run_id
```

You don’t block on the work — the job is queued in Redis.

### 4. Watch logs live (no polling)

```bash
# terminal A
npx wscat -c ws://localhost:8000/ws/runs/{run_id}
# or: /tmp/.../wscat if you installed wscat locally

# terminal B (or just wait if run already queued)
curl -X POST http://localhost:8000/projects/{project_id}/runs \
  -H "Content-Type: application/json" \
  -d '{"steps": ["echo step1", "sleep 1", "echo step2"]}'
```

You’ll see:

```json
{"type": "step_start", "step": 0, "command": "echo step1", ...}
{"type": "step_complete", "step": 0, "stdout": "step1\n", ...}
{"type": "step_start", "step": 1, "command": "sleep 1", ...}
{"type": "step_complete", "step": 1, "stdout": "", ...}
{"type": "run_complete", "status": "success", ...}
```

### 5. Check status / history later

```bash
curl http://localhost:8000/runs/{run_id}
# status, started_at/ended_at, each step’s stdout + exit_code

curl http://localhost:8000/projects/{project_id}/runs
# all runs for this project
```

### What runs under the hood (you don’t wire this)

| Concern | Handled by |
|---------|------------|
| Async job queue | Redis + Celery worker |
| Sandboxed execution | Docker: 1 CPU, 512MB, **network disabled** |
| Run history | PostgreSQL |
| Live logs | Redis Pub/Sub → WebSocket |
| Scale-out | Add more workers (`--concurrency`, more replicas) |

### What it doesn’t do (yet)

- No git clone / webhooks — steps are shell strings **you** send
- No UI — curl + WebSocket only
- No auth — anyone who can reach `:8000` can trigger runs
- No YAML workflows, secrets, matrix builds, or artifacts

**One-liner:** point any script/test suite at this API and get queued, isolated, streamable runs — a headless private Actions backend.

## Stack

| Piece | Tech |
|-------|------|
| API | FastAPI + Uvicorn |
| DB | PostgreSQL + SQLAlchemy |
| Queue | Redis + Celery |
| Execution | Docker (isolated, network off, CPU/mem limits) |
| Logs | Redis Pub/Sub + WebSocket |
| Deps | uv |

## Quick start (Docker Compose)

```bash
docker compose up -d --build
```

Starts `postgres`, `redis`, `api` (:8000), `worker`.

## Quick start (local dev)

```bash
# infra
docker compose up -d postgres redis   # or your own pg/redis
# if a standalone redis already holds :6379, stop it first

uv sync

# API
PYTHONPATH=$PWD uv run uvicorn main:app --port 8000

# worker (separate terminal)
PYTHONPATH=$PWD uv run celery -A tasks worker --loglevel=info --concurrency=2
```

## API

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/projects` | Create project |
| GET | `/projects` | List projects |
| GET | `/projects/{id}` | Get project |
| POST | `/projects/{id}/runs` | Queue run (202) |
| GET | `/projects/{id}/runs` | List runs |
| GET | `/runs/{id}` | Run status + steps |
| WS | `/ws/runs/{id}` | Live step logs |

### Example

```bash
curl -X POST http://localhost:8000/projects \
  -H "Content-Type: application/json" \
  -d '{"name": "test"}'

curl -X POST http://localhost:8000/projects/{project_id}/runs \
  -H "Content-Type: application/json" \
  -d '{"steps": ["echo hello", "sleep 1", "echo done"]}'

curl http://localhost:8000/runs/{run_id}
```

WebSocket messages: `step_start`, `step_complete`, `run_complete`.

## Tests

Requires API + worker running:

```bash
uv run pytest tests/ -v
```

## Project layout

```
main.py          # FastAPI app, models, Docker runner, WebSocket
tasks.py         # Celery app + execute_pipeline (pub/sub broadcasts)
tests/
  test_full_flow.py
docker-compose.yml
Dockerfile
```
