import asyncio
import json
import httpx
import websockets

async def test_full_pipeline_flow():
    async with httpx.AsyncClient() as client:
        # 1. Create project
        project = await client.post(
            "http://localhost:8000/projects",
            json={"name": "test-project"}
        )
        project_id = project.json()["id"]
        
        # 2. Create run
        run = await client.post(
            f"http://localhost:8000/projects/{project_id}/runs",
            json={"steps": ["echo hello", "sleep 1", "echo done"]}
        )
        run_id = run.json()["id"]
        assert run.json()["status"] == "pending"
        
        # 3. Connect WebSocket
        async with websockets.connect(
            f"ws://localhost:8000/ws/runs/{run_id}"
        ) as ws:
            messages = []
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=15.0)
                    data = json.loads(msg)
                    messages.append(data)
                    if data["type"] == "run_complete":
                        break
                except asyncio.TimeoutError:
                    break
        
        # 4. Verify
        assert len(messages) > 0
        assert any(m["type"] == "step_start" for m in messages)
        assert any(m["type"] == "run_complete" for m in messages)
        
        # 5. Check final status
        final_run = await client.get(f"http://localhost:8000/runs/{run_id}")
        assert final_run.json()["status"] == "success"
