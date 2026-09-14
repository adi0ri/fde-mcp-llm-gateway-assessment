"""Real sockets/subprocesses catch streaming buffering that ASGITransport hides."""

import asyncio
import json
import os
import socket
import sys
import time

import httpx
from conftest import API_KEY, SECRET

from gateway_lab.auth import issue_demo_token


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def test_live_gateway_streaming_and_three_second_fallback(tmp_path):
    mock_port, gateway_port = free_port(), free_port()
    while gateway_port == mock_port:
        gateway_port = free_port()
    mock_url = f"http://127.0.0.1:{mock_port}"
    gateway_url = f"http://127.0.0.1:{gateway_port}"
    env = {
        **os.environ,
        "JWT_SECRET": SECRET,
        "TENANT_API_KEYS": json.dumps({API_KEY: "tenant-a"}),
        "DATABASE_PATH": str(tmp_path / "live.sqlite3"),
        "MCP_DOWNSTREAM_URL": mock_url + "/mcp",
        "PRIMARY_URL": mock_url + "/primary/generate",
        "BACKUP_URL": mock_url + "/backup/generate",
    }
    processes, logs = [], []
    try:
        for module, port in (
            ("gateway_lab.mock:app", mock_port),
            ("gateway_lab.app:app", gateway_port),
        ):
            log = (tmp_path / f"server-{port}.log").open("wb")
            logs.append(log)
            processes.append(
                await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "uvicorn",
                    module,
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--log-level",
                    "warning",
                    env=env,
                    stdout=log,
                    stderr=log,
                )
            )
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            for url in (mock_url, gateway_url):
                for _ in range(100):
                    try:
                        if (await client.get(url + "/health")).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("Server failed to start")
            auth = {"Authorization": "Bearer " + issue_demo_token(SECRET, "viewer")}
            blocked = await client.post(
                gateway_url + "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": "live",
                    "method": "tools/call",
                    "params": {"name": "admin_reset_key"},
                },
                headers=auth,
            )
            assert blocked.status_code == 403
            assert blocked.json()["error"]["code"] == -32001
            started, first, completed, text = time.perf_counter(), None, None, ""
            async with client.stream(
                "POST",
                gateway_url + "/v1/stream",
                headers={"X-API-Key": API_KEY},
                json={"prompt": "Hi", "max_tokens": 100},
            ) as response:
                assert response.status_code == 200
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        value = json.loads(line[6:])
                        if "text" in value:
                            first = first or time.perf_counter()
                            text += value["text"]
                        if "usage" in value:
                            completed = time.perf_counter()
            assert text.count("[REDACTED]") == 3
            assert "alice" not in text and "4111" not in text and "123-45" not in text
            assert first is not None and completed is not None
            ttft = first - started
            assert ttft < 1.0
            assert completed - first > 0.15  # Client receives text before the provider finishes.
            started = time.perf_counter()
            fallback = await client.post(
                gateway_url + "/v1/completions",
                json={"prompt": "[primary-timeout]", "max_tokens": 100},
                headers={"X-API-Key": API_KEY},
            )
            elapsed = time.perf_counter() - started
            assert fallback.status_code == 200 and fallback.json()["provider"] == "backup"
            assert 2.9 <= elapsed < 4.0
            limited = await client.post(
                gateway_url + "/v1/completions",
                json={"prompt": "[primary-429]", "max_tokens": 100},
                headers={"X-API-Key": API_KEY},
            )
            assert limited.status_code == 200 and limited.json()["provider"] == "backup"
            # Interrupt a live stream, then prove the gateway remains usable.
            async with client.stream(
                "POST",
                gateway_url + "/v1/stream",
                json={"prompt": "Hi"},
                headers={"X-API-Key": API_KEY},
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        break
            assert (await client.get(gateway_url + "/health")).status_code == 200
            print(f"Live TTFT={ttft:.3f}s; primary timeout fallback={elapsed:.3f}s")
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
        for process in processes:
            await asyncio.wait_for(process.wait(), 10)
        for log in logs:
            log.close()
