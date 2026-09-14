"""One-command demo. Prints temporary LOCAL credentials; never used for stdio."""

import multiprocessing
import os
import secrets
import time

import httpx
import uvicorn

from gateway_lab.auth import issue_demo_token


def serve_mock():
    uvicorn.run("gateway_lab.mock:app", host="127.0.0.1", port=9001, log_level="warning")


def main():
    import json

    secret = secrets.token_urlsafe(48)
    api_key = secrets.token_urlsafe(24)
    os.environ["JWT_SECRET"] = secret
    os.environ["TENANT_API_KEYS"] = json.dumps({api_key: "demo-tenant"})
    worker = multiprocessing.get_context("spawn").Process(target=serve_mock, daemon=True)
    worker.start()
    try:
        for _ in range(100):
            if not worker.is_alive():
                raise RuntimeError("Mock provider failed to start; check port 9001")
            try:
                if httpx.get("http://127.0.0.1:9001/health", timeout=0.2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        else:
            raise RuntimeError("Mock provider startup timed out")
        print("Gateway: http://127.0.0.1:9000 (Ctrl+C stops both services)")
        print("X-API-Key:", api_key)
        for role in ("viewer", "admin"):
            print(f"{role} bearer token:", issue_demo_token(secret, role))
        uvicorn.run("gateway_lab.app:app", host="127.0.0.1", port=9000)
    finally:
        worker.terminate()
        worker.join(timeout=5)


if __name__ == "__main__":
    main()
