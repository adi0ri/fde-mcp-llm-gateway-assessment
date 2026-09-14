"""Run after `python -m gateway_lab.demo`; enter that demo's temporary credentials."""

import argparse
import getpass
import json
import time

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:9000")
    args = parser.parse_args()
    api_key = getpass.getpass("Demo X-API-Key: ")
    viewer = getpass.getpass("Demo viewer bearer token: ")
    admin = getpass.getpass("Demo admin bearer token: ")
    with httpx.Client(base_url=args.url, timeout=10, trust_env=False) as client:
        listing = client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={"Authorization": "Bearer " + viewer},
        )
        listing.raise_for_status()
        print(
            "Available tools:",
            ", ".join(tool["name"] for tool in listing.json()["result"]["tools"]),
        )
        for role, token, status in (("viewer", viewer, 403), ("admin", admin, 200)):
            response = client.post(
                "/mcp",
                headers={"Authorization": "Bearer " + token},
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "admin_reset_key", "arguments": {}},
                },
            )
            assert response.status_code == status, response.text
            print(f"Admin tool called by {role}: HTTP {response.status_code}")
        print("Redacted stream: ", end="", flush=True)
        started, first = time.perf_counter(), None
        with client.stream(
            "POST",
            "/v1/stream",
            headers={"X-API-Key": api_key},
            json={"prompt": "Hi", "max_tokens": 100},
        ) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line.startswith("data: "):
                    value = json.loads(line[6:])
                    if "text" in value:
                        first = first or time.perf_counter()
                        print(value["text"], end="", flush=True)
                    if "error" in value:
                        raise RuntimeError(value["error"]["code"])
        print(f"\nTime to first text: {first - started:.3f}s")
        for prompt in ("[primary-429]", "[primary-timeout]"):
            started = time.perf_counter()
            response = client.post(
                "/v1/completions",
                headers={"X-API-Key": api_key},
                json={"prompt": prompt, "max_tokens": 100},
            )
            response.raise_for_status()
            assert response.json()["provider"] == "backup"
            print(f"{prompt}: backup succeeded after {time.perf_counter() - started:.3f}s")
    print("All HTTP demo checks passed.")


if __name__ == "__main__":
    main()
