import json

import httpx
import jwt
import pytest
from conftest import SECRET, gateway

from gateway_lab.auth import issue_demo_token


def rpc(name="admin_reset_key", request_id=7):
    return {"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {"name": name}}


async def test_viewer_blocked_without_downstream_call_admin_allowed(settings):
    calls = []

    def downstream(request):
        calls.append(request)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 7, "result": {}})

    async with gateway(settings, downstream) as (client, _):
        for role, expected in (("viewer", 403), ("admin", 200)):
            response = await client.post(
                "/mcp",
                json=rpc(),
                headers={
                    "Authorization": "Bearer " + issue_demo_token(SECRET, role),
                },
            )
            assert response.status_code == expected
            if role == "viewer":
                assert response.json()["error"] == {
                    "code": -32001,
                    "message": "Unauthorized Tool Call",
                }
                assert calls == []
        assert len(calls) == 1
        assert "authorization" not in calls[0].headers


async def test_list_and_non_admin_tool_are_transparent(settings):
    seen = []
    returned = b'{ "jsonrpc": "2.0", "id": "list", "result": {"tools": []} }'

    def downstream(request):
        seen.append(request)
        return httpx.Response(
            200,
            content=returned,
            headers={
                "content-type": "application/json",
                "mcp-session-id": "session-demo",
            },
        )

    async with gateway(settings, downstream) as (client, _):
        headers = {
            "Authorization": "Bearer " + issue_demo_token(SECRET, "viewer"),
            "Content-Type": "application/json",
            "MCP-Protocol-Version": "2025-11-25",
        }
        raw = b'{ "jsonrpc": "2.0", "id": "list", "method": "tools/list", "params": {} }'
        response = await client.post("/mcp", content=raw, headers=headers)
        assert response.content == returned
        assert response.headers["mcp-session-id"] == "session-demo"
        assert seen[0].content == raw
        assert seen[0].headers["mcp-protocol-version"] == "2025-11-25"
        await client.post("/mcp", json=rpc("get_customer_record"), headers=headers)
        assert len(seen) == 2


@pytest.mark.parametrize(
    "token",
    [
        None,
        "viewer",
        issue_demo_token(SECRET, "admin", -1),
        issue_demo_token("different-secret-with-at-least-32-bytes", "admin"),
        issue_demo_token(SECRET, "superuser"),
        jwt.encode({"role": "admin"}, "", algorithm="none"),
    ],
)
async def test_bad_auth_never_reaches_upstream(settings, token):
    def downstream(_):
        pytest.fail("Authentication bypass")

    async with gateway(settings, downstream) as (client, _):
        headers = {"Authorization": "Bearer " + token} if token else {}
        response = await client.post("/mcp", json=rpc(), headers=headers)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"


async def test_batch_duplicate_keys_notification_and_invalid_params(settings):
    def downstream(_):
        pytest.fail("Rejected request reached upstream")

    async with gateway(settings, downstream) as (client, _):
        headers = {
            "Authorization": "Bearer " + issue_demo_token(SECRET, "viewer"),
            "Content-Type": "application/json",
        }
        notification = rpc()
        del notification["id"]
        response = await client.post("/mcp", json=notification, headers=headers)
        assert response.status_code == 403 and response.content == b""
        for raw, code in [
            (json.dumps([rpc()]), -32600),
            ('{"jsonrpc":"2.0","id":1,"method":"tools/list","method":"tools/call"}', -32700),
            (json.dumps({**rpc(), "params": {"name": 123}}), -32602),
            ("{", -32700),
        ]:
            response = await client.post("/mcp", content=raw, headers=headers)
            assert response.json()["error"]["code"] == code


async def test_upstream_failure_is_sanitized(settings):
    def downstream(_):
        raise httpx.ConnectError("secret=private-credential; internal.host")

    async with gateway(settings, downstream) as (client, _):
        response = await client.post(
            "/mcp",
            json=rpc("get_customer_record"),
            headers={
                "Authorization": "Bearer " + issue_demo_token(SECRET, "viewer"),
            },
        )
        assert response.status_code == 502
        assert response.json()["error"]["code"] == -32603
        assert "private" not in response.text
