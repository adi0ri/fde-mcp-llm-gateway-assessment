import asyncio
import json
import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.shared.exceptions import McpError


@pytest.fixture
async def wire():
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "gateway_lab.mcp_server",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def send(value, response=True):
        line = value if isinstance(value, str) else json.dumps(value)
        process.stdin.write((line + "\n").encode())
        await process.stdin.drain()
        if response:
            raw = await asyncio.wait_for(process.stdout.readline(), timeout=10)
            parsed = json.loads(raw)  # Every stdout line must be a JSON-RPC envelope.
            assert parsed["jsonrpc"] == "2.0"
            assert "result" in parsed or "error" in parsed
            return parsed

    await send(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "wire-test", "version": "1"},
            },
        }
    )
    await send({"jsonrpc": "2.0", "method": "notifications/initialized"}, response=False)
    yield send
    process.stdin.close()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.terminate()
        await process.wait()
    # No trailing accidental prints; all diagnostics stayed on stderr.
    assert await process.stdout.read() == b""
    assert b"Customer MCP server starting" in await process.stderr.read()


async def test_official_client_handshake_and_both_tools():
    params = StdioServerParameters(command=sys.executable, args=["-m", "gateway_lab.mcp_server"])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as client:
            await client.initialize()
            listing = await client.list_tools()
            assert {tool.name for tool in listing.tools} == {
                "get_customer_record",
                "trigger_refund",
            }
            assert all(tool.inputSchema["additionalProperties"] is False for tool in listing.tools)
            customer = await client.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
            assert customer.structuredContent["customer_id"] == "CUST-00001"
            refund = await client.call_tool(
                "trigger_refund",
                {
                    "customer_id": "CUST-00001",
                    "amount": 12.5,
                    "reason": "Duplicate charge",
                },
            )
            assert refund.structuredContent["status"] == "simulated"
            missing = await client.call_tool("get_customer_record", {"customer_id": "CUST-99999"})
            assert missing.isError
            with pytest.raises(McpError) as caught:
                await client.call_tool("get_customer_record", {"customer_id": "bad"})
            assert caught.value.error.code == -32602


async def test_invalid_fields_return_protocol_errors_and_server_recovers(wire):
    valid = {"customer_id": "CUST-00001", "amount": 12.5, "reason": "Duplicate charge"}
    invalid = (
        [
            {"customer_id": value}
            for value in (
                "cust-00001",
                "CUST-1234",
                "CUST-123456",
                "CUST-１２３４５",
                "CUST-00001\n",
                1,
                None,
            )
        ]
        + [{"amount": value} for value in (0, -1, True, "2.5", None)]
        + [{"reason": value} for value in ("short", "          ", 123, None, "a        ")]
        + [{"extra": "rejected"}]
    )
    for i, change in enumerate(invalid, 10):
        result = await wire(
            {
                "jsonrpc": "2.0",
                "id": i,
                "method": "tools/call",
                "params": {
                    "name": "trigger_refund",
                    "arguments": {**valid, **change},
                },
            }
        )
        assert result["id"] == i
        assert result["error"]["code"] == -32602
        assert "input" not in str(result["error"].get("data"))
    for arguments in ({}, {"amount": 1}, {**valid, "amount": 1e309}):
        result = await wire(
            {
                "jsonrpc": "2.0",
                "id": 60,
                "method": "tools/call",
                "params": {
                    "name": "trigger_refund",
                    "arguments": arguments,
                },
            }
        )
        assert result["error"]["code"] in (-32700, -32602)
    result = await wire({"jsonrpc": "2.0", "id": "alive", "method": "tools/list"})
    assert len(result["result"]["tools"]) == 2


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        ("{", -32700),
        ("[]", -32600),
        ('{"jsonrpc":"2.0","id":2,"method":"ping","method":"tools/list"}', -32700),
        ({"jsonrpc": "1.0", "id": 2, "method": "tools/list"}, -32600),
        ({"jsonrpc": "2.0", "id": True, "method": "tools/list"}, -32600),
        ({"jsonrpc": "2.0", "id": 2, "method": "unknown/method"}, -32601),
        ({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {}}, -32602),
        ({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": []}, -32602),
        (
            {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "missing"}},
            -32602,
        ),
    ],
)
async def test_wire_errors(wire, payload, code):
    assert (await wire(payload))["error"]["code"] == code


async def test_oversized_line_is_drained_and_next_request_succeeds(wire):
    response = await wire("x" * (1024 * 1024 + 1))
    assert response["error"]["code"] == -32600
    response = await wire({"jsonrpc": "2.0", "id": "after-large-line", "method": "tools/list"})
    assert response["id"] == "after-large-line" and len(response["result"]["tools"]) == 2
