"""Loopback-only demo upstreams. Fault controls are confined to this mock service."""

import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from mcp.shared.exceptions import McpError

from gateway_lab.mcp_server import SCHEMAS, execute_tool
from gateway_lab.provider import TokenCounter
from gateway_lab.wire import WireError, error, parse_rpc


@asynccontextmanager
async def lifespan(app):
    app.state.counter = await asyncio.to_thread(TokenCounter)
    yield


app = FastAPI(title="Local mock upstreams", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/mcp")
async def mcp(request: Request):
    try:
        payload = parse_rpc(await request.body())
    except WireError as exc:
        return JSONResponse(error(exc.code, exc.message, exc.request_id))
    request_id = payload.get("id")
    if "id" not in payload:
        return Response(status_code=204)
    method, params = payload["method"], payload.get("params", {})
    if method == "initialize":
        result = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-downstream", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {
            "tools": [
                {"name": name, "inputSchema": schema.model_json_schema()}
                for name, schema in SCHEMAS.items()
            ]
            + [
                {
                    "name": "admin_reset_key",
                    "description": "Simulated admin action",
                    "inputSchema": {"type": "object", "properties": {}},
                }
            ]
        }
    elif method == "tools/call":
        if params.get("name") == "admin_reset_key":
            result = {
                "content": [{"type": "text", "text": "Simulated key reset"}],
                "isError": False,
            }
        else:
            try:
                result = execute_tool(params.get("name"), params.get("arguments", {})).model_dump(
                    by_alias=True,
                    exclude_none=True,
                )
            except McpError as exc:
                return JSONResponse(error(exc.error.code, exc.error.message, request_id))
    elif method == "ping":
        result = {}
    else:
        return JSONResponse(error(-32601, "Method not found", request_id))
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


@app.post("/{provider}/generate")
async def generate(provider: str, request: Request):
    value = await request.json()
    prompt = value["prompt"]
    if provider == "primary":
        if "[primary-429]" in prompt:
            return JSONResponse({"private_debug": "do-not-forward"}, status_code=429)
        if "[primary-timeout]" in prompt:
            await asyncio.sleep(4)
    if "[both-fail]" in prompt:
        return JSONResponse({"private_debug": "do-not-forward"}, status_code=500)
    text = "Hello! Email alice@example.com, SSN 123-45-6789, card 4111 1111 1111 1111. Done."
    counter = app.state.counter
    tokens = counter.encoding.encode(text, disallowed_special=())[: value["max_tokens"]]
    text = counter.encoding.decode(tokens)
    usage = {"prompt_tokens": counter.count(prompt), "completion_tokens": counter.count(text)}
    if not value.get("stream"):
        return {"text": text, "usage": usage}

    async def chunks():
        for i in range(0, len(text), 3):
            yield "data: " + json.dumps({"delta": text[i : i + 3]}) + "\n\n"
            await asyncio.sleep(0.015)
        yield "data: " + json.dumps({"usage": usage}) + "\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(chunks(), media_type="text/event-stream")
