# MCP & LLM Gateway Assessment

Four runnable engineering tasks: an official-SDK MCP server, an authenticated MCP proxy,
a streaming PII guardrail, and a token-aware SQLite model router. All examples use local
mock data and providers; no paid API account is required. Refunds are simulated.

## Quick start

Requires Python **3.11+**. Run these commands from the repository root.

```sh
python -m venv .venv
```

Activate the environment:

```sh
# macOS / Linux
source .venv/bin/activate
```

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

Install and start the demo:

```sh
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps
python -m gateway_lab.demo
```

The demo starts the gateway at `http://127.0.0.1:9000` and mock upstreams at
`http://127.0.0.1:9001`. It prints an API key and one-hour viewer/admin bearer tokens.
Keep that terminal running. **Ctrl+C stops both services.** Token accounting persists
in `data/gateway.sqlite3`; demo credentials are generated afresh on every launch.

In another activated terminal, exercise all HTTP endpoints:

```sh
python scripts/exercise_gateway.py
```

The script securely prompts for the three demo credentials, checks viewer denial and
admin access, requests a redacted stream, and demonstrates both fallback triggers.

## Task map

| Task | Implementation | How to run / verify |
|---|---|---|
| **1. MCP server** | [`mcp_server.py`](src/gateway_lab/mcp_server.py) | `python -m gateway_lab.mcp_server`; official-client and raw stdio tests |
| **2. MCP security gateway** | [`app.py`](src/gateway_lab/app.py), [`auth.py`](src/gateway_lab/auth.py) | `POST /mcp`; JWT role check before forwarding |
| **3. Streaming PII guardrail** | [`redaction.py`](src/gateway_lab/redaction.py), [`provider.py`](src/gateway_lab/provider.py) | `POST /v1/stream`; bounded SSE and candidate buffers |
| **4. Rate limit and fallback** | [`limiter.py`](src/gateway_lab/limiter.py), [`provider.py`](src/gateway_lab/provider.py) | `POST /v1/completions`; on-disk SQLite, 50,000 tokens/minute/key, 3-second primary deadline |

## Task 1: official MCP stdio server

Example MCP client configuration after installation (use the absolute path to your
virtual environment's Python executable when the client does not inherit it):

```json
{
  "mcpServers": {
    "customer-records": {
      "command": "python",
      "args": ["-m", "gateway_lab.mcp_server"]
    }
  }
}
```

Use normal MCP initialization before invoking tools. The server advertises:

```json
{"name":"get_customer_record","arguments":{"customer_id":"CUST-00001"}}
```

```json
{"name":"trigger_refund","arguments":{"customer_id":"CUST-00001","amount":12.5,"reason":"Duplicate charge"}}
```

- IDs use exactly five **ASCII digits** following `CUST-`.
- Amounts must be finite positive JSON numbers. Numeric strings and booleans are rejected;
  a JSON integer such as `12` is a valid number and becomes `12.0`.
- Reasons need at least 10 characters after trimming; extra fields are rejected.
- Invalid arguments and unknown tools return JSON-RPC **`-32602`**. Invalid JSON returns
  `-32700`, invalid envelopes `-32600`, and unknown methods `-32601`.
- A valid but nonexistent customer returns a tool result with `isError: true`.
- All server diagnostics go to **stderr**. stdout carries only newline-delimited JSON-RPC.

The assessment explicitly asks for JSON-RPC validation errors. This implementation follows
that requirement; newer MCP guidance normally recommends `isError` tool results for argument
validation. See [the protocol decision](docs/design.md#task-1-protocol-and-stdio).

## HTTP API

| Endpoint | Auth | Request |
|---|---|---|
| `POST /mcp` | `Authorization: Bearer <signed JWT>` | MCP JSON-RPC object |
| `POST /v1/completions` | `X-API-Key: <tenant key>` | `{"prompt":"Hi","max_tokens":100}` |
| `POST /v1/stream` | `X-API-Key: <tenant key>` | `{"prompt":"Hi","max_tokens":100}` |
| `GET /health` | None | Returns process readiness after startup |

Set `Content-Type: application/json` on POST requests. `/mcp` passes `tools/list`
through unchanged, including admin tool names. It authorizes **execution**: a viewer's
`admin_*` call returns HTTP 403 and the following error without contacting the downstream:

```json
{"jsonrpc":"2.0","id":7,"error":{"code":-32001,"message":"Unauthorized Tool Call"}}
```

The LLM stream is SSE with `delta`, `done`, or `error` events. Only sanitized text and
validated usage are sent downstream; raw upstream events are not forwarded.

```text
event: delta
data: {"text": "Hello! Email [REDACTED], "}

event: done
data: {"usage": {"prompt_tokens": 1, "completion_tokens": 33}, "provider": "primary"}
```

The usage numbers above illustrate the shape. Actual counts come from the provider.
Concatenating all `delta.text` fields from the default mock produces:

```text
Hello! Email [REDACTED], SSN [REDACTED], card [REDACTED]. Done.
```

Mock fault controls, passed as part of `prompt`:

| Prompt marker | Behavior |
|---|---|
| `[primary-429]` | Primary returns 429; backup serves the request |
| `[primary-timeout]` | Primary delays four seconds; router cancels waiting at three seconds and uses backup |
| `[both-fail]` | Upstream returns 500; gateway returns a sanitized 502 |

The policy retries **429 and timeout only**, once. Other failures return a standard payload:

```json
{"error":{"code":"UPSTREAM_UNAVAILABLE","message":"Model request could not be completed","request_id":"..."}}
```

Local quota errors use HTTP 429, `RATE_LIMITED`, and `Retry-After`. Validation errors use
HTTP 400 and `INVALID_REQUEST`. No upstream exception message or stack trace is included.

## Run tests

```sh
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m pip check
```

The suite includes actual subprocess stdio tests and a real-socket gateway integration
test, alongside authentication, stream-boundary, memory-bound, SQLite contention,
persistence, clock, token accounting, timeout cancellation, and sanitization tests.

To see local TTFT and fallback timing from the live test:

```sh
python -m pytest tests/test_live_http.py -q -s
```

CI runs the suite on Windows and Linux with Python 3.11 and 3.13.

## Configuration and integration

For separate gateway and mock processes, copy [`.env.example`](.env.example) to `.env`,
set random secrets, and run:

```sh
python -m uvicorn gateway_lab.mock:app --host 127.0.0.1 --port 9001
python -m uvicorn gateway_lab.app:app --host 127.0.0.1 --port 9000 --env-file .env
```

The provider interface is an explicit **local adapter contract**, documented in
[`docs/design.md`](docs/design.md#provider-adapter-contract). Real vendors need request,
delta, tokenization, and usage translation in that adapter. It does not claim drop-in
compatibility with a particular vendor's API.

See [design decisions and operational limits](docs/design.md) for the exact guardrail
coverage, stream timing tradeoffs, quota semantics, and deployment boundaries.
