# FDE Assessment

Python implementations of four tasks covering MCP servers, security gateways, streaming guardrails, and model routing.

## Tasks

| Task | Implementation |
|---|---|
| 1. MCP server | [Official SDK stdio server](src/gateway_lab/mcp_server.py) with strict input validation and JSON-RPC errors |
| 2. MCP gateway | [JWT authentication and tool authorization](src/gateway_lab/app.py) for `admin_*` calls |
| 3. Streaming guardrail | [Bounded PII redaction](src/gateway_lab/redaction.py) for emails, SSNs, and card numbers across chunks |
| 4. Model router | [SQLite token limiter](src/gateway_lab/limiter.py) and [fallback routing](src/gateway_lab/provider.py): 50,000 tokens/minute/key, 3-second primary timeout |

## Setup

Requires **Python 3.11+**.

```sh
python -m venv .venv
```

Activate with `source .venv/bin/activate` (macOS/Linux) or `.\.venv\Scripts\Activate.ps1` (Windows PowerShell), then run:

```sh
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps
python -m gateway_lab.demo
```

The gateway runs at `http://127.0.0.1:9000`, with mock upstreams on port `9001`. Demo credentials are printed in the terminal. No paid API keys are needed; refunds are simulated.

From another activated terminal, run `python scripts/exercise_gateway.py` to check authorization, streaming redaction, and fallback. Use `python -m gateway_lab.mcp_server` for the standalone stdio server.

## Tests

```sh
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m pip check
```

65 tests cover protocol handling, authorization, stream boundaries, SQLite concurrency, and fallback. CI runs on Windows and Linux with Python 3.11 and 3.13.

See [design notes](docs/design.md), [verification results](docs/verification.md), and [configuration](.env.example) for details.
