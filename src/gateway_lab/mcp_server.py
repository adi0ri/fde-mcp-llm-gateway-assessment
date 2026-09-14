"""Task 1: official SDK server/stdio transport with explicit protocol errors."""

import json
import logging
import sys
import uuid
from typing import Annotated, get_args

import anyio
import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from gateway_lab.wire import WireError, error, parse_rpc, strict_json

LOG = logging.getLogger(__name__)
MAX_LINE = 1024 * 1024
CLIENT_METHODS = {
    model.model_fields["method"].default for model in get_args(types.ClientRequestType)
}


class CustomerInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    customer_id: Annotated[str, Field(pattern=r"^CUST-[0-9]{5}$", min_length=10, max_length=10)]


class RefundInput(CustomerInput):
    amount: Annotated[float, Field(gt=0, allow_inf_nan=False)]
    reason: Annotated[str, Field(min_length=10, max_length=4096, pattern=r"\S(?:[\s\S]*\S)?")]


SCHEMAS = {"get_customer_record": CustomerInput, "trigger_refund": RefundInput}
CUSTOMERS = {"CUST-00001": {"customer_id": "CUST-00001", "name": "Demo Customer", "tier": "gold"}}


def execute_tool(name: str, arguments: dict) -> types.CallToolResult:
    schema = SCHEMAS.get(name)
    if schema is None:
        raise McpError(types.ErrorData(code=-32602, message="Unknown tool"))
    try:
        params = schema.model_validate(arguments)
        if isinstance(params, RefundInput) and len(params.reason.strip()) < 10:
            raise ValueError("Reason must contain at least 10 characters after trimming")
    except (ValidationError, ValueError) as exc:
        details = (
            [
                {"field": ".".join(map(str, item["loc"])), "type": item["type"]}
                for item in exc.errors(include_input=False, include_context=False)
            ]
            if isinstance(exc, ValidationError)
            else [{"field": "reason", "type": "string_too_short"}]
        )
        raise McpError(
            types.ErrorData(code=-32602, message="Invalid params", data=details)
        ) from None
    if params.customer_id not in CUSTOMERS:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text="Customer not found")],
            isError=True,
        )
    if isinstance(params, RefundInput):
        # Simulated acknowledgement only; no payment system is connected.
        result = {
            "refund_id": "REF-" + uuid.uuid4().hex,
            "status": "simulated",
            "customer_id": params.customer_id,
            "amount": params.amount,
        }
    else:
        result = CUSTOMERS[params.customer_id]
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(result))],
        structuredContent=result,
        isError=False,
    )


def create_server() -> Server:
    server = Server("customer-records", version="1.0.0")

    @server.list_tools()
    async def list_tools():
        return [
            types.Tool(
                name=name,
                description=(
                    "Fetch a mock customer record"
                    if name == "get_customer_record"
                    else "Simulate a refund; no real money moves"
                ),
                inputSchema=schema.model_json_schema(),
            )
            for name, schema in SCHEMAS.items()
        ]

    async def call_tool(request: types.CallToolRequest):
        try:
            result = execute_tool(request.params.name, request.params.arguments or {})
            return types.ServerResult(result)
        except McpError:
            raise
        except Exception:
            LOG.error("Unexpected tool execution failure")
            raise McpError(types.ErrorData(code=-32603, message="Internal error")) from None

    # SDK 1.30's call_tool decorator converts exceptions into isError results.
    # Register its low-level handler directly to preserve the assessment's -32602 contract.
    server.request_handlers[types.CallToolRequest] = call_tool
    return server


class ProtocolOutput:
    """Serialize complete lines from SDK responses and framing errors."""

    def __init__(self, stream):
        self.stream, self.lock = stream, anyio.Lock()

    async def write(self, text: str):
        async with self.lock:
            await self.stream.write(text.encode("utf-8"))
            await self.stream.flush()

    async def flush(self):
        pass  # write() flushes while holding the lock.


class ValidatedInput:
    """Validate framing before passing lines to the official stdio transport.

    The SDK transport forwards parse exceptions to its session without a JSON-RPC
    reply. This adapter adds -32700/-32600/-32602 replies and bounded line reads.
    """

    def __init__(self, stream, output: ProtocolOutput):
        self.stream, self.output = stream, output

    async def _readline(self):
        return await anyio.to_thread.run_sync(self.stream.readline, MAX_LINE + 1)

    async def __aiter__(self):
        while raw := await self._readline():
            try:
                if len(raw) > MAX_LINE:
                    while not raw.endswith(b"\n"):
                        raw = await self._readline()
                        if not raw:
                            break
                    raise WireError(-32600, "Request exceeds size limit")
                try:
                    value = strict_json(raw)
                except (ValueError, UnicodeError, RecursionError):
                    raise WireError(-32700, "Parse error") from None
                # Server-to-client calls are unused, but allow SDK response envelopes.
                if isinstance(value, dict) and ("result" in value or "error" in value):
                    try:
                        types.JSONRPCMessage.model_validate(value)
                    except ValidationError:
                        raise WireError(-32600, "Invalid Request") from None
                else:
                    value = parse_rpc(raw)
                    if "id" in value:
                        try:
                            types.ClientRequest.model_validate(value)
                        except ValidationError:
                            unknown = value["method"] not in CLIENT_METHODS
                            raise WireError(
                                -32601 if unknown else -32602,
                                "Method not found" if unknown else "Invalid params",
                                value["id"],
                            ) from None
                yield raw.decode("utf-8")
            except WireError as exc:
                await self.output.write(
                    json.dumps(
                        error(
                            exc.code,
                            exc.message,
                            exc.request_id,
                        )
                    )
                    + "\n"
                )


async def run():
    output = ProtocolOutput(anyio.wrap_file(sys.stdout.buffer))
    stdin = ValidatedInput(sys.stdin.buffer, output)
    LOG.info("Customer MCP server starting (stdio)")
    server = create_server()
    async with stdio_server(stdin=stdin, stdout=output) as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main():
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
        force=True,
    )
    anyio.run(run)


if __name__ == "__main__":
    main()
