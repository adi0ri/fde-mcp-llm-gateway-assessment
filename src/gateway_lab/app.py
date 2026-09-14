"""Tasks 2–4: authenticated MCP proxy, streaming guardrail and resilient router."""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

import anyio
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from gateway_lab.auth import AuthError, bearer_role, tenant_bucket
from gateway_lab.config import Settings
from gateway_lab.limiter import RateLimited, TokenLimiter
from gateway_lab.provider import (
    CompletionRequest,
    ModelRouter,
    TokenCounter,
    UpstreamFailure,
    Usage,
    bounded_body,
    sse,
    sse_data,
)
from gateway_lab.redaction import StreamingRedactor
from gateway_lab.wire import WireError, error, parse_rpc, strict_json

LOG = logging.getLogger(__name__)
BODY_LIMIT = 64 * 1024


class GatewayError(Exception):
    def __init__(self, status: int, code: str, message: str, retry_after: int | None = None):
        self.status, self.code, self.message, self.retry_after = status, code, message, retry_after
        super().__init__(code)


def gateway_error(code: str, message: str, request_id: str) -> dict:
    return {"error": {"code": code, "message": message, "request_id": request_id}}


async def read_body(request: Request) -> bytes:
    if request.headers.get("content-type", "").split(";")[0].lower() != "application/json":
        raise GatewayError(415, "UNSUPPORTED_MEDIA_TYPE", "Use application/json")
    body = bytearray()
    try:
        async with asyncio.timeout(10):
            async for chunk in request.stream():
                if len(body) + len(chunk) > BODY_LIMIT:
                    raise GatewayError(413, "REQUEST_TOO_LARGE", "Request exceeds size limit")
                body.extend(chunk)
    except TimeoutError:
        raise GatewayError(408, "REQUEST_TIMEOUT", "Request body timed out") from None
    return bytes(body)


def validate_usage(usage: Usage, prompt_tokens: int, max_tokens: int):
    if usage.prompt_tokens != prompt_tokens or usage.completion_tokens > max_tokens:
        raise UpstreamFailure("invalid_usage")


def create_app(settings: Settings | None = None, transport: httpx.AsyncBaseTransport | None = None):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = settings or Settings.from_env()
        limiter = TokenLimiter(config.database, config.tokens_per_minute)
        await limiter.initialize()
        # Resolve tokenizer at startup, so its first-use asset download never adds request TTFT.
        counter = await asyncio.to_thread(TokenCounter)
        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(10, connect=3),
            follow_redirects=False,
            trust_env=False,
        ) as client:
            app.state.config, app.state.limiter, app.state.counter = config, limiter, counter
            app.state.client = client
            app.state.router = ModelRouter(
                client,
                config.primary_url,
                config.backup_url,
                config.primary_timeout,
                config.backup_timeout,
                config.provider_token,
            )
            yield

    app = FastAPI(title="MCP & LLM Gateway Assessment", version="1.0.0", lifespan=lifespan)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.request_id = uuid.uuid4().hex
        try:
            response = await call_next(request)
        except Exception as exc:
            LOG.error(
                "Gateway failure request_id=%s type=%s",
                request.state.request_id,
                type(exc).__name__,
            )
            response = JSONResponse(
                gateway_error(
                    "INTERNAL_ERROR", "Request could not be completed", request.state.request_id
                ),
                status_code=500,
            )
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    @app.exception_handler(GatewayError)
    async def handle_error(request: Request, exc: GatewayError):
        headers = {"Retry-After": str(exc.retry_after)} if exc.retry_after is not None else {}
        return JSONResponse(
            gateway_error(exc.code, exc.message, request.state.request_id),
            status_code=exc.status,
            headers=headers,
        )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/mcp")
    async def mcp_gateway(request: Request):
        try:
            raw = await read_body(request)
            payload = parse_rpc(raw)
        except WireError as exc:
            return JSONResponse(error(exc.code, exc.message, exc.request_id), status_code=400)
        except GatewayError as exc:
            return JSONResponse(error(-32600, exc.message), status_code=exc.status)
        request_id = payload.get("id")
        notification = "id" not in payload

        def reject(code, message, status):
            if notification:
                return Response(status_code=status)
            return JSONResponse(error(code, message, request_id), status_code=status)

        try:
            role = bearer_role(request.headers.get("authorization"), app.state.config)
        except AuthError:
            response = reject(-32000, "Authentication required", 401)
            response.headers["WWW-Authenticate"] = "Bearer"
            return response
        if payload["method"] == "tools/call":
            params = payload.get("params", {})
            if not isinstance(params.get("name"), str) or (
                "arguments" in params and not isinstance(params["arguments"], dict)
            ):
                return reject(-32602, "Invalid params", 400)
            if params["name"].startswith("admin_") and role != "admin":
                return reject(-32001, "Unauthorized Tool Call", 403)
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        for header in ("mcp-protocol-version", "mcp-session-id"):
            if header in request.headers:
                headers[header] = request.headers[header]
        if app.state.config.downstream_token:
            headers["Authorization"] = "Bearer " + app.state.config.downstream_token
        try:
            async with asyncio.timeout(3):
                async with app.state.client.stream(
                    "POST", app.state.config.downstream_url, content=raw, headers=headers
                ) as downstream:
                    body = await bounded_body(downstream)
                    if notification:
                        return Response(status_code=204)
                    if (
                        downstream.headers.get("content-type", "").split(";")[0]
                        != "application/json"
                    ):
                        raise UpstreamFailure("invalid_response")
                    # This task implements HTTP/JSON responses, not a stateful HTTP SSE session.
                    # Preserve accepted JSON response bytes, status and selected transport headers.
                    strict_json(body)
                    response_headers = {
                        key: value
                        for key, value in downstream.headers.items()
                        if key
                        in ("content-type", "mcp-session-id", "mcp-protocol-version", "retry-after")
                    }
                    return Response(
                        body, status_code=downstream.status_code, headers=response_headers
                    )
        except (TimeoutError, httpx.HTTPError, UpstreamFailure, ValueError, UnicodeError):
            return reject(-32603, "Downstream unavailable", 502)

    async def admit(request: Request):
        try:
            tenant = tenant_bucket(request.headers.get("x-api-key"), app.state.config)
        except AuthError:
            raise GatewayError(401, "UNAUTHORIZED", "Valid API key required") from None
        try:
            body = CompletionRequest.model_validate(strict_json(await read_body(request)))
        except (ValueError, UnicodeError, RecursionError):
            raise GatewayError(400, "INVALID_REQUEST", "Invalid completion request") from None
        prompt_tokens = await asyncio.to_thread(app.state.counter.count, body.prompt)
        try:
            reservation = await app.state.limiter.reserve(tenant, prompt_tokens + body.max_tokens)
        except RateLimited as exc:
            raise GatewayError(
                429, "RATE_LIMITED", "Token budget exhausted", exc.retry_after
            ) from None
        return body, prompt_tokens, reservation

    @app.post("/v1/completions")
    async def complete(request: Request):
        body, prompt_tokens, reservation = await admit(request)
        try:
            result, provider = await app.state.router.complete(body)
            validate_usage(result.usage, prompt_tokens, body.max_tokens)
            if app.state.counter.count(result.text) != result.usage.completion_tokens:
                raise UpstreamFailure("invalid_usage")
            await app.state.limiter.settle(reservation, result.usage.total)
        except UpstreamFailure:
            raise GatewayError(
                502, "UPSTREAM_UNAVAILABLE", "Model request could not be completed"
            ) from None
        redactor = StreamingRedactor()
        return {
            "text": redactor.feed(result.text) + redactor.finish(),
            "usage": result.usage.model_dump(),
            "provider": provider,
            "request_id": request.state.request_id,
        }

    @app.post("/v1/stream")
    async def stream(request: Request):
        body, prompt_tokens, reservation = await admit(request)
        try:
            handle = await app.state.router.open_stream(body)
        except UpstreamFailure:
            raise GatewayError(
                502, "UPSTREAM_UNAVAILABLE", "Model request could not be completed"
            ) from None

        async def events():
            redactor = StreamingRedactor()
            usage = None
            total_chars = 0
            finished = False
            try:
                # Socket read timeout is 10s; this is a second, total stream deadline.
                async with asyncio.timeout(60):
                    async for data in sse_data(handle.response):
                        if data == "[DONE]":
                            if usage is None:
                                raise UpstreamFailure("missing_usage")
                            validate_usage(usage, prompt_tokens, body.max_tokens)
                            await app.state.limiter.settle(reservation, usage.total)
                            tail = redactor.finish()
                            if tail:
                                yield sse("delta", {"text": tail})
                            yield sse(
                                "done", {"usage": usage.model_dump(), "provider": handle.provider}
                            )
                            finished = True
                            break
                        value = strict_json(data)
                        if not isinstance(value, dict):
                            raise UpstreamFailure("invalid_stream")
                        if (
                            set(value) == {"delta"}
                            and type(value["delta"]) is str
                            and usage is None
                        ):
                            delta = value["delta"]
                            total_chars += len(delta)
                            if total_chars > min(1024 * 1024, body.max_tokens * 128):
                                raise UpstreamFailure("response_too_large")
                            cleaned = redactor.feed(delta)
                            if cleaned:
                                yield sse("delta", {"text": cleaned})
                        elif set(value) == {"usage"} and usage is None:
                            usage = Usage.model_validate(value["usage"])
                        else:
                            raise UpstreamFailure("invalid_stream")
                if not finished:
                    raise UpstreamFailure("truncated_stream")
            except Exception as exc:
                # Never flush incomplete candidates after a broken stream. No raw provider
                # data, exception messages or upstream metadata are copied into error events.
                LOG.warning(
                    "Stream failed request_id=%s type=%s",
                    request.state.request_id,
                    type(exc).__name__,
                )
                yield sse(
                    "error",
                    gateway_error(
                        "UPSTREAM_STREAM_ERROR",
                        "Model stream interrupted",
                        request.state.request_id,
                    ),
                )
            finally:
                # Starlette cancels the response task when the client disconnects.
                # Shield cleanup so that cancellation also closes the upstream socket.
                with anyio.move_on_after(3, shield=True):
                    await handle.response.aclose()

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    return app


app = create_app()
