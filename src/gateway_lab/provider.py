"""Provider adapter and bounded incremental SSE parsing.

The explicit mock adapter contract is described in docs/design.md. Public model
vendors can be integrated by translating their request, delta and usage schemas here.
"""

import asyncio
import codecs
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
import tiktoken
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from gateway_lab.wire import strict_json

MAX_EVENT = 64 * 1024
MAX_RESPONSE = 1024 * 1024


class CompletionRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    prompt: str = Field(min_length=1, max_length=16_384)
    max_tokens: int = Field(default=256, ge=1, le=4096)


class Usage(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)

    @property
    def total(self) -> int:
        return self.prompt_tokens + self.completion_tokens


class ProviderResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    text: str = Field(max_length=MAX_RESPONSE)
    usage: Usage


class UpstreamFailure(Exception):
    def __init__(self, kind: str):
        self.kind = kind
        super().__init__(kind)


class TokenCounter:
    def __init__(self):
        self.encoding = tiktoken.get_encoding("cl100k_base")

    def count(self, text: str) -> int:
        return len(self.encoding.encode(text, disallowed_special=()))


async def bounded_body(response: httpx.Response, limit: int = MAX_RESPONSE) -> bytes:
    result = bytearray()
    async for chunk in response.aiter_bytes():
        if len(result) + len(chunk) > limit:
            raise UpstreamFailure("invalid_response")
        result.extend(chunk)
    return bytes(result)


async def sse_data(response: httpx.Response) -> AsyncIterator[str]:
    """Decode UTF-8/CRLF/multiline SSE across arbitrary HTTP chunk boundaries."""
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    pending = ""
    data = []
    size = 0
    async for raw in response.aiter_bytes():
        # Limit temporary text even if an upstream transport supplies a giant chunk.
        for offset in range(0, len(raw), 4096):
            pending += decoder.decode(raw[offset : offset + 4096])
            while "\n" in pending:
                line, pending = pending.split("\n", 1)
                line = line.removesuffix("\r")
                size += len(line) + 1
                if size > MAX_EVENT:
                    raise UpstreamFailure("invalid_stream")
                if not line:
                    if data:
                        yield "\n".join(data)
                    data, size = [], 0
                elif line.startswith("data:"):
                    data.append(line[5:].removeprefix(" "))
                # Comments and other SSE fields are never sent to the client.
            if len(pending) + size > MAX_EVENT:
                raise UpstreamFailure("invalid_stream")
    pending += decoder.decode(b"", final=True)
    if pending or data:
        raise UpstreamFailure("truncated_stream")


def sse(event: str, value: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(value, ensure_ascii=True)}\n\n".encode()


@dataclass
class StreamHandle:
    response: httpx.Response
    provider: str


class ModelRouter:
    def __init__(
        self,
        client: httpx.AsyncClient,
        primary: str,
        backup: str,
        primary_timeout: float = 3,
        backup_timeout: float = 3,
        token: str = "",
    ):
        self.client = client
        self.endpoints = [("primary", primary, primary_timeout), ("backup", backup, backup_timeout)]
        self.headers = {"Authorization": "Bearer " + token} if token else {}

    @staticmethod
    def _status(response: httpx.Response):
        if response.status_code == 429:
            raise UpstreamFailure("rate_limited")
        if not 200 <= response.status_code < 300:
            raise UpstreamFailure("unavailable")

    async def complete(self, request: CompletionRequest) -> tuple[ProviderResult, str]:
        for index, (name, url, timeout) in enumerate(self.endpoints):
            try:
                # A total deadline includes response headers AND body, not just socket inactivity.
                async with asyncio.timeout(timeout):
                    async with self.client.stream(
                        "POST",
                        url,
                        headers=self.headers,
                        json={**request.model_dump(), "stream": False},
                    ) as r:
                        self._status(r)
                        result = ProviderResult.model_validate(strict_json(await bounded_body(r)))
                        return result, name
            except (TimeoutError, httpx.TimeoutException):
                failure = UpstreamFailure("timeout")
            except (ValueError, UnicodeError, ValidationError):
                failure = UpstreamFailure("invalid_response")
            except httpx.HTTPError:
                failure = UpstreamFailure("unavailable")
            except UpstreamFailure as exc:
                failure = exc
            if index == 0 and failure.kind in ("timeout", "rate_limited"):
                continue
            raise failure
        raise UpstreamFailure("unavailable")

    async def open_stream(self, request: CompletionRequest) -> StreamHandle:
        for index, (name, url, timeout) in enumerate(self.endpoints):
            response = None
            try:
                async with asyncio.timeout(timeout):
                    outgoing = self.client.build_request(
                        "POST",
                        url,
                        headers={**self.headers, "Accept": "text/event-stream"},
                        json={**request.model_dump(), "stream": True},
                    )
                    response = await self.client.send(outgoing, stream=True)
                    self._status(response)
                    if (
                        response.headers.get("content-type", "").split(";")[0]
                        != "text/event-stream"
                    ):
                        raise UpstreamFailure("invalid_stream")
                return StreamHandle(response, name)
            except (TimeoutError, httpx.TimeoutException):
                failure = UpstreamFailure("timeout")
            except httpx.HTTPError:
                failure = UpstreamFailure("unavailable")
            except UpstreamFailure as exc:
                failure = exc
            except BaseException:
                if response is not None:
                    await response.aclose()
                raise
            if response is not None:
                await response.aclose()
            if index == 0 and failure.kind in ("timeout", "rate_limited"):
                continue
            raise failure
        raise UpstreamFailure("unavailable")
