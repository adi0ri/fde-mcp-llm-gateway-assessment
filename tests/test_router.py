import asyncio
import json
from dataclasses import replace

import httpx
import pytest
from conftest import API_KEY, BytesStream, gateway

from gateway_lab.provider import CompletionRequest, ModelRouter, TokenCounter, UpstreamFailure


@pytest.mark.parametrize("failure", ["429", "timeout", "slow_body"])
async def test_primary_falls_back_once_and_cancels_timed_out_work(failure):
    calls, cancelled = [], []
    body = BytesStream([b"{}"], delay=0.1)

    async def provider(request):
        calls.append(request.url.host)
        if request.url.host == "primary":
            if failure == "429":
                return httpx.Response(429, text="private upstream data")
            if failure == "slow_body":
                return httpx.Response(200, stream=body)
            try:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
        return httpx.Response(
            200,
            json={
                "text": "Hello",
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        router = ModelRouter(
            client, "http://primary/generate", "http://backup/generate", primary_timeout=0.01
        )
        result, name = await router.complete(CompletionRequest(prompt="Hi"))
        assert result.text == "Hello" and name == "backup"
        assert calls == ["primary", "backup"]
        if failure == "timeout":
            assert cancelled == [True]
        if failure == "slow_body":
            assert body.closed


async def test_default_primary_deadline_is_three_seconds():
    async with httpx.AsyncClient() as client:
        router = ModelRouter(client, "http://primary", "http://backup")
        assert router.endpoints[0][2] == 3.0


async def test_non_retryable_failure_never_calls_backup():
    calls = []

    def provider(request):
        calls.append(request.url.host)
        return httpx.Response(500, text="stack trace private")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        router = ModelRouter(client, "http://primary", "http://backup")
        with pytest.raises(UpstreamFailure):
            await router.complete(CompletionRequest(prompt="Hi"))
        assert calls == ["primary"]


async def test_primary_success_does_not_call_backup_and_usage_settles(settings):
    calls = []

    def provider(request):
        calls.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "text": "Hello",
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                },
            },
        )

    settings = replace(settings, tokens_per_minute=10)
    async with gateway(settings, provider) as (client, _):
        for _ in range(2):
            response = await client.post(
                "/v1/completions",
                json={"prompt": "Hi", "max_tokens": 5},
                headers={"X-API-Key": API_KEY},
            )
            assert response.status_code == 200
        # Two requests initially reserved 12 tokens; actual usage was only 4.
        assert calls == ["/primary/generate"] * 2


async def test_errors_auth_and_local_quota_are_standardized(settings):
    calls = []

    def provider(request):
        calls.append(request.url.path)
        return httpx.Response(429, json={"error": "secret=upstream-stack-trace"})

    async with gateway(replace(settings, tokens_per_minute=10), provider) as (client, _):
        denied = await client.post("/v1/completions", json={"prompt": "Hi"})
        assert denied.status_code == 401 and calls == []
        headers = {"X-API-Key": API_KEY}
        response = await client.post(
            "/v1/completions", json={"prompt": "Hi", "max_tokens": 5}, headers=headers
        )
        assert response.status_code == 502
        assert set(response.json()["error"]) == {"code", "message", "request_id"}
        assert "secret" not in response.text and "stack" not in response.text
        assert calls == ["/primary/generate", "/backup/generate"]
        limited = await client.post(
            "/v1/completions", json={"prompt": "Hi", "max_tokens": 5}, headers=headers
        )
        assert limited.status_code == 429
        assert limited.json()["error"]["code"] == "RATE_LIMITED"
        assert 1 <= int(limited.headers["retry-after"]) <= 60
        assert len(calls) == 2


@pytest.mark.parametrize(
    "body",
    [
        {"prompt": "", "max_tokens": 1},
        {"prompt": "Hi", "max_tokens": True},
        {"prompt": "Hi", "max_tokens": "5"},
        {"prompt": "Hi", "extra": "bad"},
    ],
)
async def test_invalid_completion_does_not_reach_provider(settings, body):
    def provider(_):
        pytest.fail("Invalid request was forwarded")

    async with gateway(settings, provider) as (client, _):
        response = await client.post("/v1/completions", json=body, headers={"X-API-Key": API_KEY})
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "INVALID_REQUEST"


async def test_stream_redacts_all_deltas_and_discards_upstream_metadata(settings):
    text = "Hello alice@example.com, SSN 123-45-6789, card 4111 1111 1111 1111."
    counter = TokenCounter()
    wire = "".join("data: " + json.dumps({"delta": c}) + "\n\n" for c in text)
    wire += (
        "data: "
        + json.dumps(
            {
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": counter.count(text),
                }
            }
        )
        + "\n\ndata: [DONE]\n\n"
    )
    upstream = BytesStream([wire.encode()[i : i + 3] for i in range(0, len(wire), 3)])

    def provider(_):
        return httpx.Response(200, stream=upstream, headers={"content-type": "text/event-stream"})

    async with gateway(settings, provider) as (client, _):
        response = await client.post(
            "/v1/stream", json={"prompt": "Hi", "max_tokens": 100}, headers={"X-API-Key": API_KEY}
        )
        values = [
            json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")
        ]
        cleaned = "".join(item.get("text", "") for item in values)
        assert cleaned == "Hello [REDACTED], SSN [REDACTED], card [REDACTED]."
        assert "event: done" in response.text
        assert "alice" not in response.text and "123-45" not in response.text
        assert upstream.closed


@pytest.mark.parametrize("suffix", ["", 'data: {"debug":"secret"}\n\n', "data: [DONE]\n\n"])
async def test_truncated_stream_is_sanitized_and_never_flushes_pending_pii(settings, suffix):
    wire = 'data: {"delta":"Safe alice@exam"}\n\n' + suffix
    stream = BytesStream([wire.encode()])

    def provider(_):
        return httpx.Response(200, stream=stream, headers={"content-type": "text/event-stream"})

    async with gateway(settings, provider) as (client, _):
        response = await client.post(
            "/v1/stream", json={"prompt": "Hi"}, headers={"X-API-Key": API_KEY}
        )
        assert "event: error" in response.text
        assert "alice" not in response.text and "secret" not in response.text
        assert "event: done" not in response.text
        assert stream.closed
