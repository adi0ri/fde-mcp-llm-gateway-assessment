from contextlib import asynccontextmanager

import httpx
import pytest

from gateway_lab.app import create_app
from gateway_lab.config import Settings

API_KEY = "test-tenant-key-0123456789"
SECRET = "test-signing-secret-0123456789abcdef"


@pytest.fixture
def settings(tmp_path):
    return Settings(
        jwt_secret=SECRET,
        tenant_api_keys={API_KEY: "tenant-a"},
        database=tmp_path / "quota.sqlite3",
    )


@asynccontextmanager
async def gateway(settings, handler):
    app = create_app(settings, httpx.MockTransport(handler))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway"
        ) as client:
            yield client, app


class BytesStream(httpx.AsyncByteStream):
    def __init__(self, chunks, delay=0):
        self.chunks, self.delay, self.closed = chunks, delay, False

    async def __aiter__(self):
        import asyncio

        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            yield chunk

    async def aclose(self):
        self.closed = True
