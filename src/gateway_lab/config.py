import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    jwt_secret: str = field(repr=False)
    tenant_api_keys: dict[str, str] = field(repr=False)
    database: Path = Path("data/gateway.sqlite3")
    downstream_url: str = "http://127.0.0.1:9001/mcp"
    primary_url: str = "http://127.0.0.1:9001/primary/generate"
    backup_url: str = "http://127.0.0.1:9001/backup/generate"
    downstream_token: str = field(default="", repr=False)
    provider_token: str = field(default="", repr=False)
    tokens_per_minute: int = 50_000
    primary_timeout: float = 3.0
    backup_timeout: float = 3.0

    def __post_init__(self):
        if len(self.jwt_secret.encode()) < 32:
            raise ValueError("JWT_SECRET must contain at least 32 bytes")
        if not self.tenant_api_keys or any(
            not isinstance(key, str) or len(key) < 16 or not isinstance(tenant, str) or not tenant
            for key, tenant in self.tenant_api_keys.items()
        ):
            raise ValueError("TENANT_API_KEYS must map keys of at least 16 characters to tenants")
        if self.tokens_per_minute <= 0 or min(self.primary_timeout, self.backup_timeout) <= 0:
            raise ValueError("Limits and timeouts must be positive")

    @classmethod
    def from_env(cls):
        return cls(
            jwt_secret=os.environ["JWT_SECRET"],
            tenant_api_keys=json.loads(os.environ["TENANT_API_KEYS"]),
            database=Path(os.getenv("DATABASE_PATH", "data/gateway.sqlite3")),
            downstream_url=os.getenv("MCP_DOWNSTREAM_URL", "http://127.0.0.1:9001/mcp"),
            primary_url=os.getenv("PRIMARY_URL", "http://127.0.0.1:9001/primary/generate"),
            backup_url=os.getenv("BACKUP_URL", "http://127.0.0.1:9001/backup/generate"),
            downstream_token=os.getenv("MCP_DOWNSTREAM_TOKEN", ""),
            provider_token=os.getenv("PROVIDER_TOKEN", ""),
        )
