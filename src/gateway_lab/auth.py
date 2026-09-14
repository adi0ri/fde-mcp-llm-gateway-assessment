import hashlib
import hmac
import time

import jwt

from gateway_lab.config import Settings

ISSUER = "gateway-lab"
AUDIENCE = "mcp-gateway"


class AuthError(Exception):
    pass


def bearer_role(header: str | None, settings: Settings) -> str:
    parts = (header or "").split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise AuthError
    try:
        claims = jwt.decode(
            parts[1],
            settings.jwt_secret,
            algorithms=["HS256"],
            issuer=ISSUER,
            audience=AUDIENCE,
            options={"require": ["exp", "iat", "sub", "iss", "aud", "role"]},
        )
        if claims["role"] not in ("admin", "viewer"):
            raise AuthError
        return claims["role"]
    except (jwt.PyJWTError, KeyError, TypeError):
        raise AuthError from None


def tenant_bucket(api_key: str | None, settings: Settings) -> str:
    if api_key:
        for known_key, tenant in settings.tenant_api_keys.items():
            if hmac.compare_digest(api_key.encode(), known_key.encode()):
                # Raw tenant API keys never enter SQLite or logs. Quota is per API key.
                return hashlib.sha256((tenant + "\0" + api_key).encode()).hexdigest()
    raise AuthError


def issue_demo_token(secret: str, role: str, expires_in: int = 3600) -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "sub": "demo-user",
            "role": role,
            "iat": now,
            "exp": now + expires_in,
            "iss": ISSUER,
            "aud": AUDIENCE,
        },
        secret,
        algorithm="HS256",
    )
