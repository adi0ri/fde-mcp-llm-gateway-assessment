"""Strict JSON parsing and small JSON-RPC helpers shared by both transports."""

import json
import math
from typing import Any


class WireError(Exception):
    def __init__(self, code: int, message: str, request_id: str | int | None = None):
        self.code, self.message, self.request_id = code, message, request_id
        super().__init__(message)


def _pairs(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def _constant(_: str):
    raise ValueError("Non-finite JSON number")


def _float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Non-finite JSON number")
    return number


def strict_json(raw: str | bytes) -> Any:
    return json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant, parse_float=_float)


def error(code: int, message: str, request_id=None, data=None) -> dict:
    detail = {"code": code, "message": message}
    if data is not None:
        detail["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": detail}


def parse_rpc(raw: str | bytes) -> dict:
    try:
        value = strict_json(raw)
    except (ValueError, UnicodeError, RecursionError):
        raise WireError(-32700, "Parse error") from None
    if not isinstance(value, dict):
        raise WireError(-32600, "Invalid Request")
    request_id = value.get("id")
    if "id" in value and type(request_id) not in (str, int):
        raise WireError(-32600, "Invalid Request")
    if value.get("jsonrpc") != "2.0" or not isinstance(value.get("method"), str):
        raise WireError(-32600, "Invalid Request", request_id)
    if "params" in value and not isinstance(value["params"], dict):
        raise WireError(-32602, "Invalid params", request_id)
    return value
