"""Small PoC for the bonus architecture brief.

Demonstrates deterministic HMAC tokenization before records become analyst
readable. Run with:

    python submission/bonus/poc/tokenization_poc.py
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone


SECRET = b"demo-key-store-this-in-kms"
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
PHONE_RE = re.compile(r"(?:\+?84|0)[\s.-]?\d{2,3}[\s.-]?\d{3}[\s.-]?\d{3,4}\b")


def token(kind: str, value: str) -> str:
    normalized = re.sub(r"\s+", "", value.strip().lower())
    digest = hmac.new(SECRET, normalized.encode(), hashlib.sha256).hexdigest()[:24]
    return f"tok_{kind}_{digest}"


def redact_text(text: str) -> tuple[str, dict[str, str]]:
    tokens: dict[str, str] = {}

    def repl_email(match: re.Match[str]) -> str:
        value = match.group(0)
        t = token("email", value)
        tokens[t] = value
        return t

    def repl_phone(match: re.Match[str]) -> str:
        value = match.group(0)
        t = token("phone", value)
        tokens[t] = value
        return t

    text = EMAIL_RE.sub(repl_email, text)
    text = PHONE_RE.sub(repl_phone, text)
    return text, tokens


@dataclass
class BronzeEvent:
    request_id: str
    tenant_id: str
    event_ts: str
    raw_json_tokenized: str
    token_count: int


@dataclass
class SilverEvent:
    request_id: str
    tenant_id: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    latency_ms: int
    cost_usd: float
    pii_token_count: int


PRICE = {
    "gpt-fast": (0.50, 1.50),
    "gpt-quality": (3.00, 10.00),
}


def bronze_ingest(raw: dict) -> tuple[BronzeEvent, dict[str, str]]:
    tokenized_payload = json.dumps(raw, ensure_ascii=False)
    tokenized_payload, token_map = redact_text(tokenized_payload)
    return (
        BronzeEvent(
            request_id=raw["request_id"],
            tenant_id=raw["tenant_id"],
            event_ts=datetime.now(timezone.utc).isoformat(),
            raw_json_tokenized=tokenized_payload,
            token_count=len(token_map),
        ),
        token_map,
    )


def silver_project(bronze: BronzeEvent) -> SilverEvent:
    raw = json.loads(bronze.raw_json_tokenized)
    c_in, c_out = PRICE[raw["model"]]
    cost = (raw["usage"]["input"] * c_in + raw["usage"]["output"] * c_out) / 1_000_000
    return SilverEvent(
        request_id=raw["request_id"],
        tenant_id=raw["tenant_id"],
        model=raw["model"],
        prompt_tokens=raw["usage"]["input"],
        completion_tokens=raw["usage"]["output"],
        latency_ms=raw["latency_ms"],
        cost_usd=round(cost, 6),
        pii_token_count=bronze.token_count,
    )


def main() -> None:
    raw = {
        "request_id": "req-001",
        "tenant_id": "tenant-a",
        "model": "gpt-quality",
        "prompt": "Ship invoice to alice@example.com, phone +84 912 345 678.",
        "response": "Confirmed. We will not expose alice@example.com downstream.",
        "usage": {"input": 1200, "output": 300},
        "latency_ms": 842,
    }

    bronze, vault_delta = bronze_ingest(raw)
    silver = silver_project(bronze)

    print("Bronze event exposed to lakehouse:")
    print(json.dumps(asdict(bronze), indent=2, ensure_ascii=False))
    print("\nSilver event exposed to analysts:")
    print(json.dumps(asdict(silver), indent=2, ensure_ascii=False))
    print("\nToken vault delta, stored separately with break-glass access:")
    print(json.dumps(vault_delta, indent=2, ensure_ascii=False))

    assert "alice@example.com" not in bronze.raw_json_tokenized
    assert "+84 912 345 678" not in bronze.raw_json_tokenized
    assert silver.pii_token_count == 2


if __name__ == "__main__":
    main()
