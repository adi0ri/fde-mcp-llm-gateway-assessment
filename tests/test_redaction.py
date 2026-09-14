import random

import httpx
import pytest
from conftest import BytesStream

from gateway_lab.provider import MAX_EVENT, UpstreamFailure, sse_data
from gateway_lab.redaction import StreamingRedactor


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Contact alice@example.com now.", "Contact [REDACTED] now."),
        ("a.b+tag@sub.example.co.uk!", "[REDACTED]!"),
        ("SSN: 123-45-6789.", "SSN: [REDACTED]."),
        ("Card 4111 1111 1111 1111, ok", "Card [REDACTED], ok"),
        ("Card 4111-1111-1111-1111.", "Card [REDACTED]."),
        ("378282246310005", "[REDACTED]"),
        ("4111\t1111\t1111\t1111", "[REDACTED]"),
        ("4111 1111 1111 1111 4111 1111 1111 1111", "[REDACTED]"),
        ("1234567890123456789", "[REDACTED]"),
        ("Call 123-456 or pay 12.50 dollars.", "Call 123-456 or pay 12.50 dollars."),
        ("Hello 🌍 world!", "Hello 🌍 world!"),
    ],
)
def test_every_split_point_and_one_character_deltas(text, expected):
    for split in range(len(text) + 1):
        redactor = StreamingRedactor()
        actual = redactor.feed(text[:split]) + redactor.feed(text[split:]) + redactor.finish()
        assert actual == expected
    redactor = StreamingRedactor()
    assert "".join(redactor.feed(c) for c in text) + redactor.finish() == expected


def test_random_partition_invariance():
    text = "Hello alice@example.com; SSN 123-45-6789 and card 4111 1111 1111 1111. End!"
    expected = "Hello [REDACTED]; SSN [REDACTED] and card [REDACTED]. End!"
    rng = random.Random(42)
    for _ in range(200):
        redactor, result, i = StreamingRedactor(), [], 0
        while i < len(text):
            length = rng.randint(1, 13)
            result.append(redactor.feed(text[i : i + length]))
            i += length
        assert "".join(result) + redactor.finish() == expected


def test_first_word_responsive_and_sensitive_prefix_never_emitted():
    redactor = StreamingRedactor()
    assert redactor.feed("Hello ") == "Hello "
    assert redactor.feed("alice@exam") == ""
    assert redactor.feed("ple.com") == ""
    assert redactor.feed(" ") == "[REDACTED] "


def test_memory_bound_and_fail_closed_for_huge_candidate():
    redactor = StreamingRedactor(max_candidate=64)
    output = []
    for _ in range(10000):
        output.append(redactor.feed("a" * 100))
    output.append(redactor.feed("@example.com "))
    output.append(redactor.finish())
    assert "".join(output) == "[REDACTED] "
    assert redactor.peak_buffer <= 64


async def test_sse_arbitrary_utf8_crlf_boundaries_multiline_and_comments():
    wire = ': ping\r\ndata: {"delta":\r\ndata: "🌍"}\r\n\r\ndata: [DONE]\r\n\r\n'.encode()
    response = httpx.Response(200, stream=BytesStream([bytes([b]) for b in wire]))
    assert [item async for item in sse_data(response)] == ['{"delta":\n"🌍"}', "[DONE]"]


@pytest.mark.parametrize(
    "wire",
    [
        b"data: " + b"x" * (MAX_EVENT + 1),
        b"data: {}",
        b"data: {}\n",
        b"data: \xff\n\n",
    ],
    ids=["oversized-event", "partial-line", "partial-event", "invalid-utf8"],
)
async def test_bad_or_oversized_sse_is_rejected(wire):
    response = httpx.Response(200, stream=BytesStream([wire]))
    with pytest.raises((UpstreamFailure, UnicodeError)):
        _ = [item async for item in sse_data(response)]
