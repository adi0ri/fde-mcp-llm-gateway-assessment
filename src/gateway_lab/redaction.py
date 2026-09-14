"""Bounded, chunk-boundary-independent redaction for ASCII email/SSN/card shapes.

Only ambiguous lexical candidates are held. An oversized candidate fails closed:
emit one marker and discard its remainder until the next unambiguous boundary.
"""

import re
import string

REDACTED = "[REDACTED]"
ATOM = frozenset(string.ascii_letters + string.digits + ".!#$%&'*+/=?^_`{|}~@-")
DIGITS = frozenset(string.digits)
NUMERIC = DIGITS | frozenset(" -\t")
EMAIL = re.compile(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
SSN = re.compile(r"(?<![0-9])[0-9]{3}-[0-9]{2}-[0-9]{4}(?![0-9])")
# Conservatively mask 13+ digit runs, including adjacent cards separated only by spaces.
# Matching is bounded by max_candidate, so the unbounded quantifier cannot grow state.
CARD = re.compile(r"(?<![0-9])[0-9](?:[ \t-]?[0-9]){12,}(?![0-9])")


class StreamingRedactor:
    def __init__(self, max_candidate: int = 512):
        if max_candidate < 32:
            raise ValueError("Candidate limit must be at least 32")
        self.max_candidate = max_candidate
        self.pending = ""
        self.numeric = False
        self.has_space = False
        self.dropping = False
        self.peak_buffer = 0

    def _flush(self) -> str:
        if self.dropping:
            result = ""
        else:
            result = EMAIL.sub(REDACTED, self.pending)
            result = SSN.sub(REDACTED, result)
            result = CARD.sub(REDACTED, result)
        self.pending = ""
        self.numeric = self.has_space = self.dropping = False
        return result

    def feed(self, delta: str) -> str:
        output = []
        for char in delta:
            active = bool(self.pending) or self.dropping
            if active:
                continues = char in ATOM
                if self.numeric:
                    # A spaced number ends before a word; unspaced digits may begin an email.
                    continues = char in NUMERIC or (not self.has_space and char in ATOM)
                if not continues:
                    output.append(self._flush())
                    active = False
            if not active:
                if char not in ATOM:
                    output.append(char)
                    continue
                self.numeric = char in DIGITS
            if self.numeric:
                self.has_space |= char in " \t"
                self.numeric = char in NUMERIC
            if not self.dropping:
                self.pending += char
                self.peak_buffer = max(self.peak_buffer, len(self.pending))
                if len(self.pending) >= self.max_candidate:
                    output.append(REDACTED)
                    self.pending = ""
                    self.dropping = True
        return "".join(output)

    def finish(self) -> str:
        return self._flush()
