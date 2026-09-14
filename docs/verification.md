# Verification

Local validation on Windows / Python 3.13:

- **65 tests passed** in the complete pytest run.
- Ruff lint and format checks passed.
- `pip check` found no broken dependency requirements.
- The one-command demo and `scripts/exercise_gateway.py` completed successfully against
  live gateway and mock processes.

Observed demo behavior:

| Check | Result |
|---|---|
| Viewer calls `admin_reset_key` | HTTP 403, JSON-RPC `-32001` |
| Admin calls `admin_reset_key` | HTTP 200 |
| Email, SSN, and card in streamed response | All three replaced with `[REDACTED]` |
| Time to first sanitized text | 0.081 seconds |
| Primary 429 | Backup succeeded in 0.063 seconds |
| Primary timeout | Backup succeeded in 3.063 seconds |

Timings are one local mock run, not a performance guarantee. The live test checks that
first text arrives before generation completes and that the real timeout fallback occurs
between 2.9 and 4 seconds. CI performs independent Windows/Linux checks on Python 3.11/3.13;
the repository's Actions page is the source of truth for those run results.
