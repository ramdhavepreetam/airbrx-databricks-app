# Gateway ↔ App Contract

**Version:** 1  
**Status:** Locked for v1. Coordinate any change with both the gateway repo and this app.

This document is the single source of truth for the interface between the Airbrx gateway and this Databricks app. Both sides must implement it identically.

---

## 1. Leading comment tag

Every SQL statement forwarded by the gateway has this prepended:

```
/* abx v=<version> ck=<fingerprint> rule=<rule_id> route=<route_value> */
SELECT ...
```

| Field | Type | Values |
|---|---|---|
| `v` | int | Protocol version. Currently `1`. Bump when algorithm changes. |
| `ck` | string | Base32-encoded fingerprint (see §2). 16 chars. |
| `rule` | string | Rule ID that matched, e.g. `r42`. |
| `route` | string | `warehouse` \| `cache_miss` \| `smaller_wh` \| `bypass` |

**Rules:**
- If a pre-existing leading comment exists (e.g. from dbt or Looker), the gateway **prepends** the `abx` block and preserves the original.
- Bypass-to-DuckDB queries never reach Databricks so they carry no tag.
- The app parses the tag with `parse_tag()` in `src/lib/tags.py`.

---

## 2. Canonical fingerprint algorithm (v=1)

Both sides must produce byte-identical output for the same SQL input.

**Steps (in order):**

1. Strip all block comments (`/* ... */`) — including the `abx` tag itself.
2. Strip all line comments (`-- ...`).
3. Lowercase the entire string.
4. Collapse all whitespace sequences (spaces, tabs, newlines) to a single space; strip leading/trailing.
5. Replace all string literals (`'...'`) with `?`.
6. Replace all numeric literals (`\b\d+(?:\.\d+)?\b`) with `?`.
7. UTF-8 encode the result.
8. SHA-256 hash the bytes.
9. Base32-encode (RFC 4648); lowercase; truncate to **16 characters**.

**Reference implementation:** `src/lib/fingerprint.py`

**Golden vectors** (pinned in `tests/test_fingerprint.py`):

| Input | Normalized | Expected ck |
|---|---|---|
| `SELECT id FROM users WHERE id = 1` | `select id from users where id = ?` | run `fingerprint("SELECT id FROM users WHERE id = 1")` |
| `/* abx v=1 ... */ SELECT id FROM users WHERE id = 1` | same as above | same ck — tag stripped first |
| `SELECT * FROM t WHERE a = 'foo'` | `select * from t where a = ?` | same as `WHERE a = 'bar'` |

**Version evolution:**
- Bump `VERSION` in `fingerprint.py` when any step changes.
- Update the `v=` field in the gateway tag.
- Old and new versions will produce different `ck` values for the same SQL — this is intentional. The app reads `v` from the tag to know which algorithm was used.
- Add new golden vectors to the test file before shipping any algorithm change.

---

## 3. What crosses the network boundary

Only these fields are sent to `api.airbrx.ai`:

**Inbound (gateway → Airbrx cloud):** nothing — gateway pushes tagged SQL into the customer workspace only.

**Outbound from app (`POST /v1/effectiveness`):**
```json
{
  "reported_at": "<ISO timestamp>",
  "effectiveness": [
    {"rule_id": "r42", "ck": "abcdef1234567890", "window": "2024-01-01/2024-01-31",
     "avoided": 142, "realized_usd": 284.50}
  ]
}
```

No query text. No warehouse IDs. No user names. No raw metering rows.

**Inbound from Airbrx cloud (`GET /v1/rules`):**
```json
{
  "rules": [
    {"rule_id": "r42", "rule_type": "cache", "description": "...", "config_json": "{...}"}
  ]
}
```
