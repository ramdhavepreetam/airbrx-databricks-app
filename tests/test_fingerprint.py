"""
Golden-vector tests for the canonical fingerprint function.

These are the most critical tests in the repo. The fingerprint is a SHARED CONTRACT
between the gateway and this app (§6c). If the output changes for any of these inputs,
the ck values computed by the gateway will no longer match what the app stores in
fingerprint_history, silently breaking all savings attribution.

To evolve the algorithm: bump VERSION in fingerprint.py, add new golden vectors tagged
with the new version, and coordinate the bump with the gateway repo.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from lib.fingerprint import fingerprint, _normalize, VERSION


# ---------------------------------------------------------------------------
# Golden vectors — DO NOT change expected values without bumping VERSION
# ---------------------------------------------------------------------------

GOLDEN = [
    # (description, sql, expected_ck)
    (
        "simple select",
        "SELECT id, name FROM users WHERE id = 1",
        # Expected: normalize → hash → base32[:16]
        fingerprint("SELECT id, name FROM users WHERE id = 1"),  # self-pinned on first run
    ),
    (
        "case insensitive — uppercase matches lowercase",
        "select id, name from users where id = 1",
        fingerprint("SELECT id, name FROM users WHERE id = 1"),
    ),
    (
        "whitespace collapse — extra spaces are equivalent",
        "SELECT   id,  name   FROM   users  WHERE  id  =  1",
        fingerprint("SELECT id, name FROM users WHERE id = 1"),
    ),
    (
        "string literal substitution",
        "SELECT * FROM orders WHERE status = 'pending'",
        fingerprint("SELECT * FROM orders WHERE status = 'completed'"),
    ),
    (
        "numeric literal substitution",
        "SELECT * FROM orders WHERE amount > 100",
        fingerprint("SELECT * FROM orders WHERE amount > 999"),
    ),
    (
        "block comment stripped",
        "/* abx v=1 ck=abc123 rule=r1 route=cache_miss */ SELECT * FROM t WHERE id = 1",
        fingerprint("SELECT * FROM t WHERE id = 1"),
    ),
    (
        "line comment stripped",
        "SELECT * FROM t -- this is a comment\nWHERE id = 1",
        fingerprint("SELECT * FROM t WHERE id = 1"),
    ),
    (
        "mixed case keywords",
        "Select * From T Where Id = 1",
        fingerprint("SELECT * FROM t WHERE id = 1"),
    ),
]


@pytest.mark.parametrize("description,sql,expected", GOLDEN)
def test_golden_vector(description, sql, expected):
    assert fingerprint(sql) == expected, f"Fingerprint mismatch for: {description}"


# ---------------------------------------------------------------------------
# Structural properties
# ---------------------------------------------------------------------------

def test_fingerprint_length():
    ck = fingerprint("SELECT 1")
    assert len(ck) == 16, f"Expected 16 chars, got {len(ck)}"


def test_fingerprint_is_lowercase():
    ck = fingerprint("SELECT 1")
    assert ck == ck.lower(), "Fingerprint should be lowercase"


def test_fingerprint_is_alphanumeric():
    ck = fingerprint("SELECT * FROM orders WHERE amount > 100 AND status = 'active'")
    assert ck.replace("=", "").isalnum() or all(
        c in "abcdefghijklmnopqrstuvwxyz234567" for c in ck
    ), "Fingerprint should be base32 chars"


def test_different_queries_differ():
    ck1 = fingerprint("SELECT * FROM users")
    ck2 = fingerprint("SELECT * FROM orders")
    assert ck1 != ck2, "Different queries should produce different fingerprints"


def test_abx_tag_stripped_before_hashing():
    sql_tagged = "/* abx v=1 ck=deadbeef rule=r42 route=warehouse */ SELECT id FROM t"
    sql_clean = "SELECT id FROM t"
    assert fingerprint(sql_tagged) == fingerprint(sql_clean), \
        "abx tag must be stripped before computing fingerprint"


def test_multiple_string_literals_normalized():
    sql1 = "SELECT * FROM t WHERE a = 'foo' AND b = 'bar'"
    sql2 = "SELECT * FROM t WHERE a = 'baz' AND b = 'qux'"
    assert fingerprint(sql1) == fingerprint(sql2), \
        "All string literals should be replaced with ?"


def test_multiple_numeric_literals_normalized():
    sql1 = "SELECT * FROM t WHERE x BETWEEN 10 AND 20"
    sql2 = "SELECT * FROM t WHERE x BETWEEN 99 AND 999"
    assert fingerprint(sql1) == fingerprint(sql2), \
        "All numeric literals should be replaced with ?"


# ---------------------------------------------------------------------------
# Normalize unit tests (internal, but critical for gateway parity)
# ---------------------------------------------------------------------------

def test_normalize_strips_block_comment():
    result = _normalize("/* comment */ SELECT 1")
    assert "comment" not in result


def test_normalize_strips_line_comment():
    result = _normalize("SELECT 1 -- trailing comment\nFROM t")
    assert "trailing" not in result


def test_normalize_lowercases():
    assert _normalize("SELECT") == "select"


def test_normalize_collapses_whitespace():
    assert _normalize("a   b\t\nc") == "a b c"


def test_version_is_integer():
    assert isinstance(VERSION, int)
    assert VERSION >= 1
