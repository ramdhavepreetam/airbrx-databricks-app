"""
Canonical query fingerprint — SHARED CONTRACT between gateway and app.
Version must match on both sides. Bump VERSION and v= field together.
"""
import re
import hashlib
import base64

VERSION = 1
_TRUNCATE_LEN = 16

_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\n]*")
_WHITESPACE = re.compile(r"\s+")
_STRING_LIT = re.compile(r"'(?:[^'\\]|\\.)*'")
_NUM_LIT = re.compile(r"\b\d+(?:\.\d+)?\b")


def _normalize(sql: str) -> str:
    sql = _BLOCK_COMMENT.sub("", sql)
    sql = _LINE_COMMENT.sub("", sql)
    sql = sql.lower()
    sql = _WHITESPACE.sub(" ", sql).strip()
    sql = _STRING_LIT.sub("?", sql)
    sql = _NUM_LIT.sub("?", sql)
    return sql


def fingerprint(sql: str) -> str:
    """Return a short base32 hash of the normalized SQL. Strips abx tags before hashing."""
    normalized = _normalize(sql)
    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    return base64.b32encode(digest).decode("ascii").lower()[:_TRUNCATE_LEN]


def fingerprint_with_version(sql: str) -> tuple[str, int]:
    return fingerprint(sql), VERSION
