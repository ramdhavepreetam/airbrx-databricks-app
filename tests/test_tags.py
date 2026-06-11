"""
Tests for the abx comment-tag parser (§6b).
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from lib.tags import parse_tag, has_tag


# ---------------------------------------------------------------------------
# parse_tag
# ---------------------------------------------------------------------------

def test_parse_tag_full():
    sql = "/* abx v=1 ck=abc123xyz rule=r42 route=warehouse */ SELECT 1"
    tag = parse_tag(sql)
    assert tag is not None
    assert tag["v"] == "1"
    assert tag["ck"] == "abc123xyz"
    assert tag["rule"] == "r42"
    assert tag["route"] == "warehouse"


def test_parse_tag_cache_miss_route():
    sql = "/* abx v=1 ck=aabbcc rule=r1 route=cache_miss */ SELECT * FROM t"
    tag = parse_tag(sql)
    assert tag["route"] == "cache_miss"


def test_parse_tag_smaller_wh_route():
    sql = "/* abx v=1 ck=xyz route=smaller_wh rule=r5 */ SELECT 1"
    tag = parse_tag(sql)
    assert tag["route"] == "smaller_wh"


def test_parse_tag_returns_none_without_tag():
    sql = "SELECT id FROM users WHERE id = 1"
    assert parse_tag(sql) is None


def test_parse_tag_returns_none_for_other_comment():
    sql = "/* dbt model: my_model */ SELECT 1"
    assert parse_tag(sql) is None


def test_parse_tag_leading_whitespace_ignored():
    sql = "  \n  /* abx v=1 ck=aaa route=warehouse rule=r1 */ SELECT 1"
    tag = parse_tag(sql)
    assert tag is not None
    assert tag["ck"] == "aaa"


def test_parse_tag_preserved_dbt_comment():
    # Gateway should prepend abx block and preserve existing comment
    sql = "/* abx v=1 ck=bbb route=warehouse rule=r2 */ /* dbt model */ SELECT 1"
    tag = parse_tag(sql)
    assert tag is not None
    assert tag["ck"] == "bbb"


def test_parse_tag_does_not_crash_on_empty_string():
    assert parse_tag("") is None


def test_parse_tag_does_not_crash_on_malformed_tag():
    sql = "/* abx */ SELECT 1"
    result = parse_tag(sql)
    # Empty kv is fine — returns empty dict or None, not an exception
    assert result is None or isinstance(result, dict)


# ---------------------------------------------------------------------------
# has_tag
# ---------------------------------------------------------------------------

def test_has_tag_true():
    assert has_tag("/* abx v=1 ck=x route=warehouse rule=r1 */ SELECT 1") is True


def test_has_tag_false():
    assert has_tag("SELECT 1") is False


def test_has_tag_false_for_other_comment():
    assert has_tag("/* some other comment */ SELECT 1") is False
