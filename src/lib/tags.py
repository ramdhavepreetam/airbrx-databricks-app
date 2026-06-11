"""
Parse the /* abx v=1 ck=... rule=... route=... */ leading comment injected by the gateway.
"""
import re
from typing import Optional

ABX = re.compile(r"^/\*\s*abx\s+(?P<kv>[^*]+)\*/", re.DOTALL)


def parse_tag(sql: str) -> Optional[dict]:
    """Return a dict of k=v pairs from the abx tag, or None if not present."""
    m = ABX.match(sql.lstrip())
    if not m:
        return None
    try:
        return dict(kv.split("=", 1) for kv in m.group("kv").split())
    except ValueError:
        return None


def has_tag(sql: str) -> bool:
    return ABX.match(sql.lstrip()) is not None
