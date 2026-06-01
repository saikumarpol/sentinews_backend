import time
from typing import Any, Dict, Optional

class SimpleCache:
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        entry = self._data.get(key)
        if not entry:
            return None
        if time.time() > entry["expires"]:
            del self._data[key]
            return None
        return entry["value"]

    def set(self, key: str, value: Any, ttl: int = 300):
        self._data[key] = {
            "value": value,
            "expires": time.time() + ttl
        }

cache = SimpleCache()
