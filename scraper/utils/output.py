import json, os
from datetime import datetime, timezone

DEFAULT_PATH = "app/firms_data.js"


def write_firms_data_js(firms: list, path: str = DEFAULT_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "meta": {
            "lastScraped": datetime.now(timezone.utc).isoformat(),
            "totalFirms": len(firms)
        },
        "firms": firms
    }
    js = "const FIRMS_DATA = " + json.dumps(payload, indent=2) + ";"
    with open(path, "w", encoding="utf-8") as f:
        f.write(js)
