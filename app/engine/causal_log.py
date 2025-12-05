import json
from datetime import datetime
from pathlib import Path
from typing import Dict

LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
CAUSAL_LOG_PATH = LOGS_DIR / "causal_memory.log"


def log_causal_event(event_type: str, details: Dict) -> None:
    event = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "event_type": event_type,
        **details,
    }
    with open(CAUSAL_LOG_PATH, "a") as f:
        f.write(json.dumps(event) + "\n")
