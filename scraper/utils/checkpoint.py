import json
import os

DEFAULT_PATH = "data/checkpoint.json"


def save_checkpoint(firms: list, phase: int, path: str = DEFAULT_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({"phase": phase, "firms": firms}, f)


def load_checkpoint(path: str = DEFAULT_PATH) -> dict | None:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def clear_checkpoint(path: str = DEFAULT_PATH) -> None:
    if os.path.exists(path):
        os.remove(path)
