import json
import os

DEFAULT_PATH = "data/checkpoint.json"


def save_checkpoint(firms: list, phase: int, path: str = DEFAULT_PATH, progress: dict = None) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = {"phase": phase, "firms": firms}
    if progress:
        data["progress"] = progress
    with open(path, "w") as f:
        json.dump(data, f)


def load_checkpoint(path: str = DEFAULT_PATH) -> dict | None:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        data = json.load(f)
    if "progress" not in data:
        data["progress"] = {}
    return data


def clear_checkpoint(path: str = DEFAULT_PATH) -> None:
    if os.path.exists(path):
        os.remove(path)
