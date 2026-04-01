import json
import os
import tempfile
import pytest
from scraper.utils.checkpoint import save_checkpoint, load_checkpoint, clear_checkpoint


@pytest.fixture
def tmp_path_checkpoint(tmp_path):
    return tmp_path / "checkpoint.json"


def test_save_and_load(tmp_path_checkpoint):
    firms = [{"id": "1", "name": "Smith Law"}]
    save_checkpoint(firms, phase=1, path=str(tmp_path_checkpoint))
    data = load_checkpoint(path=str(tmp_path_checkpoint))
    assert data["firms"] == firms
    assert data["phase"] == 1


def test_load_returns_none_when_missing(tmp_path):
    path = str(tmp_path / "no_file.json")
    assert load_checkpoint(path=path) is None


def test_clear_deletes_file(tmp_path_checkpoint):
    save_checkpoint([], phase=1, path=str(tmp_path_checkpoint))
    clear_checkpoint(path=str(tmp_path_checkpoint))
    assert not tmp_path_checkpoint.exists()


def test_clear_is_safe_when_missing(tmp_path):
    clear_checkpoint(path=str(tmp_path / "ghost.json"))  # should not raise


def test_save_and_load_with_progress(tmp_path_checkpoint):
    firms = [{"id": "1", "name": "Smith Law"}]
    progress = {"ks_courts_last_reg": 15000, "justia_last_city_idx": 42}
    save_checkpoint(firms, phase=3, path=str(tmp_path_checkpoint), progress=progress)
    data = load_checkpoint(path=str(tmp_path_checkpoint))
    assert data["firms"] == firms
    assert data["phase"] == 3
    assert data["progress"] == progress


def test_load_old_format_gets_empty_progress(tmp_path_checkpoint):
    """Old checkpoints without progress key should return progress: {}."""
    with open(str(tmp_path_checkpoint), "w") as f:
        json.dump({"phase": 2, "firms": [{"id": "1"}]}, f)
    data = load_checkpoint(path=str(tmp_path_checkpoint))
    assert data["progress"] == {}
    assert data["phase"] == 2


def test_save_without_progress_omits_key(tmp_path_checkpoint):
    save_checkpoint([], phase=1, path=str(tmp_path_checkpoint))
    with open(str(tmp_path_checkpoint)) as f:
        raw = json.load(f)
    assert "progress" not in raw
