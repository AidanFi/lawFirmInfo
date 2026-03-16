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
