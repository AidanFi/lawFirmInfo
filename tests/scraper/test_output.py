import json, re
from scraper.utils.output import write_firms_data_js

def test_writes_valid_js(tmp_path):
    out = tmp_path / "firms_data.js"
    firms = [{"id": "1", "name": "Test Firm"}]
    write_firms_data_js(firms, path=str(out))
    content = out.read_text()
    assert content.startswith("const FIRMS_DATA =")
    assert content.endswith(";")

def test_meta_fields_present(tmp_path):
    out = tmp_path / "firms_data.js"
    write_firms_data_js([{"id": "1"}], path=str(out))
    content = out.read_text()
    # Strip JS assignment wrapper to parse JSON
    json_str = re.sub(r'^const FIRMS_DATA\s*=\s*', '', content).strip().rstrip(';').strip()
    data = json.loads(json_str)
    assert "lastScraped" in data["meta"]
    assert "totalFirms" in data["meta"]
    assert data["meta"]["totalFirms"] == 1

def test_firms_array_matches_input(tmp_path):
    out = tmp_path / "firms_data.js"
    firms = [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]
    write_firms_data_js(firms, path=str(out))
    json_str = re.sub(r'^const FIRMS_DATA\s*=\s*', '', out.read_text()).strip().rstrip(';').strip()
    data = json.loads(json_str)
    assert len(data["firms"]) == 2
