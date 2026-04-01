"""Integration tests for the full scraper pipeline."""
import json
import os
import uuid
import pytest
from unittest.mock import patch, MagicMock
from scraper.utils.normalize import deduplicate_firms
from scraper.utils.checkpoint import save_checkpoint, load_checkpoint


def _make_firm(name, city, phone=None, website=None, email=None,
               practice_areas=None, coordinates=None, sources=None, street=""):
    return {
        "id": str(uuid.uuid4()),
        "name": name,
        "practiceAreas": practice_areas or [],
        "summary": None,
        "website": website,
        "phone": phone,
        "email": email,
        "address": {"street": street, "city": city, "county": "", "state": "KS", "zip": ""},
        "coordinates": coordinates,
        "referralScore": "low",
        "sources": sources or ["test"],
    }


class TestDedupPipeline:
    """Test multi-source dedup scenarios that would occur in a real run."""

    def test_ks_courts_plus_google_dedup(self, tmp_path):
        """Firms from KS Courts and Google Places for same firm should merge."""
        log = str(tmp_path / "dupes.log")
        firms = [
            # From KS Courts
            _make_firm("Fleeson Gooing Coulson & Kitch LLC", "Wichita",
                       phone="316-267-7361", sources=["ks_courts"],
                       street="1900 Epic Center"),
            # From Google Places (slightly different name)
            _make_firm("Fleeson, Gooing, Coulson & Kitch", "Wichita",
                       phone="316-267-7361", sources=["google_places"],
                       website="https://www.fleeson.com",
                       coordinates={"lat": 37.6, "lng": -97.3}),
        ]
        result = deduplicate_firms(firms, log_path=log)
        assert len(result) == 1
        # Should have merged data from both sources
        firm = result[0]
        assert "ks_courts" in firm["sources"]
        assert "google_places" in firm["sources"]
        assert firm["coordinates"] is not None
        assert firm["website"] == "https://www.fleeson.com"

    def test_multi_source_merge(self, tmp_path):
        """Firm data from 3+ sources merges correctly."""
        log = str(tmp_path / "dupes.log")
        firms = [
            _make_firm("Foulston Siefkin LLP", "Wichita",
                       sources=["ks_courts"], phone="316-267-6371"),
            _make_firm("Foulston Siefkin", "Wichita",
                       sources=["google_places"],
                       website="https://www.foulston.com",
                       coordinates={"lat": 37.68, "lng": -97.33}),
            _make_firm("Foulston Siefkin LLP", "Wichita",
                       sources=["ksbar"],
                       practice_areas=["Business Law", "Employment Law"]),
        ]
        result = deduplicate_firms(firms, log_path=log)
        assert len(result) == 1
        firm = result[0]
        assert len(firm["sources"]) == 3
        assert firm["phone"] == "316-267-6371"
        assert firm["website"] == "https://www.foulston.com"
        assert "Business Law" in firm["practiceAreas"]

    def test_dedup_stress_test(self, tmp_path):
        """10 overlapping firms should reduce to expected unique count."""
        log = str(tmp_path / "dupes.log")
        firms = [
            # Group 1: Same firm, different name variants (should merge to 1)
            _make_firm("Smith & Associates LLC", "Wichita", phone="316-555-0001"),
            _make_firm("Smith Associates", "Wichita", phone="316-555-0001"),
            _make_firm("Smith and Associates", "Wichita"),

            # Group 2: Same website domain (should merge to 1)
            _make_firm("Johnson Law Group", "Topeka", website="https://www.johnsonlaw.com"),
            _make_firm("Johnson Legal", "Lawrence", website="https://johnsonlaw.com/about"),

            # Group 3: Completely unique firms (stay as 3)
            _make_firm("Williams Law Office", "Dodge City"),
            _make_firm("Brown & Partners", "Manhattan"),
            _make_firm("Davis Legal Services", "Salina"),

            # Group 4: Same phone different name (should merge to 1)
            _make_firm("ABC Law", "Hays", phone="785-555-9999"),
            _make_firm("XYZ Legal", "Hays", phone="785-555-9999"),
        ]
        result = deduplicate_firms(firms, log_path=log)
        # Group 1: 3→1, Group 2: 2→1, Group 3: 3, Group 4: 2→1 = 6 total
        assert len(result) == 6

    def test_no_false_positives(self, tmp_path):
        """Different firms with somewhat similar names should NOT merge."""
        log = str(tmp_path / "dupes.log")
        firms = [
            _make_firm("Kansas Law Group", "Wichita"),
            _make_firm("Kansas Legal Services", "Wichita"),
            _make_firm("Wichita Law Firm", "Wichita"),
        ]
        result = deduplicate_firms(firms, log_path=log)
        assert len(result) == 3


class TestCheckpointResume:
    def test_checkpoint_preserves_firms(self, tmp_path):
        path = str(tmp_path / "cp.json")
        firms = [_make_firm("Test Firm", "Wichita")]
        save_checkpoint(firms, phase=3, path=path, progress={"website_last_idx": 50})
        loaded = load_checkpoint(path=path)
        assert loaded["phase"] == 3
        assert len(loaded["firms"]) == 1
        assert loaded["progress"]["website_last_idx"] == 50

    def test_checkpoint_round_trip_preserves_data(self, tmp_path):
        path = str(tmp_path / "cp.json")
        firm = _make_firm("Smith Law LLC", "Wichita", phone="316-555-0100",
                          website="https://smithlaw.com", email="info@smithlaw.com",
                          practice_areas=["Family Law", "Criminal Defense"],
                          coordinates={"lat": 37.6, "lng": -97.3},
                          sources=["ks_courts", "google_places"])
        save_checkpoint([firm], phase=4, path=path)
        loaded = load_checkpoint(path=path)
        loaded_firm = loaded["firms"][0]
        assert loaded_firm["name"] == "Smith Law LLC"
        assert loaded_firm["phone"] == "316-555-0100"
        assert loaded_firm["coordinates"]["lat"] == 37.6
        assert set(loaded_firm["sources"]) == {"ks_courts", "google_places"}
