import os
import uuid
import pytest
from scraper.utils.normalize import (
    normalize_firm_name, normalize_practice_area, are_same_firm,
    deduplicate_firms, _pass1_exact_match, _pass2_fuzzy_match,
    _pass3_domain_phone_dedup, _merge_firm_records, _get_base_domain,
)


def _make_firm(name="Test Firm", city="Wichita", phone=None, website=None,
               email=None, practice_areas=None, coordinates=None,
               sources=None, street="", zip_=""):
    return {
        "id": str(uuid.uuid4()),
        "name": name,
        "practiceAreas": practice_areas or [],
        "summary": None,
        "website": website,
        "phone": phone,
        "email": email,
        "address": {"street": street, "city": city, "county": "", "state": "KS", "zip": zip_},
        "coordinates": coordinates,
        "referralScore": "low",
        "sources": sources or ["test"],
    }


class TestNormalizeFirmName:
    def test_lowercase(self):
        assert normalize_firm_name("SMITH LAW FIRM") == "smith"

    def test_strips_llc(self):
        assert normalize_firm_name("Smith & Associates LLC") == "smith associates"

    def test_strips_llp(self):
        assert normalize_firm_name("Johnson LLP") == "johnson"

    def test_strips_pc(self):
        assert normalize_firm_name("Williams Law PC") == "williams"

    def test_strips_law_firm(self):
        assert normalize_firm_name("Davis Law Firm") == "davis"

    def test_strips_law_office(self):
        assert normalize_firm_name("Miller Law Office") == "miller"

    def test_strips_attorneys_at_law(self):
        assert normalize_firm_name("Jones Attorneys at Law") == "jones"

    def test_strips_punctuation(self):
        assert normalize_firm_name("Brown & Green, P.A.") == "brown green"

    def test_strips_and_variations(self):
        assert normalize_firm_name("White and Black Associates") == "white black associates"

    def test_strips_pllc(self):
        assert normalize_firm_name("Taylor PLLC") == "taylor"


class TestAreSameFirm:
    def test_exact_match(self):
        assert are_same_firm("Smith & Associates LLC", "Smith and Associates") is True

    def test_different_firms(self):
        assert are_same_firm("Johnson Law Group", "Williams Law Firm") is False

    def test_partial_overlap_not_same(self):
        assert are_same_firm("Kansas City Law Group", "Lawrence Law Group") is False

    def test_slight_typo(self):
        assert are_same_firm("Smithe & Associates", "Smith & Associates") is True


class TestNormalizePracticeArea:
    def test_exact_match(self):
        assert normalize_practice_area("Personal Injury") == "Personal Injury"

    def test_case_insensitive(self):
        assert normalize_practice_area("personal injury") == "Personal Injury"

    def test_fuzzy_workers_comp(self):
        assert normalize_practice_area("Workers Comp") == "Workers' Compensation"

    def test_fuzzy_workers_compensation(self):
        assert normalize_practice_area("Workers' Compensation Law") == "Workers' Compensation"

    def test_unknown_returns_title_case(self):
        result = normalize_practice_area("Exotic Niche Law")
        assert result == "Exotic Niche Law"


class TestGetBaseDomain:
    def test_strips_www(self):
        assert _get_base_domain("https://www.example.com/page") == "example.com"

    def test_no_www(self):
        assert _get_base_domain("https://example.com") == "example.com"

    def test_none_returns_empty(self):
        assert _get_base_domain(None) == ""

    def test_empty_returns_empty(self):
        assert _get_base_domain("") == ""


class TestMergeFirmRecords:
    def test_merges_practice_areas(self):
        a = _make_firm(practice_areas=["Family Law"])
        b = _make_firm(practice_areas=["Criminal Defense", "Family Law"])
        result = _merge_firm_records(a, b)
        assert set(result["practiceAreas"]) == {"Family Law", "Criminal Defense"}

    def test_fills_missing_phone(self):
        a = _make_firm(phone=None)
        b = _make_firm(phone="316-555-0100")
        result = _merge_firm_records(a, b)
        assert result["phone"] == "316-555-0100"

    def test_higher_source_wins_phone(self):
        a = _make_firm(phone="111-111-1111", sources=["ksbar"])
        b = _make_firm(phone="222-222-2222", sources=["google_places"])
        result = _merge_firm_records(a, b)
        assert result["phone"] == "222-222-2222"

    def test_fills_missing_email(self):
        a = _make_firm(email=None)
        b = _make_firm(email="test@example.com")
        result = _merge_firm_records(a, b)
        assert result["email"] == "test@example.com"

    def test_fills_missing_coordinates(self):
        a = _make_firm(coordinates=None)
        b = _make_firm(coordinates={"lat": 37.6, "lng": -97.3})
        result = _merge_firm_records(a, b)
        assert result["coordinates"] == {"lat": 37.6, "lng": -97.3}

    def test_merges_sources(self):
        a = _make_firm(sources=["ks_courts"])
        b = _make_firm(sources=["google_places"])
        result = _merge_firm_records(a, b)
        assert set(result["sources"]) == {"ks_courts", "google_places"}

    def test_fills_missing_street(self):
        a = _make_firm(street="")
        b = _make_firm(street="123 Main St")
        result = _merge_firm_records(a, b)
        assert result["address"]["street"] == "123 Main St"


class TestPass1ExactMatch:
    def test_merges_exact_name_same_city(self):
        firms = [
            _make_firm("Smith Law LLC", "Wichita", phone="111"),
            _make_firm("Smith Law LLC", "Wichita", phone="222"),
        ]
        result = _pass1_exact_match(firms)
        assert len(result) == 1

    def test_different_cities_stay_separate(self):
        firms = [
            _make_firm("Smith Law LLC", "Wichita"),
            _make_firm("Smith Law LLC", "Topeka"),
        ]
        result = _pass1_exact_match(firms)
        assert len(result) == 2

    def test_normalization_collapses_variants(self):
        firms = [
            _make_firm("Smith & Associates LLC", "Wichita"),
            _make_firm("Smith and Associates", "Wichita"),
        ]
        result = _pass1_exact_match(firms)
        assert len(result) == 1


class TestPass2FuzzyMatch:
    def test_fuzzy_names_merge(self):
        firms = [
            _make_firm("Smith and Associates", "Wichita"),
            _make_firm("Smith Associates LLC", "Wichita"),
        ]
        result = _pass2_fuzzy_match(firms)
        assert len(result) == 1

    def test_different_firms_stay_separate(self):
        firms = [
            _make_firm("Smith Law", "Wichita"),
            _make_firm("Johnson Law", "Wichita"),
        ]
        result = _pass2_fuzzy_match(firms)
        assert len(result) == 2

    def test_transitive_merge(self):
        # A matches B, B matches C, so all three merge
        firms = [
            _make_firm("Smith Associates", "Wichita"),
            _make_firm("Smith & Associates", "Wichita"),
            _make_firm("Smith and Associates LLC", "Wichita"),
        ]
        result = _pass2_fuzzy_match(firms)
        assert len(result) == 1


class TestPass3DomainPhoneDedup:
    def test_same_domain_merges(self):
        firms = [
            _make_firm("Firm A", "Wichita", website="https://www.smithlaw.com"),
            _make_firm("Firm B", "Topeka", website="https://smithlaw.com/about"),
        ]
        result = _pass3_domain_phone_dedup(firms)
        assert len(result) == 1

    def test_same_phone_merges(self):
        firms = [
            _make_firm("Firm A", "Wichita", phone="316-555-0100"),
            _make_firm("Firm B", "Wichita", phone="316-555-0100"),
        ]
        result = _pass3_domain_phone_dedup(firms)
        assert len(result) == 1

    def test_no_overlap_stays_separate(self):
        firms = [
            _make_firm("Firm A", phone="111", website="https://a.com"),
            _make_firm("Firm B", phone="222", website="https://b.com"),
        ]
        result = _pass3_domain_phone_dedup(firms)
        assert len(result) == 2


class TestDeduplicateFirms:
    def test_full_pipeline(self, tmp_path):
        log = str(tmp_path / "dupes.log")
        firms = [
            _make_firm("Smith & Associates LLC", "Wichita", phone="316-555-0100",
                       practice_areas=["Family Law"], sources=["ks_courts"]),
            _make_firm("Smith Associates", "Wichita", phone=None,
                       practice_areas=["Criminal Defense"], sources=["google_places"]),
            _make_firm("Totally Different Firm", "Topeka", phone="785-555-0200",
                       sources=["ks_courts"]),
            _make_firm("Another Firm", "Wichita", phone="316-555-0100",
                       website="https://www.smithlaw.com", sources=["ksbar"]),
            _make_firm("Yet Another", "Lawrence",
                       website="https://www.smithlaw.com", sources=["ks_courts"]),
        ]
        result = deduplicate_firms(firms, log_path=log)
        # Smith variants (exact/fuzzy) + Another (phone) + Yet Another (domain) -> 1
        # Totally Different -> 1
        # Total: 2
        assert len(result) == 2
        assert os.path.exists(log)

    def test_no_duplicates_unchanged(self, tmp_path):
        log = str(tmp_path / "dupes.log")
        firms = [
            _make_firm("Firm A", "Wichita"),
            _make_firm("Firm B", "Topeka"),
            _make_firm("Firm C", "Lawrence"),
        ]
        result = deduplicate_firms(firms, log_path=log)
        assert len(result) == 3
