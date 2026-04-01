from unittest.mock import MagicMock, patch
from scraper.phases.google_places import (
    scrape_google_places, KANSAS_CITIES, COUNTY_SEATS_105,
    merge_google_into_firms,
)

def _make_place(name, address, phone, website, lat, lng):
    return {
        "place_id": "abc123",
        "name": name,
        "formatted_address": address,
        "geometry": {"location": {"lat": lat, "lng": lng}},
        "formatted_phone_number": phone,
        "website": website,
    }

def test_returns_list_of_firms():
    mock_client = MagicMock()
    mock_client.places.return_value = {
        "results": [_make_place("Smith Law", "123 Main, Wichita, KS 67202", "(316) 555-0100", "https://smith.com", 37.69, -97.34)]
    }
    mock_client.place.return_value = {"result": _make_place("Smith Law", "123 Main, Wichita, KS 67202", "(316) 555-0100", "https://smith.com", 37.69, -97.34)}
    firms = scrape_google_places(mock_client, cities=["Wichita"])
    assert len(firms) == 1
    assert firms[0]["name"] == "Smith Law"
    assert firms[0]["phone"] == "(316) 555-0100"
    assert firms[0]["website"] == "https://smith.com"

def test_deduplicates_same_firm():
    mock_client = MagicMock()
    duplicate = _make_place("Smith & Associates LLC", "123 Main, Wichita, KS 67202", "(316) 555-0100", "https://smith.com", 37.69, -97.34)
    mock_client.places.return_value = {"results": [duplicate, duplicate]}
    mock_client.place.return_value = {"result": duplicate}
    firms = scrape_google_places(mock_client, cities=["Wichita"])
    assert len(firms) == 1

def test_kansas_cities_list_has_entries():
    assert len(KANSAS_CITIES) >= 50

def test_extracts_city_from_address():
    mock_client = MagicMock()
    place = _make_place("X Law", "100 Oak St, Topeka, KS 66603", "(785) 555-0100", None, 39.05, -95.68)
    mock_client.places.return_value = {"results": [place]}
    mock_client.place.return_value = {"result": place}
    firms = scrape_google_places(mock_client, cities=["Topeka"])
    assert firms[0]["address"]["city"] == "Topeka"
    assert firms[0]["address"]["state"] == "KS"


# --------------- COUNTY_SEATS_105 tests ---------------

def test_county_seats_has_105_entries():
    assert len(COUNTY_SEATS_105) == 105


# --------------- merge_google_into_firms tests ---------------

def _make_existing_firm(name, city, phone=None, website=None, street="", zip_code=""):
    return {
        "id": "existing-1",
        "name": name,
        "practiceAreas": ["Family Law"],
        "summary": None,
        "website": website,
        "phone": phone,
        "email": None,
        "address": {"street": street, "city": city, "county": "", "state": "KS", "zip": zip_code},
        "coordinates": None,
        "referralScore": "medium",
        "sources": ["ks_courts"],
    }


def _make_google_firm(name, city, phone=None, website=None, lat=37.69, lng=-97.34, street="100 Main St", zip_code="67202"):
    return {
        "id": "google-1",
        "name": name,
        "practiceAreas": [],
        "summary": None,
        "website": website,
        "phone": phone,
        "email": None,
        "address": {"street": street, "city": city, "county": "", "state": "KS", "zip": zip_code},
        "coordinates": {"lat": lat, "lng": lng},
        "referralScore": "low",
        "sources": ["google_places"],
    }


def test_merge_enriches_existing_firm_with_coordinates():
    firms = [_make_existing_firm("Smith Law", "Wichita")]
    google_firms = [_make_google_firm("Smith Law LLC", "Wichita")]
    result = merge_google_into_firms(firms, google_firms)
    assert len(result) == 1
    assert result[0]["coordinates"] == {"lat": 37.69, "lng": -97.34}
    assert "google_places" in result[0]["sources"]
    assert "ks_courts" in result[0]["sources"]


def test_merge_adds_new_firm_when_no_match():
    firms = [_make_existing_firm("Smith Law", "Wichita")]
    google_firms = [_make_google_firm("Jones & Associates", "Topeka")]
    result = merge_google_into_firms(firms, google_firms)
    assert len(result) == 2
    assert result[1]["name"] == "Jones & Associates"


def test_merge_does_not_overwrite_existing_phone_with_none():
    firms = [_make_existing_firm("Smith Law", "Wichita", phone="(316) 555-0100")]
    google_firms = [_make_google_firm("Smith Law LLC", "Wichita", phone=None)]
    result = merge_google_into_firms(firms, google_firms)
    assert len(result) == 1
    assert result[0]["phone"] == "(316) 555-0100"
