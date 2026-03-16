from unittest.mock import MagicMock, patch
from scraper.phases.google_places import scrape_google_places, KANSAS_CITIES

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
