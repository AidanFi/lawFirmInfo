from unittest.mock import patch, MagicMock
from scraper.utils.geocode import geocode_firms


def _make_location(lat, lng):
    """Return a mock geopy Location object."""
    loc = MagicMock()
    loc.latitude = lat
    loc.longitude = lng
    return loc


def _make_firm(city="Wichita", street="", state="KS", zipcode="67202", coordinates=None):
    return {
        "address": {"street": street, "city": city, "county": "", "state": state, "zip": zipcode},
        "coordinates": coordinates,
    }


@patch("scraper.utils.geocode.Nominatim")
def test_full_address_geocode_succeeds(mock_nominatim_cls):
    mock_geocoder = MagicMock()
    mock_nominatim_cls.return_value = mock_geocoder
    mock_geocoder.geocode.return_value = _make_location(37.69, -97.33)

    firms = [_make_firm(street="100 Main St")]
    result = geocode_firms(firms, delay=0)

    assert result is firms
    assert firms[0]["coordinates"] == {"lat": 37.69, "lng": -97.33}


@patch("scraper.utils.geocode.Nominatim")
def test_full_address_fails_city_fallback_succeeds(mock_nominatim_cls):
    mock_geocoder = MagicMock()
    mock_nominatim_cls.return_value = mock_geocoder

    # City geocode succeeds in phase 1, then full address fails in phase 2
    city_loc = _make_location(37.68, -97.34)
    mock_geocoder.geocode.side_effect = [city_loc, None]

    firms = [_make_firm(street="999 Nonexistent Rd")]
    geocode_firms(firms, delay=0)

    assert firms[0]["coordinates"] == {"lat": 37.68, "lng": -97.34}


@patch("scraper.utils.geocode.Nominatim")
def test_both_fail_coordinates_remain_none(mock_nominatim_cls):
    mock_geocoder = MagicMock()
    mock_nominatim_cls.return_value = mock_geocoder
    mock_geocoder.geocode.return_value = None

    firms = [_make_firm()]
    geocode_firms(firms, delay=0)

    assert firms[0]["coordinates"] is None


@patch("scraper.utils.geocode.Nominatim")
def test_city_cache_avoids_redundant_calls(mock_nominatim_cls):
    mock_geocoder = MagicMock()
    mock_nominatim_cls.return_value = mock_geocoder

    city_loc = _make_location(37.68, -97.34)
    # Phase 1: one city geocode call (Topeka). Phase 2: no street, so uses cache only.
    mock_geocoder.geocode.side_effect = [city_loc]

    firms = [_make_firm(city="Topeka"), _make_firm(city="Topeka")]
    geocode_firms(firms, delay=0)

    assert firms[0]["coordinates"] == {"lat": 37.68, "lng": -97.34}
    assert firms[1]["coordinates"] == {"lat": 37.68, "lng": -97.34}
    # Only 1 geocode call (city in phase 1), firms use cache in phase 2
    assert mock_geocoder.geocode.call_count == 1


@patch("scraper.utils.geocode.Nominatim")
def test_firms_with_coordinates_are_skipped(mock_nominatim_cls):
    mock_geocoder = MagicMock()
    mock_nominatim_cls.return_value = mock_geocoder

    firms = [_make_firm(coordinates={"lat": 39.0, "lng": -95.0})]
    geocode_firms(firms, delay=0)

    mock_geocoder.geocode.assert_not_called()
    assert firms[0]["coordinates"] == {"lat": 39.0, "lng": -95.0}


@patch("scraper.utils.geocode.Nominatim")
def test_firms_with_no_city_are_skipped(mock_nominatim_cls):
    mock_geocoder = MagicMock()
    mock_nominatim_cls.return_value = mock_geocoder

    firms = [_make_firm(city="")]
    geocode_firms(firms, delay=0)

    mock_geocoder.geocode.assert_not_called()
    assert firms[0]["coordinates"] is None


@patch("scraper.utils.geocode.Nominatim")
def test_progress_printing(mock_nominatim_cls, capsys):
    mock_geocoder = MagicMock()
    mock_nominatim_cls.return_value = mock_geocoder
    mock_geocoder.geocode.return_value = _make_location(37.0, -97.0)

    # 101 firms so the progress message fires
    firms = [_make_firm() for _ in range(101)]
    geocode_firms(firms, delay=0)

    captured = capsys.readouterr()
    assert "Assigned coordinates to 100 firms" in captured.out
