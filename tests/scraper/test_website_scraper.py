from unittest.mock import patch, Mock
from scraper.phases.website_scraper import scrape_firm_website

def _mock_response(html: str, status=200):
    r = Mock()
    r.status_code = status
    r.text = html
    r.raise_for_status = Mock()
    return r

def test_extracts_meta_description():
    html = '<html><head><meta name="description" content="Top PI firm in Wichita."></head><body></body></html>'
    with patch("scraper.phases.website_scraper.requests.get", return_value=_mock_response(html)):
        result = scrape_firm_website("https://example.com", "Example Firm", "Wichita")
    assert result["summary"] == "Top PI firm in Wichita."

def test_falls_back_to_og_description():
    html = '<html><head><meta property="og:description" content="OG summary."></head><body></body></html>'
    with patch("scraper.phases.website_scraper.requests.get", return_value=_mock_response(html)):
        result = scrape_firm_website("https://example.com", "X", "Y")
    assert result["summary"] == "OG summary."

def test_falls_back_to_paragraph():
    html = '<html><body><main><p>This is a long enough paragraph about our law firm services in Kansas.</p></main></body></html>'
    with patch("scraper.phases.website_scraper.requests.get", return_value=_mock_response(html)):
        result = scrape_firm_website("https://example.com", "X", "Y")
    assert "paragraph" in result["summary"]

def test_fallback_summary_when_no_content():
    html = '<html><body></body></html>'
    with patch("scraper.phases.website_scraper.requests.get", return_value=_mock_response(html)):
        result = scrape_firm_website("https://example.com", "Best Law", "Topeka")
    assert result["summary"] == "Best Law — law firm in Topeka, Kansas"

def test_extracts_email_from_mailto():
    html = '<html><body><a href="mailto:contact@bestlaw.com">Email us</a></body></html>'
    with patch("scraper.phases.website_scraper.requests.get", return_value=_mock_response(html)):
        result = scrape_firm_website("https://example.com", "X", "Y")
    assert result["email"] == "contact@bestlaw.com"

def test_skips_noreply_email():
    html = '<html><body><a href="mailto:noreply@bestlaw.com">x</a></body></html>'
    with patch("scraper.phases.website_scraper.requests.get", return_value=_mock_response(html)):
        result = scrape_firm_website("https://example.com", "X", "Y")
    assert result["email"] is None

def test_returns_none_on_request_error():
    with patch("scraper.phases.website_scraper.requests.get", side_effect=Exception("timeout")):
        result = scrape_firm_website("https://example.com", "X", "Y")
    assert result["summary"] is not None  # fallback used
    assert result["email"] is None
