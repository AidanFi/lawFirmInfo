#!/usr/bin/env python3
"""
State Bar of Texas Member Directory discovery — county-level.

The State Bar's public "Find a Lawyer" directory (texasbar.com) is a public
service under Texas Government Code Section 81.115 and is searchable by
county. It is the Texas equivalent of the KS Courts registry: the true
completeness ceiling of licensed attorneys with a primary practice address
in a given county.

Search is a stateless POST (no session/cookies required) to:
  Result_form_client.cfm  with fields: Submitted=1, Find=1, County=<id>, Start=<n>
25 results per page. Each card gives: attorney name, firm/company (h5, often
blank), street/city/state/zip, phone (tel: link), status icon, ContactID.

Status icons (legend on the search-results page):
  green circle  = Eligible to practice   <- ONLY status we keep
  red square    = Not Eligible to practice
  yellow club   = Non-Practicing
  aqua diamond  = Inactive
  blue triangle = Deceased

Usage: python3 texasbar_discover.py <county-slug>
"""
import csv
import json
import re
import sys
import time
from pathlib import Path

import requests

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")

SEARCH_URL = (
    "https://www.texasbar.com/AM/Template.cfm?Section=Find_A_Lawyer"
    "&Template=/CustomSource/MemberDirectory/Result_form_client.cfm"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

# County name -> list of "County" select option ids on the search form.
# Some counties (observed: HARRIS) appear twice in the State Bar's dropdown
# under two different ids — both must be queried to get full coverage.
TEXAS_COUNTIES = {
    "harris-county-tx": {
        "name": "Harris",
        "state": "TX",
        "msa": "Houston",
        "county_ids": [101, 102],
    },
    "dallas-county-tx": {
        "name": "Dallas",
        "state": "TX",
        "msa": "Dallas-Fort Worth",
        "county_ids": [57],
    },
    "tarrant-county-tx": {
        "name": "Tarrant",
        "state": "TX",
        "msa": "Dallas-Fort Worth",
        "county_ids": [220],
    },
    "bexar-county-tx": {
        "name": "Bexar",
        "state": "TX",
        "msa": "San Antonio",
        "county_ids": [15],
    },
    "travis-county-tx": {
        "name": "Travis",
        "state": "TX",
        "msa": "Austin",
        "county_ids": [227],
    },
}

ARTICLE_RE = re.compile(r'<article class="lawyer">.*?</article>', re.S)
GIVEN_RE = re.compile(r'given-name">([^<]*)</span>')
FAMILY_RE = re.compile(r'family-name">([^<]*)</span>')
H5_RE = re.compile(r'<h5>([^<]*)</h5>')
ADDR_RE = re.compile(r'class="address">(.*?)</p>', re.S)
PHONE_RE = re.compile(r'tel:([\d\-]+)')
STATUS_RE = re.compile(r'status-icon ([a-z]+ [a-z]+)"')
CONTACTID_RE = re.compile(r'ContactID=(\d+)')
PAGENUM_RE = re.compile(r'PageClick\((\d+),\s*25\)')


def parse_address(raw: str) -> tuple[str, str, str, str]:
    text = re.sub(r"<br\s*/?>", "|", raw)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    parts = [p.strip() for p in text.split("|") if p.strip()]
    if not parts:
        return "", "", "", ""
    loc = parts[-1]
    m = re.match(r"^(.+?),\s*([A-Z]{2})\s*(\d{5})?", loc)
    city, state, zipcode = "", "", ""
    if m:
        city = m.group(1).strip()
        state = m.group(2).strip()
        zipcode = (m.group(3) or "").strip()
    street = " ".join(parts[:-1]) if len(parts) > 1 else ""
    return street, city, state, zipcode


def parse_page(html: str) -> list[dict]:
    out = []
    for art in ARTICLE_RE.findall(html):
        fam = FAMILY_RE.search(art)
        if not fam:
            continue
        given_parts = [g.strip() for g in GIVEN_RE.findall(art) if g.strip()]
        name = " ".join(given_parts + [fam.group(1).strip()]).strip()
        if not name:
            continue

        h5 = H5_RE.search(art)
        company = h5.group(1).strip() if h5 else ""

        addr_m = ADDR_RE.search(art)
        street, city, state, zipcode = parse_address(addr_m.group(1)) if addr_m else ("", "", "", "")

        phone_m = PHONE_RE.search(art)
        phone = phone_m.group(1) if phone_m else ""

        status_m = STATUS_RE.search(art)
        status = status_m.group(1) if status_m else ""

        cid_m = CONTACTID_RE.search(art)
        contact_id = cid_m.group(1) if cid_m else ""

        out.append({
            "name": name,
            "company": company,
            "street": street,
            "city": city,
            "state": state,
            "zip": zipcode,
            "phone": phone,
            "status": status,
            "contact_id": contact_id,
        })
    return out


def fetch_county_id(session: requests.Session, county_id: int, delay: float = 0.35) -> list[dict]:
    results = []
    start = 1
    max_page = None
    page = 0
    while True:
        page += 1
        try:
            r = session.post(
                SEARCH_URL,
                data={"Submitted": "1", "Find": "1", "County": str(county_id), "Start": str(start)},
                timeout=30,
            )
        except Exception as e:
            print(f"  [county_id={county_id}] error at start={start}: {e}, retrying in 15s")
            time.sleep(15)
            continue

        if r.status_code == 429:
            print(f"  [county_id={county_id}] rate limited, waiting 60s")
            time.sleep(60)
            continue
        if r.status_code != 200:
            print(f"  [county_id={county_id}] HTTP {r.status_code} at start={start}, stopping")
            break

        entries = parse_page(r.text)
        if not entries:
            break
        results.extend(entries)

        if max_page is None:
            page_nums = [int(n) for n in PAGENUM_RE.findall(r.text)]
            max_page = max(page_nums) if page_nums else page
            print(f"  [county_id={county_id}] {max_page} pages total ({max_page * 25} attorneys, approx)")

        if page >= max_page:
            break
        start += 25
        time.sleep(delay)

        if page % 40 == 0:
            print(f"  [county_id={county_id}] progress: page {page}/{max_page}, {len(results)} attorneys so far")

    print(f"  [county_id={county_id}] done: {len(results)} attorneys")
    return results


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in TEXAS_COUNTIES:
        print(f"Usage: python3 texasbar_discover.py <slug>  (one of {list(TEXAS_COUNTIES)})")
        sys.exit(1)

    slug = sys.argv[1]
    info = TEXAS_COUNTIES[slug]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{slug}_statebar_cache.json"

    session = requests.Session()
    session.headers.update(HEADERS)

    all_entries = []
    for cid in info["county_ids"]:
        print(f"\n=== {slug}: County id {cid} ===")
        all_entries.extend(fetch_county_id(session, cid))

    cache_path.write_text(json.dumps(all_entries, indent=1))
    print(f"\nWrote {len(all_entries)} raw attorney records to {cache_path}")


if __name__ == "__main__":
    main()
