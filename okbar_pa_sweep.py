#!/usr/bin/env python3
"""
Sweep OKBar by practice area × county to get past the 200-result cap.
Focuses on counties that hit the 200-result ceiling.
"""
import csv
import re
import sys
import time
import warnings
from pathlib import Path

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

DATA_DIR = Path("app/county-data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

START_URL = "https://ams.okbar.org/eweb/startpage.aspx?site=FALWEB"

# Practice areas most likely to yield diverse new attorneys
PRACTICE_AREAS = {
    'ece86f4a-38e9-4c7a-baad-5105c5ed7ca1': 'Criminal Defense',
    'fa60e988-d18f-454a-8b79-5d2a66c0d2d2': 'Family Law',
    '42bf5bee-4391-47d5-a345-c087d95218bb': 'Personal Injury',
    '43cfe3e1-62a3-4e5c-9dcd-d52f8d35e5f9': 'General Practice',
    '5549f3d8-2fe8-4433-ae2d-f8ec5f2a1724': 'Estate Planning and Probate',
    '3548eb74-ebcc-445f-bf95-eac1f3636132': 'Litigation',
    '7a74209d-8a6f-4611-a702-025c8adb6dc9': 'Business & Corporate Law',
    'e28b10db-031a-49d0-9f66-5c85ad83db06': 'Workers Compensation',
    '2e9b6750-2181-4fda-a3bf-4dba570dd64d': 'Employment Law',
    '03b5886f-5930-4880-86b4-d8edbd8f6193': 'Bankruptcy - Business',
    '7f535e77-313e-486c-a87e-857f045b20bf': 'Bankruptcy - Personal',
    '0bb49620-d66b-4200-9963-b802376de650': 'Real Property (Land)',
    '9228489f-4de3-488c-b010-af15b24393c2': 'Immigration',
    '3d36c3fe-7452-4c9b-a57c-c8d716bb320e': 'Medical Malpractice',
    '899e0533-2a87-4c42-8349-3c898450a3ce': 'Probate',
    'f1ef4137-dff3-447c-ba42-6f087afd8c00': 'Divorce',
    'eb3e0184-9629-4e1e-ab8a-e2805eee26a2': 'Trial Practice',
    '9b13aded-6199-4a17-819c-c9900af5981c': 'Government Practice',
    '40432bb1-383e-4d6f-bb4d-9fdd52e68c2d': 'Labor & Employment Law',
    '78e6464d-6d94-4c18-98a0-9dbe6c07dac1': 'Taxation',
}

# Counties that likely hit the 200 cap — sweep these
TARGET_COUNTIES = {
    'Oklahoma': 'oklahoma-county-ok',
    'Tulsa': 'tulsa-county-ok',
    'Canadian': 'canadian-county-ok',
    'Cleveland': 'cleveland-county-ok',
    'Creek': 'creek-county-ok',
    'Logan': 'logan-county-ok',
    'Rogers': 'rogers-county-ok',
    'Wagoner': 'wagoner-county-ok',
    'Mcclain': 'mcclain-county-ok',
}

VALID_OK_AREA = {"405", "918", "580", "800", "833", "844", "855", "866", "877", "888"}
_PHONE_RE = re.compile(r'\((\d{3})\)\s*(\d{3})[\-\.](\d{4})')

GENERIC_EMAIL = {
    'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com','icloud.com',
    'msn.com','live.com','me.com','comcast.net','att.net','sbcglobal.net',
    'verizon.net','cox.net','earthlink.net','protonmail.com','proton.me',
    'bellsouth.net','suddenlink.net','windstream.net','ok.gov','okbar.org',
    'feedthechildren.org','ywca.org','cherokee.org','chickasaw.net',
}


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _norm_domain(url):
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        return re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
    except Exception:
        return ""


def _clean_phone(raw):
    m = _PHONE_RE.search(raw)
    if not m:
        return ""
    area = m.group(1)
    if area not in VALID_OK_AREA:
        return ""
    return f"({area}) {m.group(2)}-{m.group(3)}"


def _parse_cards(soup):
    records = []
    row_fluid = soup.find("div", class_="row-fluid")
    if not row_fluid:
        return records
    cards = row_fluid.find_all("div", class_="span4", recursive=False)
    if not cards:
        cards = row_fluid.find_all("div", recursive=False)
    for card in cards:
        lines = [ln.strip() for ln in card.get_text("\n").split("\n") if ln.strip()]
        if not lines:
            continue
        name = lines[0]
        if len(name) < 4 or re.search(r"(browse|search|contact\s+us|view\s+profile)", name, re.I):
            continue
        phone = email = website = city = zip_code = ""
        address_lines = []
        for ln in lines[1:]:
            if re.match(r'\(\d{3}\)', ln):
                phone = _clean_phone(ln)
            elif "@" in ln and "." in ln and " " not in ln.strip():
                email = ln.strip()
            elif ln.startswith("http") or ln.startswith("www."):
                website = ln.strip() if ln.startswith("http") else f"http://{ln.strip()}"
            elif re.match(r"^.+,\s*OK\s+\d{5}", ln):
                m = re.match(r"^(.+),\s*OK\s+(\d{5})", ln)
                if m:
                    city = m.group(1).strip()
                    zip_code = m.group(2)
            elif re.match(r"^(Phone|Address)\s*:", ln, re.I):
                pass
            else:
                address_lines.append(ln)
        firm_name = name
        if address_lines:
            potential_firm = address_lines[0]
            if not re.match(r"^\d+\s+\w+|^PO\s+Box|^P\.O\.", potential_firm, re.I):
                firm_name = potential_firm
        if not website and email and "@" in email:
            domain = email.split("@")[-1].lower()
            if domain not in GENERIC_EMAIL:
                website = f"https://{domain}"
        records.append({
            "law_firm_name": firm_name, "attorney_name": name,
            "website": website, "phone": phone, "email": email,
            "city": city, "zip_code": zip_code,
        })
    return records


def _get_pagination_args(html):
    args = list(set(int(m) for m in re.findall(r"__doPostBack\('[^']+','(\d+)'\)", html)))
    args.sort()
    return args


def _load_existing(slug):
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(path.open()))
    names = {_norm(r["law_firm_name"]) for r in rows}
    phones = {r["phone_number"].strip() for r in rows if r.get("phone_number", "").strip()}
    domains = {_norm_domain(r["website"]) for r in rows if r.get("website", "").strip()}
    return rows, names, phones, domains


def scrape_pa_county(session, hidden, okbar_county, pa_uuid):
    payload = {
        **hidden,
        "C_2_2$ValueDropDownList0": pa_uuid,
        "C_2_2$ValueTextBox1": "",
        "C_2_2$ValueDropDownList2": okbar_county,
        "C_2_2$ValueDropDownList3": "OK",
        "C_2_2$ValueTextBox4": "",
        "C_2_2$ValueDropDownList5": "",
        "C_2_2$ValueDropDownList6": "",
        "C_2_2$ButtonFindGo": "Search",
    }
    r = session.post(START_URL, data=payload, timeout=30, verify=False)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    all_records = _parse_cards(soup)

    result_hidden = {inp["name"]: inp.get("value", "") for inp in soup.find_all("input", type="hidden")}
    page_args = _get_pagination_args(r.text)
    pg_target_match = re.search(r"__doPostBack\('([^']+)','(\d+)'\)", r.text)
    evt_target = pg_target_match.group(1) if pg_target_match else ""
    results_url = r.url

    for page_arg in page_args:
        time.sleep(1.5)
        pg_payload = {**result_hidden, "__EVENTTARGET": evt_target, "__EVENTARGUMENT": str(page_arg)}
        r2 = session.post(results_url, data=pg_payload, timeout=30, verify=False)
        if r2.status_code != 200:
            break
        s2 = BeautifulSoup(r2.text, "html.parser")
        cards = _parse_cards(s2)
        if not cards:
            break
        all_records.extend(cards)
        result_hidden = {inp["name"]: inp.get("value", "") for inp in s2.find_all("input", type="hidden")}
        new_tgt = re.search(r"__doPostBack\('([^']+)','(\d+)'\)", r2.text)
        if new_tgt:
            evt_target = new_tgt.group(1)

    return all_records


def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    grand_total_new = 0

    for okbar_county, slug in TARGET_COUNTIES.items():
        print(f"\n{'='*50}")
        print(f"{okbar_county} County sweep ({slug})")
        county_new = 0

        rows, existing_names, existing_phones, existing_domains = _load_existing(slug)
        fieldnames = list(rows[0].keys())

        for pa_uuid, pa_name in PRACTICE_AREAS.items():
            # Refresh session hidden fields
            try:
                r = session.get(START_URL, timeout=15, verify=False)
                hidden = {inp["name"]: inp.get("value", "") for inp in BeautifulSoup(r.text, "html.parser").find_all("input", type="hidden")}
            except Exception as e:
                print(f"  {pa_name}: session refresh failed — {e}")
                continue

            try:
                raw = scrape_pa_county(session, hidden, okbar_county, pa_uuid)
            except Exception as e:
                print(f"  {pa_name}: scrape failed — {e}")
                time.sleep(3)
                continue

            # Merge new records
            new_rows = []
            for rec in raw:
                name = rec["law_firm_name"].strip()
                phone = rec["phone"].strip()
                website = rec["website"].strip()
                email = rec["email"].strip()

                if not name or len(name) < 4:
                    continue
                nkey = _norm(name)
                if nkey in existing_names:
                    continue
                if phone and phone in existing_phones:
                    continue
                domain = _norm_domain(website)
                if domain and domain in existing_domains:
                    continue
                # Fuzzy name check
                is_dup = any(len(ex) >= 8 and nkey[:15] == ex[:15] for ex in existing_names if len(nkey) >= 10)
                if is_dup:
                    continue

                existing_names.add(nkey)
                if phone:
                    existing_phones.add(phone)
                if domain:
                    existing_domains.add(domain)

                # Determine county/msa from slug
                county_name = slug.replace('-county-ok', '').replace('-', ' ').title()
                msa = "Oklahoma City" if slug in {
                    'oklahoma-county-ok','canadian-county-ok','cleveland-county-ok',
                    'grady-county-ok','logan-county-ok','mcclain-county-ok','pottawatomie-county-ok'
                } else "Tulsa"

                new_row = {
                    "law_firm_name": name, "website": website,
                    "google_business_profile": "", "legal_directory_listing": "",
                    "city": rec["city"], "state": "OK", "county": okbar_county,
                    "phone_number": phone, "email": email,
                    "practice_area": pa_name, "street_address": "",
                    "zip_code": rec["zip_code"], "msa": msa,
                    "priority": "3", "number_of_lawyers": "",
                }
                for fn in fieldnames:
                    if fn not in new_row:
                        new_row[fn] = ""
                new_rows.append(new_row)
                rows.append(new_row)

            if new_rows:
                # Write incrementally so we don't lose work
                all_rows = sorted(rows, key=lambda r: (r.get("city",""), r.get("law_firm_name","")))
                csv_path = DATA_DIR / f"{slug}.csv"
                with csv_path.open("w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=fieldnames)
                    w.writeheader()
                    w.writerows(all_rows)

            county_new += len(new_rows)
            if len(new_rows) > 0:
                print(f"  {pa_name}: +{len(new_rows)} new (total raw={len(raw)})")
            time.sleep(1.5)

        print(f"  County total new: {county_new}")
        grand_total_new += county_new

    print(f"\n{'='*50}")
    print(f"Grand total new firms added: {grand_total_new}")

    # Print final counts
    total = 0
    for p in sorted(DATA_DIR.glob("*-county-ok.csv")):
        rows_f = list(csv.DictReader(p.open()))
        total += len(rows_f)
        print(f"  {p.stem}: {len(rows_f)}")
    print(f"  TOTAL OK: {total}")


if __name__ == "__main__":
    run()
