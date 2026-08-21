#!/usr/bin/env python3
"""
Build a statewide (name, city) -> attorney_count map from the local
.kscourts_cache HTML archive (14,622 cached attorney detail pages, no network
needed). Used as a fallback by add_lawyer_counts.py for counties that never
got an individual per-county kscourts cache JSON built.
"""
import csv, json, re
from pathlib import Path
from bs4 import BeautifulSoup

CACHE_DIR = Path(".kscourts_cache")
OUT_PATH = Path("data/county/_statewide_lawyer_counts.json")


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd)\b", "", name)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def parse_detail(html: str):
    soup = BeautifulSoup(html, "lxml")
    data = {}
    for row in soup.find_all("div", class_="row"):
        label_el = row.find("strong")
        if not label_el:
            continue
        label = label_el.get_text(strip=True)
        divs = row.find_all("div")
        val_div = next((d for d in divs if not d.find("strong") and d.get_text(strip=True)), None)
        if not val_div:
            continue
        data[label] = val_div.get_text(separator="|", strip=True)

    addr_raw = data.get("Business Mailing Address", "")
    addr_parts = [p.strip() for p in addr_raw.split("|") if p.strip()]
    firm_name, city, state = "", "", ""
    if addr_parts:
        for i, part in enumerate(addr_parts):
            m = re.match(r"^(.+),\s*([A-Z]{2})\s*(\d{5})?$", part)
            if m:
                city = m.group(1).strip()
                state = m.group(2).strip()
                before = addr_parts[:i]
                if len(before) >= 2:
                    firm_name = before[0]
                break
    atty_name_raw = data.get("Attorney Name", "")
    if "," in atty_name_raw:
        last, first = atty_name_raw.split(",", 1)
        atty_name = f"{first.strip()} {last.strip()}"
    else:
        atty_name = atty_name_raw
    return firm_name or atty_name, city, state


def main():
    counts = {}
    files = list(CACHE_DIR.glob("*.txt"))
    print(f"Parsing {len(files)} cached attorney pages...")
    for i, f in enumerate(files):
        if i % 2000 == 0:
            print(f"  {i}/{len(files)}")
        try:
            html = f.read_text(encoding="utf-8")
        except Exception:
            continue
        firm_name, city, state = parse_detail(html)
        if state.upper() != "KS" or not firm_name:
            continue
        key = normalize(firm_name) + "|" + city.lower().strip()
        counts[key] = counts.get(key, 0) + 1

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(counts, indent=1))
    print(f"Wrote {len(counts)} (firm,city) -> count entries to {OUT_PATH}")


if __name__ == "__main__":
    main()
