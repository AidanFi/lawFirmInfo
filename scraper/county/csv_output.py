import csv
import os

from scraper.county.config import get_priority

CSV_COLUMNS = [
    "law_firm_name",
    "website",
    "google_business_profile",
    "legal_directory_listing",
    "city",
    "state",
    "county",
    "phone_number",
    "email",
    "practice_area",
    "street_address",
    "zip_code",
    "msa",
    "priority",
]


def _select_practice_area(firm: dict) -> str:
    areas = firm.get("practiceAreas") or []
    if not areas:
        return "General"
    best_area = areas[0]
    best_score = get_priority(best_area)
    for area in areas[1:]:
        score = get_priority(area)
        if score > best_score:
            best_area = area
            best_score = score
    return best_area


def _select_directory_listing(firm: dict) -> str:
    for key in ("martindale_url", "justia_url", "avvo_url", "findlaw_url"):
        url = firm.get(key)
        if url:
            return url
    return ""


def _firm_to_row(firm: dict, county_config: dict) -> dict:
    addr = firm.get("address") or {}
    practice_area = _select_practice_area(firm)
    return {
        "law_firm_name": firm.get("name", ""),
        "website": firm.get("website") or "",
        "google_business_profile": firm.get("google_business_profile") or "",
        "legal_directory_listing": _select_directory_listing(firm),
        "city": addr.get("city", ""),
        "state": addr.get("state", county_config["state"]),
        "county": county_config["name"].replace(" County", ""),
        "phone_number": firm.get("phone") or "",
        "email": firm.get("email") or "",
        "practice_area": practice_area,
        "street_address": addr.get("street") or "",
        "zip_code": addr.get("zip") or "",
        "msa": county_config["msa"],
        "priority": str(get_priority(practice_area)),
    }


def firms_to_csv(firms: list, county_config: dict, output_path: str) -> int:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    rows = [_firm_to_row(f, county_config) for f in firms]
    rows.sort(key=lambda r: (r["city"], r["law_firm_name"]))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[csv] Wrote {len(rows)} firms to {output_path}")
    return len(rows)
