#!/usr/bin/env python3
"""
Cleanup pass for Sedgwick + additional KS counties (2nd batch).
Removes government entities, non-law employers, and duplicates.
"""
import csv
import re
from pathlib import Path

DATA_DIR = Path("app/county-data")

# ── Exact-name removal ───────────────────────────────────────────────────────
REMOVE_EXACT = {
    # Sedgwick County government
    "Sedgwick County", "Sedgwick County District Attorney", "Sedgwick County Attorney",
    "Sedgwick County Counselor", "Sedgwick County District Court",
    "City of Wichita", "Wichita City Attorney", "Wichita Police Department",
    "Kansas Department of Revenue", "Kansas Department of Labor",
    "Kansas Attorney General", "Kansas Bureau of Investigation",
    "United States Attorney", "U.S. Attorney", "US Attorney",
    "US Department of Justice", "US Army", "Department of Defense",
    # Fort Riley / military
    "Fort Riley", "U.S. Army", "US Army Judge Advocate", "Judge Advocate General",
    "Office of the Staff Judge Advocate", "Staff Judge Advocate",
    "JAG Corps", "Army JAG",
    # Riley County government
    "Riley County Attorney", "Riley County District Court",
    "Manhattan City Attorney",
    # Kansas State University
    "Kansas State University", "KSU", "K-State",
    # Ellis County government
    "Ellis County Attorney", "Ellis County District Court",
    "Hays City Attorney",
    # Fort Hays State University
    "Fort Hays State University", "FHSU",
    # Ford County government
    "Ford County Attorney", "Ford County District Court",
    "Dodge City City Attorney",
    # Finney County government
    "Finney County Attorney", "Finney County District Court",
    "Garden City City Attorney",
    # Geary County / Fort Riley area
    "Geary County Attorney", "Geary County District Court",
    "Junction City City Attorney",
    # Seward County
    "Seward County Attorney", "Seward County District Court",
    # Sumner County
    "Sumner County Attorney", "Sumner County District Court",
    # Atchison County
    "Atchison County Attorney", "Atchison County District Court",
    # Pottawatomie County
    "Pottawatomie County Attorney", "Pottawatomie County District Court",
    # Crawford County
    "Crawford County Attorney", "Crawford County District Court",
    # McPherson College (already in central cleanup but safety)
    "McPherson College",
    # Hospitals
    "Wesley Medical Center", "Via Christi", "Ascension Via Christi",
    "Kansas Heart Hospital", "Kansas Spine Hospital", "Wichita VA Medical",
    "Stormont Vail Health", "Mercy Regional Health", "Mercy Health Center",
    "Hutchinson Regional", "Newton Medical",
    "Menorah Medical Center",
    # Universities
    "Wichita State University", "WSU", "Friends University",
    "Newman University", "Southwestern College",
    "Benedictine College",  # Atchison
    "Dodge City Community College",
    # Major corporations (in-house counsel)
    "Spirit AeroSystems", "Spirit AeroSystems Inc.", "Spirit AeroSystems Inc",
    "Koch Industries", "Koch Capabilities LLC", "Koch Legal Capability LLC",
    "Koch Engineered Solutions, LLC", "Koch Engineered Solutions LLC",
    "Chase Koch Family Office, LLC", "Chase Koch Family Office LLC",
    "INTRUST Bank, N.A.", "INTRUST Bank NA", "INTRUST Bank",
    "Citizens Bank Of Kansas", "Midwest Trust Company",
    "Cessna", "Textron Aviation Inc", "Textron Aviation Inc.",
    "Textron Financial Corporation", "Textron Financial Corp",
    "The Boeing Company", "Boeing",
    "Bombardier", "Airbus Americas",
    "Cargill", "Tyson Foods", "National Beef",
    "Pizza Hut", "YUM! Brands",
    "BNSF Railway", "Union Pacific Railroad",
    "Dillons", "Kroger",
    "Westar Energy", "Evergy",
    "Midwest Energy",
    "Southwest Airlines",
    "US Bank", "Capitol Federal",
    "McCurdy Real Estate & Auction, LLC", "McCurdy Real Estate",
    "Hunter Health", "Kansas Health Foundation",
    "Medical Development Mgmt LLC", "Line Medical, Inc.",
    "Northeast Magnet High School", "Wichita Public Schools",
    # Cargill entities
    "Cargill, Inc.", "Cargill Inc", "Cargill Meat Solutions Corp.",
    "Cargill Meat Solutions Corporation", "Cargill Meat Solutions Corp",
    # Other corps
    "KS Corporation Commission", "Kansas Corporation Commission",
    "Wichita Region, Porsche Club of America, Inc.",
    "Inter-Americas Ins Corp",
    # Federal offices
    "Office of the Federal Public Defender",
    "Federal Defender's Office",
    "Federal Public Defender, District of Kansas",
    "Kansas Federal Public Defender",
    "Sedgwick County Public Defender Office",
    "Office of the United States Trustee",
    "United States Attorney's Office",
    "U.S. Bankruptcy Court for the District of Kansas",
    "US Federal Bankruptcy Court",
    "Probate Dept, Court Bldg",
    "KS Dept of Children and Families",
    "KS Dept Of Labor Div of Work Comp",
    "KS Dept Of Labor Div. Of Workers Comp",
    "City of Wichita Law Dept",
    "City of Wichita Law Department",
    "City of Wichita Prosecutor's Office",
}

REMOVE_PATTERN = re.compile(
    r"\b(county\s+attorney|district\s+court|clerk\s+of\s+(the\s+)?court|"
    r"judicial\s+district|department\s+of|dept\s+of\b|"
    r"state\s+of\s+kansas|board\s+of\s+(county\s+)?|"
    r"USD\s*\d+|unified\s+school|community\s+college|"
    r"public\s+schools?\b|high\s+school\b|magnet\s+(school|high)\b|"
    r"medical\s+center|regional\s+medical|memorial\s+hospital|"
    r"health\s+(system|foundation)\b|healthcare\b|"
    r"\bfort\s+riley\b|judge\s+advocate|"
    r"city\s+attorney|municipal\s+court|"
    r"public\s+defender|federal\s+defender|"
    r"\bcity\s+of\b|\bcities\s+of\b|"
    r"\boffice\s+of\s+the\b|"
    r"united\s+states\s+(attorney|trustee|bankruptcy)|"
    r"\bu\.?s\.?\s+(attorney|trustee|bankruptcy|district|marshal)\b|"
    r"federal\s+(public|bankruptcy|district|court)\b|"
    r"bankruptcy\s+court\b|probate\s+dept\b|"
    r"\bbank\s*,?\s*n\.?a\.?\b|\btrust\s+company\b|\btrust\s+bank\b|"
    r"\bauction\s*(house|llc|co)?\b|real\s+estate\s+&\s+auction\b|"
    r"aerospace\b|aviation\s+inc\b|financial\s+corp\b|"
    r"engineered\s+solutions\b|capabilities\s+llc\b|"
    r"family\s+office\b|\bhealth\s+clinic\b|clinic\b\s+llc\b|"
    r"prosecution\s+services\b|prosecutor.?s\s+office\b)\b",
    re.I,
)

# Cities that appear in multiple states — require KS area code or other signal
AMBIGUOUS_CITIES = {
    "wichita",       # KS only? actually common name, but primarily Wichita KS
    "derby",         # many Derbys; Derby KS is real but watch for Derby England
    "andover",       # Andover MA, KS, etc.
    "haysville",     # primarily KS
    "manhattan",     # Manhattan KS, Manhattan NY
    "junction city", # mainly KS
    "liberal",       # Liberal KS, but "liberal" appears in descriptions too
    "hays",          # Hays KS, also surname
    "dodge city",    # primarily KS
    "garden city",   # Garden City KS, Garden City NY
    "wellington",    # Wellington KS, Wellington OH, etc.
    "atchison",      # Atchison KS, also surname
    "caldwell",      # many Caldwells
    "newton",        # Newton KS handled by Harvey county; Newton MA/TX
    "pittsburg",     # Pittsburg KS (no h), Pittsburg TX
}

# Area codes that are definitely NOT KS
NONKS_AREA_CODES = {
    "212", "718", "646", "347", "929",   # NYC
    "617", "857",                         # Boston
    "312", "773", "872",                  # Chicago
    "713", "832", "281",                  # Houston TX
    "214", "469", "972",                  # Dallas TX
    "512", "737",                         # Austin TX
    "918", "405", "580",                  # Oklahoma
    "334",                                # Alabama
    "615", "931",                         # Tennessee
    "317", "765",                         # Indiana
    "913",                                # Johnson County KS (not these counties)
    "816",                                # Missouri KC
    "314",                                # St. Louis
    "636",                                # St. Charles MO
    "573",                                # Columbia MO
    "417",                                # SW Missouri
}

# Area code by county for phone repair
COUNTY_AREA_CODE = {
    "sedgwick-county-ks": "316",
    "riley-county-ks": "785",
    "ellis-county-ks": "785",
    "finney-county-ks": "620",
    "geary-county-ks": "785",
    "ford-county-ks": "620",
    "seward-county-ks": "620",
    "sumner-county-ks": "620",
    "atchison-county-ks": "913",  # NE KS uses 913 like KC area
    "pottawatomie-county-ks": "785",
    "crawford-county-ks": "620",
    "barber-county-ks": "620",
    "pratt-county-ks": "620",
    "comanche-county-ks": "620",
    "kiowa-county-ks": "620",
}


def normalize(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(
        r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|"
        r"attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd|co|inc|"
        r"corp|company|limited|ltd|incorporated|associates?)\b",
        "", name,
    )
    return re.sub(r"[^a-z0-9]", "", name)


def _share_name_token(a: str, b: str) -> bool:
    """True if names share at least one significant (4+ char) token."""
    ta = {t for t in re.findall(r"[a-z]{4,}", a.lower()) if t not in
          ("firm", "office", "attorney", "attorneys", "lawyer", "lawyers",
           "legal", "group", "associates", "chartered")}
    tb = {t for t in re.findall(r"[a-z]{4,}", b.lower()) if t not in
          ("firm", "office", "attorney", "attorneys", "lawyer", "lawyers",
           "legal", "group", "associates", "chartered")}
    return bool(ta & tb)


def fix_phone(phone: str, area_code: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 7:
        digits = area_code + digits
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


def process_county(slug: str) -> int | None:
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        print(f"  {slug}: CSV not found")
        return None

    area_code = COUNTY_AREA_CODE.get(slug, "785")

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(csv.DictReader(open(path)).fieldnames or [])

    kept = []
    removed_junk = 0
    removed_dups = 0

    seen_norm = set()
    seen_phone = set()

    for r in rows:
        name = r.get("law_firm_name", "").strip()
        phone = r.get("phone_number", "").strip()
        city = r.get("city", "").strip().lower()

        # Fix 7-digit phones
        if phone:
            r["phone_number"] = fix_phone(phone, area_code)
            phone = r["phone_number"]

        # Remove exact matches
        if name in REMOVE_EXACT:
            removed_junk += 1
            continue

        # Remove by pattern
        if REMOVE_PATTERN.search(name):
            removed_junk += 1
            continue

        # Remove generic C-corps / non-law entities not caught above
        law_kws = {"law", "legal", "attorney", "attorneys", "lawyer", "lawyers",
                   "counsel", "llp", "pllc", " pa ", " pc ", "chartered", "chtd"}
        name_lower = name.lower()
        has_law_kw = any(kw in name_lower for kw in law_kws)
        corp_suffix = bool(re.search(
            r"\b(inc\.?|corp\.?|corporation|incorporated|solutions\b|"
            r"industries\b|systems\b|technologies\b|manufacturing\b|"
            r"petroleum\b|oil\s+corp|club\s+of|foods?\s+corp|meat\s+corp|"
            r"commission\b|porsche|nuvative|trucking\b)\b",
            name, re.I,
        ))
        if corp_suffix and not has_law_kw:
            removed_junk += 1
            continue

        # kscourts always-keep rule
        ksc = r.get("legal_directory_listing", "")
        is_kscourts = "kscourts" in ksc

        # Ambiguous city: require KS area code or kscourts link
        if city in AMBIGUOUS_CITIES and not is_kscourts:
            area = re.sub(r"\D", "", phone)[:3] if phone else ""
            if area in NONKS_AREA_CODES:
                removed_junk += 1
                continue

        # Dedup by normalized name
        norm = normalize(name) + "|" + city
        if not normalize(name):
            removed_junk += 1
            continue

        if norm in seen_norm:
            removed_dups += 1
            continue

        # Dedup by phone (same phone number = same firm)
        digits = re.sub(r"\D", "", phone) if phone else ""
        if digits and len(digits) >= 10 and digits in seen_phone:
            # Merge: keep existing entry (already in kept)
            removed_dups += 1
            continue

        # Token-overlap + phone-based dedup
        is_dup = False
        for kr in kept[-100:]:
            kname = kr.get("law_firm_name", "")
            kphone = re.sub(r"\D", "", kr.get("phone_number", ""))
            kcity = kr.get("city", "").strip().lower()
            if kcity != city:
                continue
            if digits and kphone == digits:
                is_dup = True
                break
            if _share_name_token(name, kname) and normalize(name) and normalize(kname):
                nname = normalize(name)
                nkname = normalize(kname)
                if nname and nkname and (nname in nkname or nkname in nname):
                    is_dup = True
                    break
        if is_dup:
            removed_dups += 1
            continue

        seen_norm.add(norm)
        if digits and len(digits) >= 10:
            seen_phone.add(digits)
        kept.append(r)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(kept)

    n = len(kept)
    print(f"  {slug}: {len(rows)} → {n} (-{removed_junk} junk, -{removed_dups} dups)")
    return n


if __name__ == "__main__":
    import sys
    slugs = sys.argv[1:] or ["sedgwick-county-ks"]
    for slug in slugs:
        process_county(slug)
