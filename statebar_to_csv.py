#!/usr/bin/env python3
"""
Aggregate a raw State Bar of Texas attorney-level cache
(data/county/<slug>_statebar_cache.json, produced by texasbar_discover.py)
into a firm-level county CSV matching the project schema.

Grouping: attorneys sharing the same firm (fuzzy-matched — the same firm
is frequently self-reported under several name variants, e.g. "Baker Botts",
"Baker Botts L.L.P.", "Baker Botts, LLP") become one firm row
(number_of_lawyers = count of matched attorneys). Attorneys with no listed
company, or a placeholder value ("Self", "Attorney", "N/A", "Retired", etc.)
become their own solo-practitioner row (law_firm_name = attorney name),
matching the convention used for KS Courts registry solo attorneys.

Only "green circle" (Eligible to practice) status is kept — excludes
Not Eligible, Non-Practicing, Inactive, and Deceased members, since
including those would put non-practicing/inaccurate entries in front
of a referral network.

Government offices, courts, law schools, and corporate in-house-counsel
departments are excluded (not referral law firms) — same policy as the
KS county cleanup scripts (final_cleanup.py).
"""
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

from rapidfuzz import fuzz

DATA_DIR = Path("app/county-data")
CACHE_DIR = Path("data/county")

FIELDNAMES = [
    "law_firm_name", "website", "google_business_profile", "legal_directory_listing",
    "city", "state", "county", "phone_number", "email", "practice_area",
    "street_address", "zip_code", "msa", "priority", "number_of_lawyers",
    "date_pulled", "source",
]

PRIORITY_MAP = {
    "Criminal Defense": 5, "DUI": 5, "Personal Injury": 5,
    "Medical Malpractice": 5, "Workers' Compensation": 5,
    "Sexual Assault": 4, "Family Law": 4, "General": 4, "Litigation": 4,
    "Employment Law": 3, "Civil Rights": 3, "Civil Litigation": 3,
    "Estate Planning": 2, "Bankruptcy": 2, "Real Estate": 2,
    "Business Law": 2, "Immigration": 2, "Military Law": 2,
}

COUNTY_META = {
    "harris-county-tx": {"name": "Harris", "state": "TX", "msa": "Houston"},
}

# Self-reported "company" values that are not a real firm name — route
# these attorneys to the solo-practitioner bucket instead of merging
# unrelated people into a fake mega-firm.
PLACEHOLDER_COMPANY = {
    "self", "self employed", "self-employed", "selfemployed", "attorney",
    "attorney at law", "attorneys at law", "n a", "na", "none", "retired",
    "mr", "ms", "mrs", "dr", "sole practitioner", "solo practitioner",
    "unemployed", "individual", "private", "private practice", "unknown",
    "not applicable", "in house", "in-house", "law student", "student",
    "government", "unaffiliated",
}

# Regex fallback for "no employer reported" variants that don't hit the
# exact set above ("None Reported", "None currently", etc.) — route to
# the solo bucket, same as a blank company field.
PLACEHOLDER_RE = re.compile(r'^(none|n/?a|unknown|not applicable)\b', re.IGNORECASE)

# Self-declared non-practicing wording ("Retired", "Inactive", "Deceased")
# — still shows "Eligible to practice" bar status, but not a firm and not
# a currently-referable solo attorney either, so drop entirely.
NONPRACTICING_RE = re.compile(r'\bretire[ds]?\b|\binactive\b|\bdeceased\b|\bunaffiliated\b', re.IGNORECASE)

# Government / court / law-school entities — matched independently of
# "law"/"legal"/"attorney" wording, since agency names legitimately
# contain those words (e.g. "District Attorney's Office").
GOVT_PATTERNS = re.compile(
    r'(district attorney|county attorney|city attorney|attorney general|'
    r'u\.?s\.? attorney|united states attorney|office of the attorney|'
    r'public defender|assigned counsel|managed counsel|'
    r'county clerk|district clerk|county court at law|justice of the peace|'
    r'\bconstable\b|sheriff.?s office|police department|fire department|'
    r'municipal court|probate court|juvenile (probation|court)|'
    r'child protective services|department of family|'
    r'independent school district|\bisd\b|school district|'
    r'\bcity of \w|\bcounty of \w|harris county(?! .*(law|pllc|llp))|'
    r'state of texas|texas department|texas legislature|texas workforce|'
    r'\bdepartment of \w|\bdept\.? of \w|internal revenue service|\birs\b|'
    r'social security administration|federal bureau|u\.?s\.? department|'
    r'u\.?s\.? district court|united states district court|'
    r'court of appeals|supreme court of texas|texas supreme court|'
    r'college of law|law center|law school|school of law|\buniversity\b)',
    re.IGNORECASE,
)

# In-house-counsel job-title phrasing self-reported as the "company" field —
# only a non-law signal when no real firm-entity suffix is also present
# (guards against false positives like "General Counsel Consulting
# Solutions, PLLC", a real PLLC law firm).
INHOUSE_TITLE_RE = re.compile(
    r'general counsel|managing counsel|deputy counsel|chief legal officer|'
    r'\bsubsidiary of\b',
    re.IGNORECASE,
)

# Corporate / institutional in-house-counsel employers — not referral law
# firms. Substring match on distinctive company-name fragments (curated
# from the Harris County dataset; energy-sector heavy since Houston is an
# energy hub).
CORP_NON_LAW_FRAGMENTS = [
    "exxon", "chevron", "shell usa", "shell oil", "conocophillips", "bp america",
    "phillips 66", "marathon oil", "occidental petroleum", "kinder morgan",
    "plains all american", "energy transfer", "targa resources", "nrg energy",
    "technipfmc", "schlumberger", "halliburton", "baker hughes",
    "cheniere energy", "enterprise products", "calpine", "hilcorp", "motiva",
    "sempra", "tc energy", "totalenergies", "enbridge", "air liquide",
    "quanta services", "service corporation international", "woodside energy",
    "apache corporation", "eog resources", "westlake chemical", "westlake corporation",
    "lyondellbasell", "dow chemical", "dow inc", "crown castle",
    "hp inc", "thomson reuters", "insperity", "alliantgroup", "corebridge financial",
    "empower pharmacy", "david weekley homes", "kbr inc", "kbr, inc",
    "deloitte", "ernst & young", "ernst young", "kpmg", "pricewaterhousecoopers", "pwc llp",
    "texas children's hospital", "baylor college of medicine", "memorial hermann",
    "houston methodist", "md anderson", "university of houston", "rice university",
    "jpmorgan chase", "wells fargo", "bbva usa", "comerica bank", "amegy bank",
    "woodforest national bank", "prosperity bank", "cadence bank",
    "sysco corporation", "waste management", "waste connections",
    "united airlines", "academy sports", "hines interests",
]
# Short/ambiguous tokens that need whole-word matching to avoid false positives
CORP_NON_LAW_WHOLE_WORDS = ["oxy", "slb", "kbr", "hines"]

CORP_NON_LAW_RE = re.compile(
    "|".join(re.escape(f) for f in CORP_NON_LAW_FRAGMENTS)
    + "|" + "|".join(r"\b" + re.escape(w) + r"\b" for w in CORP_NON_LAW_WHOLE_WORDS),
    re.IGNORECASE,
)

LAW_FIRM_SUFFIX_RE = re.compile(
    r'\b(llp|pllc|l\.?l\.?p\.?|p\.?l\.?l\.?c\.?|law firm|law office|law group)\b|'
    r'(?:&|and)\s*associates\b', re.IGNORECASE,
)


def normalize_key(name: str) -> str:
    n = (name or "").lower().strip()
    n = re.sub(r"[^\w\s]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


_SUFFIX_WORDS = {
    "llp", "llc", "lp", "pllc", "plc", "pc", "pa", "ltd", "inc", "incorporated",
    "corp", "corporation", "company", "co", "law", "laws", "firm", "firms",
    "group", "office", "offices", "attorney", "attorneys", "lawyer", "lawyers",
    "associates", "and", "the", "of", "a",
}


def _tokens(name: str) -> list[str]:
    s = name.lower().replace("|", " ").replace("+", " ")
    s = re.sub(r"[.']", "", s)
    s = re.sub(r"[^a-z0-9&]+", " ", s).replace("&", " ")
    return [t for t in s.split() if t and t not in _SUFFIX_WORDS]


# Small curated stoplist of genuinely generic tokens (geography/industry
# words) used ONLY to decide subset-rule eligibility below — deliberately
# NOT frequency-based, since common partner surnames (Baker, Brown, Smith,
# Johnson...) recur across many *distinct* real firms and must never be
# treated as "generic" or unrelated firms sharing a surname would merge
# (verified: "Mayer LLP" and "Mayer Brown LLP" are different real firms).
GENERIC_SUBSET_TOKENS = {
    "houston", "texas", "tx", "harris", "county", "usa", "us", "america",
    "international", "national", "center", "department", "services",
    "management", "insurance", "capital", "university", "partners",
    "north", "south", "east", "west", "downtown", "energy", "legal",
}


def same_firm(a_tokens: list[str], b_tokens: list[str], a_key: str, b_key: str) -> bool:
    if not a_tokens or not b_tokens:
        return False
    score = fuzz.token_sort_ratio(a_key, b_key)
    if score >= 85:
        return True
    sa, sb = set(a_tokens), set(b_tokens)
    short, long_ = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
    distinctive_short = short - GENERIC_SUBSET_TOKENS
    if len(distinctive_short) >= 2 and short.issubset(long_):
        return True
    return False


def is_non_law(name: str) -> bool:
    if not name:
        return False
    if GOVT_PATTERNS.search(name):
        return True
    if CORP_NON_LAW_RE.search(name):
        return True
    if INHOUSE_TITLE_RE.search(name) and not LAW_FIRM_SUFFIX_RE.search(name):
        return True
    return False


def _mode(values: list[str]) -> str:
    vals = [v.strip() for v in values if v and v.strip()]
    if not vals:
        return ""
    return Counter(vals).most_common(1)[0][0]


def cluster_firms(company_groups: dict[str, list[dict]]) -> list[list[dict]]:
    """Cluster distinct company strings that refer to the same firm.

    Star-clustering against a canonical anchor (largest groups first) —
    NOT transitive union-find — so two unrelated small groups can never
    chain together just because both matched some third group (that was
    the actual cause of an earlier false 227-attorney merge).
    """
    keys = list(company_groups.keys())
    displays = {k: _mode([m["company"] for m in company_groups[k]]) for k in keys}
    toks = {k: _tokens(displays[k]) for k in keys}
    norm_keys = {k: " ".join(sorted(toks[k])) for k in keys}

    # Process largest raw groups first so real firms anchor their own
    # cluster before any smaller variant gets a chance to.
    keys.sort(key=lambda k: -len(company_groups[k]))

    token_index = defaultdict(list)  # non-generic token -> anchor keys
    assigned = {}  # key -> anchor key
    anchors = []

    for k in keys:
        if k in assigned:
            continue
        candidates = set()
        for t in set(toks[k]):
            candidates.update(token_index.get(t, []))
        match = None
        for anchor in candidates:
            if same_firm(toks[k], toks[anchor], norm_keys[k], norm_keys[anchor]):
                match = anchor
                break
        if match:
            assigned[k] = match
        else:
            anchors.append(k)
            assigned[k] = k
            for t in set(toks[k]):
                token_index[t].append(k)

    clusters = defaultdict(list)
    for k in keys:
        clusters[assigned[k]].extend(company_groups[k])
    return list(clusters.values())


def build_csv(slug: str) -> int:
    cache_path = CACHE_DIR / f"{slug}_statebar_cache.json"
    if not cache_path.exists():
        print(f"Cache not found: {cache_path}")
        sys.exit(1)

    entries = json.loads(cache_path.read_text())
    meta = COUNTY_META[slug]

    kept_status = [e for e in entries if e.get("status", "").strip() == "green circle"]
    print(f"Loaded {len(entries)} raw records, {len(kept_status)} 'Eligible to practice'")

    raw_groups = defaultdict(list)
    solos = []
    dropped_placeholder = 0
    for e in kept_status:
        company = (e.get("company") or "").strip()
        key = normalize_key(company)
        if not company:
            solos.append(e)
        elif NONPRACTICING_RE.search(company):
            dropped_placeholder += 1
        elif key in PLACEHOLDER_COMPANY or PLACEHOLDER_RE.search(company):
            solos.append(e)
        else:
            raw_groups[key].append(e)

    print(f"{len(raw_groups)} distinct raw company strings before fuzzy-merge, "
          f"{len(solos)} solo/placeholder attorneys, {dropped_placeholder} dropped as non-practicing")

    clusters = cluster_firms(raw_groups)
    print(f"{len(clusters)} firms after fuzzy-merge dedup")

    rows = []
    today = date.today().isoformat()
    dropped_non_law = 0

    for members in clusters:
        display_name = _mode([m["company"] for m in members])
        # prefer the fullest formal name (with a legal-entity suffix) as
        # display — and decide non-law status on THAT name, since a mixed
        # cluster (some members reported "X, PLLC", others just "X") should
        # be judged by its most complete/formal self-report, not whichever
        # variant happened to be more common.
        formal_candidates = [m["company"].strip() for m in members if LAW_FIRM_SUFFIX_RE.search(m["company"])]
        if formal_candidates:
            display_name = _mode(formal_candidates)
        if is_non_law(display_name):
            dropped_non_law += 1
            continue
        city = _mode([m.get("city", "") for m in members])
        street = _mode([m.get("street", "") for m in members])
        zipc = _mode([m.get("zip", "") for m in members])
        phone = _mode([m.get("phone", "") for m in members])
        rows.append({
            "law_firm_name": display_name,
            "website": "",
            "google_business_profile": "",
            "legal_directory_listing": "",
            "city": city,
            "state": meta["state"],
            "county": meta["name"],
            "phone_number": phone,
            "email": "",
            "practice_area": "General",
            "street_address": street,
            "zip_code": zipc,
            "msa": meta["msa"],
            "priority": str(PRIORITY_MAP["General"]),
            "number_of_lawyers": str(len(members)),
            "date_pulled": today,
            "source": "State Bar of Texas Member Directory",
        })

    dropped_solo_non_law = 0
    for e in solos:
        name = e.get("name", "").strip()
        if not name:
            continue
        if is_non_law(name):
            dropped_solo_non_law += 1
            continue
        rows.append({
            "law_firm_name": name,
            "website": "",
            "google_business_profile": "",
            "legal_directory_listing": "",
            "city": e.get("city", ""),
            "state": meta["state"],
            "county": meta["name"],
            "phone_number": e.get("phone", ""),
            "email": "",
            "practice_area": "General",
            "street_address": e.get("street", ""),
            "zip_code": e.get("zip", ""),
            "msa": meta["msa"],
            "priority": str(PRIORITY_MAP["General"]),
            "number_of_lawyers": "1",
            "date_pulled": today,
            "source": "State Bar of Texas Member Directory",
        })

    for r in rows:
        for field in ("law_firm_name", "city", "street_address"):
            r[field] = re.sub(r"\s+", " ", r[field]).strip()

    rows.sort(key=lambda r: (r["city"], r["law_firm_name"]))

    out_path = DATA_DIR / f"{slug}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Dropped {dropped_non_law} non-law firm clusters, {dropped_solo_non_law} non-law solo entries")
    print(f"Wrote {len(rows)} firm rows to {out_path}")
    return len(rows)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 statebar_to_csv.py <slug>")
        sys.exit(1)
    build_csv(sys.argv[1])
