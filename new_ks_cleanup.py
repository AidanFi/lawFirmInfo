#!/usr/bin/env python3
"""
Comprehensive cleanup for 5 new KS counties:
Douglas, Franklin, Jefferson, Osage, Shawnee.
Same law-firm-only standard as the existing 5 KS counties.
"""
import csv, re, json
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).parent / "app/county-data"
MANIFEST_PATH = DATA_DIR / "manifest.json"
NEW_COUNTIES = ["douglas-county-ks","franklin-county-ks","jefferson-county-ks","osage-county-ks","shawnee-county-ks"]

LAW_IND = re.compile(
    r"\b(law|legal|attorney|attorneys|atty|counsel|llp|pllc|lllp|pllp|mediati|"
    r"arbitr|litigat|paralegal|abogad|esq|barrister|solicitor|chartered|chtd)\b"
    r"|p\.c\.|p\.a\.|l\.c\.|l\.l\.p\.|l\.l\.c\.|lpa\b", re.I)

PERSON_NAME = re.compile(r"^[A-Z][a-z'\-]+\.?(\s+[A-Z]\.?)?(\s+[A-Za-z'\-]+){1,3}(\s*,?\s*(jr\.?|sr\.?|ii|iii|iv))?\s*$")
KSCOURTS_NAME = re.compile(r"^[A-Za-z'\-]+,\s+[A-Za-z]+(\s+[A-Za-z]\.?)?(\s+(jr\.?|sr\.?|ii|iii|iv))?$")
LEGAL_SUFFIX = re.compile(r"\b(jr\.?|sr\.?|ii|iii|iv|esq\.?|j\.d\.?|jd\.?|atty\.?)\b", re.I)

def is_person(name):
    n = name.strip()
    cleaned = LEGAL_SUFFIX.sub("", n).strip().strip(",").strip()
    if PERSON_NAME.match(cleaned): return True
    if KSCOURTS_NAME.match(n): return True
    if re.match(r"^[A-Z]\.\s+[A-Za-z]+\s+[A-Za-z'\-]+$", n): return True
    return False

def parse_person(name):
    cleaned = LEGAL_SUFFIX.sub("", name).strip().strip(",").strip()
    if re.search(r"\b(law|legal|attorneys|counsel|llp|pllc|mediator|arbitr|litigat|office|group|services|solutions|firm)\b", cleaned, re.I):
        return None
    m = re.match(r"^([A-Za-z'\-]+),\s+([A-Za-z]+)(?:\s+([A-Za-z]))?$", cleaned.strip())
    if m:
        return (m.group(2).lower(), m.group(1).lower())
    parts = [p.strip(".") for p in cleaned.split() if p.strip(".")]
    if not parts or len(parts) > 5 or len(parts) < 2: return None
    if not all(re.match(r"^[A-Za-z'\-]+$", p) for p in parts): return None
    return (parts[0].lower(), parts[-1].lower())

REMOVE_EXACT = {
    "n/a","none","retired","na",
    # Aggregator/spam services
    "social security disability advisors","legal aid legal services corp",
    "personal injury place","us workers comp","bankruptcy home","foreclosure defense",
    # Douglas County government
    "douglas county district court trustee","douglas county courthouse, div 6",
    "douglas county district court","seventh judicial district public defender's office",
    "seventh judicial district public defender office","7th judicial district public defender's office",
    "douglas county, ks district court self-help office","district court trustee",
    "department for children and families",
    # Franklin County government
    "franklin county district court",
    # Jefferson County government
    "jefferson county district court",
    # Shawnee County government (all variants)
    "kansas supreme court","ks supreme court-office of judicial administration",
    "kansas department of health and environment","kansas dept of health and environment",
    "ks dept of health & environment","ks dept of health and environment","kdhe",
    "kdhe - division of healthcare finance","kansas dept of transportation",
    "kansas department of transportation","ks dept of transportation",
    "kansas department of administration","kansas secretary of state","sbids",
    "third judicial district public defender","third judicial district public defender officer",
    "third judicial district public defender's office","3rd judicial dist public defender office",
    "ks dept of labor, work comp div.","ks dept of labor, div of workers compensation",
    "ks dept of labor, div of work comp","ks dept of revenue","ks department of revenue",
    "kansas department of revenue","kansas dept of revenue","state of ks department of revenue",
    "clerk of the appellate courts","kansas department of insurance",
    "kansas department of insurance, securities division","department of commerce",
    "kansas department of commerce","kansas department of corrections","ks dept of corrections",
    "ks dept of labor","kansas department of labor",
    "kansas department of labor - workers compensation division",
    "state of kansas department of labor","kansas legislative research dept",
    "kansas legislative research department","ks legislative research dept",
    "ks legislative division of post audit","capital appellate defender office",
    "kansas capital appellate defender office","kansas appellate defenders office",
    "kansas appellate defender office","appellate defender office","appellate defender's office",
    "appellate defender office - sbids","appellate reporter's office",
    "kansas board of indigent defense services","kansas state board of indigents' defense services",
    "ks state board of indigent defense services","northeast kansas conflicts public defender office",
    "united states district court","united states district court for the district of kansas",
    "u. s. district court for the district of kansas","us district court",
    "shawnee county district court","shawnee county district court - div. 4",
    "shawnee co. district court","third judicial district court",
    "third judicial district court of kansas","third judicial district court, division 2",
    "shawnee county courthouse","shawnee county public defender",
    "federal public defender","federal public defender's office","kansas federal public defender",
    "district court judge","kansas state treasurer's office",
    "office of the state bank commissioner","kansas office of the state bank commissioner",
    "ks workers comp appeals board","ks department of education","kansas department of education",
    "kansas state department of education","ks department for aging and disability services",
    "kansas department for aging and disability services","kdads",
    "ks dept for children and families","kansas department for children and families",
    "kansas dept. of children and families",
    # Shawnee non-law corporations
    "blue cross & blue shield of ks , inc.","blue cross blue shield of kansas",
    "blue cross and blue shield of kansas, inc.","bcbsks",
    "ofg financial services","american home life insurance company",
    "security benefit life insurance company","disability rights center of kansas",
    "disability rights center of ks","disability rights center of ks , inc",
    "gen iii construction & development llc","vitalcore health strategies",
    "jmc","kammco","younwilliams","youngwilliams","evergy","fbi","kpers",
    "curb","kgfa","ywcss","mfcu","security 1st","ks department for aging and disability services",
    # Douglas non-law
    "bsr","btbc","the university of kansas nunemaker center honors center","rilinglaw",
    # Single first names only
    "joseph","thomas","richard","lynn",
}

KW_DUMP = re.compile(
    r"(,\s*ks\s+.{0,60}(lawyer|attorney)\s*$)"
    r"|(^top\s+rated\s+(lawyer|attorney))"
    r"|(social\s+security\s+disability\s+advi)"
    r"|(^personal\s+injury\s+place\s*$)"
    r"|(^us\s+workers\s+comp\s*$)"
    r"|(^bankruptcy\s+home\s*$)"
    r"|(^foreclosure\s+defense\s*$)", re.I)

GOVT_PATTERN = re.compile(
    r"\b(public\s+defender|appellate\s+defender|indigent\s+defense|"
    r"department\s+of\s+(?:health|transport|admin|revenue|correction|commerce|insurance|education|labor|wildlife)|"
    r"dept\.?\s+of\s+(?:health|transport|admin|revenue|correction|commerce|insurance|education|labor)|"
    r"secretary\s+of\s+state|state\s+treasurer|state\s+bank\s+commissioner|"
    r"judicial\s+district\s+(?:public|court)|legislative\s+research|"
    r"division\s+of\s+post\s+audit|workers\s+comp\s+appeals\s+board|"
    r"children\s+and\s+families|aging\s+and\s+disability|"
    r"state\s+board\s+of\s+indigent|board\s+of\s+indigent)\b", re.I)

CORP_PATTERN = re.compile(
    r"\b(insurance\s+(company|co\.|group|corp)|financial\s+(services|group)|"
    r"health\s+(strategies|plan)|blue\s+cross|construction\s+&?\s+development|"
    r"disability\s+rights\s+center|vital\s*core|retirement\s+(system|fund))\b", re.I)

# Firms actually in Shawnee County (Topeka) — remove from other counties
SHAWNEE_ONLY = {
    "goodell stratton edmonds & palmer llp","law firm of tenopir & huerter",
    "the law firm of tenopir & huerter","hoffman & hoffman","harrington samantha",
    "heathman law office","befort law firm","pearson truck accident law",
    "mindy b. reynolds - attorney at law","downing, shaye l",
    "lanterman, stephen d","ebert william f",
}

def entry_score(r):
    s = 0
    if r.get("website","").strip(): s += 4
    if r.get("phone_number","").strip(): s += 3
    if r.get("email","").strip(): s += 2
    if r.get("legal_directory_listing","").strip(): s += 2
    if r.get("street_address","").strip(): s += 3
    s += len(r.get("law_firm_name","")) * 0.01
    return s

def process_county(slug):
    path = DATA_DIR / f"{slug}.csv"
    rows = list(csv.DictReader(open(path)))
    fieldnames = list(rows[0].keys()) if rows else []
    original = len(rows)
    removed = []

    # Pass 1: Exact + pattern removals
    kept = []
    for r in rows:
        name = r.get("law_firm_name","").strip()
        name_lc = name.lower()
        if name_lc in REMOVE_EXACT:
            removed.append(f"EXACT: {name!r}"); continue
        if KW_DUMP.search(name):
            removed.append(f"KW-DUMP: {name!r}"); continue
        if GOVT_PATTERN.search(name) and not LAW_IND.search(name):
            removed.append(f"GOVT: {name!r}"); continue
        if CORP_PATTERN.search(name) and not LAW_IND.search(name) and not is_person(name):
            removed.append(f"CORP: {name!r}"); continue
        # Cross-county: Shawnee-only firms removed from other counties
        if slug != "shawnee-county-ks" and name_lc in SHAWNEE_ONLY:
            removed.append(f"SHAWNEE-ONLY: {name!r} / {r.get('city','')}"); continue
        kept.append(r)
    rows = kept

    # Pass 2: Within Shawnee — collapse rural YP city dups for known firms (keep best)
    if slug == "shawnee-county-ks":
        groups = defaultdict(list)
        non_target = []
        for r in rows:
            if r.get("law_firm_name","").strip().lower() in SHAWNEE_ONLY:
                groups[r.get("law_firm_name","").strip().lower()].append(r)
            else:
                non_target.append(r)
        shawnee_kept = []
        for name_lc, group in groups.items():
            group.sort(key=entry_score, reverse=True)
            shawnee_kept.append(group[0])
            for drop in group[1:]:
                removed.append(f"SHAWNEE-DUP: {drop['law_firm_name']!r} / {drop.get('city','')}")
        rows = shawnee_kept + non_target

    # Pass 3: Multi-city within-county dedup (same name+phone in 3+ cities)
    phone_groups = defaultdict(list)
    no_phone = []
    for r in rows:
        phone = r.get("phone_number","").strip()
        if phone:
            phone_groups[(r.get("law_firm_name","").strip().lower(), phone)].append(r)
        else:
            no_phone.append(r)
    kept2 = []
    for (name_lc, phone), group in phone_groups.items():
        cities = {r.get("city","").strip().lower() for r in group}
        if len(cities) >= 3:
            group.sort(key=entry_score, reverse=True)
            kept2.append(group[0])
            for drop in group[1:]:
                removed.append(f"MULTI-CITY: {drop['law_firm_name']!r} / {drop.get('city','')}")
        else:
            kept2.extend(group)
    rows = kept2 + no_phone

    # Pass 4: Person-name soft dedup
    person_groups = defaultdict(list)
    non_person = []
    for r in rows:
        name = r.get("law_firm_name","").strip()
        city = r.get("city","").strip().lower()
        parsed = parse_person(name)
        if parsed:
            person_groups[(parsed[0], parsed[1], city)].append(r)
        else:
            non_person.append(r)
    person_kept = []
    for key, group in person_groups.items():
        if len(group) == 1:
            person_kept.extend(group)
        else:
            group.sort(key=entry_score, reverse=True)
            person_kept.append(group[0])
            for drop in group[1:]:
                removed.append(f"PERSON-DUP: {drop['law_firm_name']!r}")
    rows = person_kept + non_person

    # Pass 5: Exact dedup
    seen = set()
    dedup = []
    for r in rows:
        key = (r.get("law_firm_name","").strip().lower(), r.get("city","").strip().lower())
        if key not in seen:
            seen.add(key); dedup.append(r)
        else:
            removed.append(f"EXACT-DUP: {r['law_firm_name']!r}")
    rows = dedup

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{slug}: {original} → {len(rows)} (-{len(removed)})")
    for note in removed:
        print(f"  {note}")
    return len(rows)


results = {}
for slug in NEW_COUNTIES:
    results[slug] = process_county(slug)

manifest = json.loads(MANIFEST_PATH.read_text())
for c in manifest["counties"]:
    if c["slug"] in results:
        c["firm_count"] = results[c["slug"]]
manifest["total_firms"] = sum(c["firm_count"] for c in manifest["counties"])
MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))

print(f"\nFinal counts:")
for slug, n in results.items():
    print(f"  {slug}: {n}")
print(f"Grand total: {manifest['total_firms']}")
