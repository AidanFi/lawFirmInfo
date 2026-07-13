#!/usr/bin/env python3
"""
Cleanup for 17 central Kansas counties.
Removes non-law businesses, government entities, non-law kscourts employers,
and other junk. Deduplicates by name+city and phone+city.
"""
import csv, re, json
from pathlib import Path

DATA_DIR = Path(__file__).parent / "app/county-data"
MANIFEST_PATH = DATA_DIR / "manifest.json"

CENTRAL_KS_COUNTIES = [
    "barton-county-ks",
    "clay-county-ks",
    "cloud-county-ks",
    "dickinson-county-ks",
    "ellsworth-county-ks",
    "harvey-county-ks",
    "kingman-county-ks",
    "lincoln-county-ks",
    "marion-county-ks",
    "mcpherson-county-ks",
    "mitchell-county-ks",
    "ottawa-county-ks",
    "reno-county-ks",
    "rice-county-ks",
    "russell-county-ks",
    "saline-county-ks",
    "stafford-county-ks",
]

KS_AREA_CODES = {"316", "620", "785", "913"}

NONKS_AREA_CODES = {
    "954", "239", "561", "305", "407",
    "214", "972", "469", "713", "832",
    "516", "212", "718", "646",
    "727", "813", "941", "786",
    "702", "775",
    "303", "720",
    "312", "773",
    "404", "770",
    "602", "480", "623",
    "617", "508",
    "414", "262",
    "513", "216",
    "678",
    "402", "531",
    "612", "651", "763", "952",
    "614", "440", "740", "419",
    "215", "610", "717", "412", "267",
    "248", "313", "734", "517",
    "401",
    "203", "860",
    "202", "301", "240",
    "703", "571", "804", "757",
    "503", "971",
    "206", "253", "425",
    "415", "510", "408",
    "310", "323", "213",
    "512", "737",
    "418", "514",
    "816", "573", "636",
    "416", "647", "437", "905",
    "604", "778", "236", "250",
    "201", "609", "856", "732", "848",
    "336", "704", "919", "828",
    "319", "515", "563",
    "844", "855", "866", "877", "888",
    "800",
    "760", "442",
    "443", "410",
    "805",
    "315",
    "902",
    "225",
    "801", "385",
    "918",
    "479", "501", "870",
    "405",  # Oklahoma City
    "580",  # SW Oklahoma
    "334",  # Alabama (Enterprise AL)
    "615",  # Nashville TN
    "931",  # Middle Tennessee
    "317",  # Indianapolis IN
    "765",  # Central Indiana (Marion IN)
    "740",  # SE Ohio (Florence OH area)
    "843",  # SC (Florence SC)
    "802",  # Vermont (Bennington VT)
}

LAW_INDICATORS = re.compile(
    r"\b(law|legal|attorney|attorneys|atty|attys|counsel|counselor|llp|pllc|lllp|pllp|"
    r"mediati|arbitr|litigat|paralegal|abogad|esq|barrister|solicitor|chartered|chtd|"
    r"defender|criminal|injury|accident|divorce|probate|bankruptcy|firm)\b"
    r"|p\.c\.|p\.a\.|l\.c\.|l\.l\.p\.|l\.l\.c\.",
    re.IGNORECASE
)

NON_LAW_PATTERNS = re.compile(
    r"\b(?:"
    r"restaurant|bistro|grill|grille|catering|bakery|cafe|tavern|bar & grill|"
    r"steakhouse|seafood|pizza|taco|burrito|sushi|diner|pub|brewery|winery|"
    r"vineyard|golf|bowling|bowl|"
    r"auto parts|auto repair|auto body|car wash|tire center|muffler|"
    r"trucking|towing|excavating|excavation|"
    r"cabinet maker|custom furniture|florist|floral|grocery|supermarket|"
    r"pawn shop|hardware|appliance|lumber|"
    r"roofing|plumbing|hvac|electrician|painting contractor|concrete|landscaping|"
    r"lawn and landscape|lawn care|lawn service|"
    r"excavat|welding|manufacturing|midstream|pipeline|oil & gas|"
    r"counseling therapy|dental|dentist|medical clinic|hospital|pharmacy|"
    r"chiropract|physical therapy|"
    r"church|chapel|cathedral|mosque|synagogue|temple|ministry|parish|"
    r"fire station|fire dept|water treatment|city clerk|city hall|"
    r"county courthouse|federal courthouse|"
    r"financial advisor|edward jones|investment advisor|"
    r"insurance agent|state farm|allstate|farmers insurance|"
    r"real estate|realty|realtor|"
    r"high school|elementary school|school district|community college|university|"
    r"highway patrol|state patrol|"
    r"supermarket|market|grocery|gas station|convenience store|"
    r"meats\b|butcher|meat shop|"
    r"museum|library|"
    r"family services|social services|"
    r"consulting llc(?! law| legal)|consulting services|"
    r"towing & recovery|"
    r"refinery|petroleum|energy company|grain elevator|co-op|cooperative"
    r")\b",
    re.IGNORECASE
)

# Always-remove names (lowercase). Non-law employers and clear junk.
REMOVE_EXACT = {
    # Government county entities
    "barton county attorney", "barton county courthouse", "barton county district court",
    "barton county attorney's office",
    "clay county attorney", "clay county courthouse", "clay county district court",
    "cloud county attorney", "cloud county courthouse", "cloud county district court",
    "dickinson county attorney", "dickinson county courthouse", "dickinson county district court",
    "ellsworth county attorney", "ellsworth county courthouse", "ellsworth county district court",
    "harvey county attorney", "harvey county courthouse", "harvey county district court",
    "kingman county attorney", "kingman county courthouse", "kingman county district court",
    "lincoln county attorney", "lincoln county courthouse", "lincoln county district court",
    "marion county attorney", "marion county courthouse", "marion county district court",
    "mcpherson county attorney", "mcpherson county courthouse", "mcpherson county district court",
    "mitchell county attorney", "mitchell county courthouse", "mitchell county district court",
    "ottawa county attorney", "ottawa county courthouse", "ottawa county district court",
    "reno county attorney", "reno county courthouse", "reno county district court",
    "rice county attorney", "rice county courthouse", "rice county district court",
    "russell county attorney", "russell county courthouse", "russell county district court",
    "saline county attorney", "saline county courthouse", "saline county district court",
    "stafford county attorney", "stafford county courthouse", "stafford county district court",
    # State government
    "state of kansas",
    "kansas department for children & families",
    "kansas department of corrections",
    "kansas department of revenue",
    # Known non-law kscourts employers — hospitals/medical
    "salina regional health center",
    "salina regional medical center",
    "newton medical center",
    "hutchinson regional medical center",
    "great bend regional hospital",
    "central kansas medical center",
    "st. luke's hospital",
    "saint luke's hospital",
    "memorial hospital",
    "susquehanna health system",
    "larned state hospital",
    # Known non-law kscourts employers — universities/colleges
    "kansas wesleyan university",
    "bethel college",
    "mcpherson college",
    "barton county community college",
    "barton community college",
    "hutchinson community college",
    "central christian college",
    "sterling college",
    "saint john's military school",
    "st. john's military school",
    "salina vocational technical school",
    # Known non-law kscourts employers — school districts
    "usd 305",  # Salina
    "usd 308", "usd 309", "usd 468",  # Hutchinson area
    "usd 373",  # Newton / Harvey County
    "usd 428",  # Great Bend / Barton
    "usd 388",  # Lincoln
    "usd 392",  # Clay Center
    "usd 333",  # Concordia / Cloud
    "usd 435",  # Abilene / Dickinson
    "usd 101",  # Ellsworth
    "usd 418",  # McPherson
    "usd 273",  # Beloit / Mitchell
    "usd 243",  # Minnepolis KS / Ottawa County
    "usd 281",  # Kingman
    "usd 408",  # Marion
    "usd 422",  # McPherson County
    "usd 331",  # Rice County
    "usd 405",  # Russell
    "usd 255",  # Stafford
    # Known non-law kscourts employers — industry
    "hf sinclair",
    "holly frontier",
    "hollyfrontier",
    "dillons food stores",
    "dillons",
    "tyson foods",
    "cargill meat solutions",
    "siebert lutheran foundation",
    # Specific junk
    "edward jones",
    "edward jones - financial advisor",
    "young williams",
    "young williams cse",
    "young williams child support services",
}

NON_LAW_DOMAINS = {
    "edwardjones.com",
    "facebook.com",
    "usd305.com", "usd308.org", "usd373.org", "usd428.org",
    "srhc.com",  # Salina Regional Health Center
    "newtonmedicalcenter.com",
    "hutchregional.com",
    "mcphersoncollege.edu",
    "bethelks.edu",
    "kwu.edu",  # Kansas Wesleyan University
    "bartonccc.edu",  # Barton Community College
    "hutchcc.edu",  # Hutchinson Community College
    "sterlingcollege.edu",
    "centralchristian.edu",
}

DIRECTORY_WEBSITE_PATTERNS = re.compile(
    r"(martindale\.com|avvo\.com|findlaw\.com|justia\.com|"
    r"lawyer-map\.com|experience\.com|lawyers\.com|"
    r"superlawyers\.com|yelp\.com|yellowpages\.com)",
    re.IGNORECASE
)

GENERIC_DIRECTORY_URLS = re.compile(
    r"martindale\.com/by-location/|"
    r"avvo\.com/find-a-lawyer/|"
    r"findlaw\.com/lawyer/|"
    r"justia\.com/lawyers/\w+-lawyers/?$",
    re.IGNORECASE
)

# Cities shared with other states that need local signal to confirm KS
AMBIGUOUS_CITIES = {
    "newton",       # Newton MA, NJ, etc.
    "lincoln",      # Lincoln NE
    "marion",       # Marion IN, OH, etc.
    "florence",     # Florence SC, AL, etc.
    "nashville",    # Nashville TN
    "canton",       # Canton OH
    "enterprise",   # Enterprise AL
    "hope",         # Hope AR
    "ellsworth",    # Ellsworth ME
    "russell",      # Russell in many states
    "salina",       # Salina UT (small but exists)
    "sterling",     # Sterling CO
    "minneapolis",  # Minneapolis MN (Ottawa County KS seat)
    "ottawa",       # Ottawa Canada / Ottawa IL
    "concordia",    # Concordia CA, etc.
    "lyons",        # Lyons NY, CO, etc.
    "beloit",       # Beloit WI
    "hutchinson",   # Hutchinson MN
    "hesston",      # rare but safe to check
}


def get_area_code(phone):
    digits = re.sub(r"[^\d]", "", phone or "")
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) >= 10:
        return digits[:3]
    return None


def _normalize_quotes(s):
    return s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')


def clean_website(row):
    website = row.get("website", "").strip()
    if not website:
        return row
    domain = re.sub(r"https?://(?:www\.)?", "", website.lower()).split("/")[0]
    if domain in NON_LAW_DOMAINS or DIRECTORY_WEBSITE_PATTERNS.search(website):
        row = dict(row)
        row["website"] = ""
    return row


def clean_directory_listing(row):
    listing = row.get("legal_directory_listing", "").strip()
    if not listing:
        return row
    if GENERIC_DIRECTORY_URLS.search(listing):
        row = dict(row)
        row["legal_directory_listing"] = ""
    return row


def fix_fields(row):
    row = dict(row)
    email = row.get("email", "")
    if email and re.search(r"(leadrouter|wixpress|sentry|tracking|noreply@)", email, re.I):
        row["email"] = ""

    # Fix bare 7-digit phone fragments (add KS area code based on county)
    phone = row.get("phone_number", "").strip()
    if re.match(r"^\d{3}-\d{4}$", phone):
        county = row.get("county", "").lower()
        # 785 area: Clay, Cloud, Dickinson, Ellsworth, Lincoln, Mitchell, Ottawa, Russell, Saline
        if county in {"clay", "cloud", "dickinson", "ellsworth", "lincoln",
                      "mitchell", "ottawa", "russell", "saline"}:
            row["phone_number"] = f"(785) {phone}"
        # 620 area: Barton, Kingman, Marion, McPherson, Reno, Rice, Stafford
        elif county in {"barton", "kingman", "marion", "mcpherson", "reno", "rice", "stafford"}:
            row["phone_number"] = f"(620) {phone}"
        # 316 area: Harvey (Newton is 316)
        elif county == "harvey":
            row["phone_number"] = f"(316) {phone}"

    website = row.get("website", "")
    if website and "?" in website:
        clean = re.sub(r"\?[^#]*", "", website)
        if len(clean) > 10:
            row["website"] = clean

    return row


def is_keyword_dump_address(street):
    if not street or not street.strip():
        return False
    s = street.strip()
    if len(s) > 100:
        return True
    if re.search(r"\.(jpg|jpeg|png|gif|pdf|webp|mp4)\)", s, re.I):
        return True
    if re.search(r"\b[a-f0-9]{20,}\b", s):
        return True
    if re.search(r"\b(award|million|billion|lawsuit|settlement|verdict|"
                 r"damages|plaintiff|defendant|case|appeal|result)\b", s, re.I):
        return True
    if re.search(r"\b(treated my|my sister|years? of practice|over \d+ years|"
                 r"highly recommend|excellent service|great attorney)\b", s, re.I):
        return True
    if len(s) > 40 and not re.search(r"\d", s[2:]):
        words = s.split()
        if len(words) > 5:
            return True
    return False


def is_out_of_state_seo(row):
    kscourts = row.get("legal_directory_listing", "").strip()
    gbp = row.get("google_business_profile", "").strip()
    name = row.get("law_firm_name", "").strip()
    phone = row.get("phone_number", "").strip()
    street = row.get("street_address", "").strip()

    if kscourts and "kscourts.gov" in kscourts:
        return False

    area = get_area_code(phone)
    if area and area in NONKS_AREA_CODES:
        if not gbp:
            return True

    if is_keyword_dump_address(street):
        return True

    if re.search(r"\b(coral springs|boca raton|fort lauderdale|miami|sarasota|"
                 r"naples|dallas|houston|austin|san antonio|new york|manhattan|"
                 r"chicago|los angeles|seattle|portland|denver|phoenix|"
                 r"minneapolis|atlanta|boston|charlotte|las vegas|"
                 r"philadelphia|washington d\.?c\.?|baltimore)\b", name, re.I):
        return True

    if re.search(r"\b(page \d+ of \d+|blog|article|post|notice|hearing|"
                 r"publication|pdf|names \w+ attorneys|names new partner|"
                 r"reviews from|developments|spotlight)\b", name, re.I):
        return True

    if "/" in name and not re.search(r"\b(law|legal|attorney|counsel)\b", name, re.I):
        return True

    if name.startswith(("'", '"', "“", "‘")):
        return True

    if re.match(r"^(home|contact|offices|services|lawyers|attorneys|results|about|"
                r"practice areas?|family law|litigation|meet us|our attorneys|"
                r"meet the team|careers|resources)\s*[-–]", name, re.I):
        return True

    if " – " in name and not any(x in name.lower() for x in ["law", "attorney", "legal"]):
        return True

    if re.search(r"\b(ohio|pennsylvania|nebraska|minnesota|connecticut|"
                 r"michigan|oregon|washington state|new jersey|illinois|"
                 r"tennessee|indiana|alabama|arkansas|colorado|utah)\b", name, re.I):
        return True

    return False


def is_junk(row):
    name = _normalize_quotes(row.get("law_firm_name", "").strip())
    name_lc = name.lower()
    kscourts = row.get("legal_directory_listing", "").strip()
    gbp = row.get("google_business_profile", "").strip()
    website = row.get("website", "").strip()
    city = row.get("city", "").strip()
    phone = row.get("phone_number", "").strip()

    # Remove non-Latin entries
    if re.search(r"[äöüÄÖÜßéèêàùâîôûç]|rechtsanwalt|kanzlei|firman\b(?! law| legal)", name, re.I):
        return True

    # REMOVE_EXACT always wins
    if name_lc in REMOVE_EXACT:
        return True

    # Government county entities
    if re.match(r"^\w[\w\s]+\s+county\s+(district\s+court(?:house)?|courthouse|"
                r"attorney'?s?\s+office?|attorney|magistrate\s+judge|judicial\s+center)\s*$",
                name, re.I):
        return True
    # City/municipality employers
    if re.match(r"^city\s+of\s+\w[\w\s]*$", name, re.I) and not LAW_INDICATORS.search(name):
        return True
    # Judicial districts (word and numeric ordinals)
    if re.search(r"\b(\d+th|\d+st|\d+nd|\d+rd|first|second|third|fourth|fifth|sixth|"
                 r"seventh|eighth|ninth|tenth|eleventh|twelfth|thirteenth|fourteenth|"
                 r"fifteenth|sixteenth)\s+judicial\s+district\b", name, re.I):
        if not re.search(r"\b(public defender|defender|legal services|legal aid)\b", name, re.I):
            return True
    # Raw county name
    if re.match(r"^\w[\w\s]+county\s*$", name, re.I) and not LAW_INDICATORS.search(name):
        return True

    # Always keep kscourts-sourced entries (after name filters)
    if kscourts and "kscourts.gov" in kscourts:
        return False

    # No city, no verifiable KS signal
    if not city and not kscourts:
        area = get_area_code(phone)
        ph_digits = re.sub(r"[^\d]", "", phone or "")
        is_us = bool(re.match(r"^1?[2-9]\d{9}$", ph_digits)) if ph_digits else False
        if not is_us or (area and area not in KS_AREA_CODES):
            return True
        if not area and not gbp:
            return True

    # SEO keyword patterns in name
    if re.search(
        r"(^results\s*[-–]|^services\s*[-–]|^contact\s*[-–]|^about\s*[-–]|"
        r"page \d+ of \d+|^\w+ county attorney$|^\w+ county courthouse$|^pdf |^publication notice)",
        name, re.I
    ):
        return True

    # "in central/south KS" pattern
    if re.search(r"\bin\s+(central|south.central|north.central|southwest|southeast|"
                 r"western|eastern|southern|northern)\s+(kansas|ks)\b", name, re.I):
        if not re.match(r"^(law office|office of|the \w+ law)", name, re.I):
            return True

    # Name > 80 chars
    if len(name) > 80:
        return True

    # Non-law business patterns
    if NON_LAW_PATTERNS.search(name) and not LAW_INDICATORS.search(name):
        return True

    # Government non-law entities
    if re.search(r"\b(city clerk|city hall|fire department|fire dept|"
                 r"water treatment|police department|county clerk|county courthouse|"
                 r"school district|sheriff'?s?\s+office|county sheriff)\b",
                 name, re.I) and not LAW_INDICATORS.search(name):
        return True

    # Out-of-state SEO spam
    if is_out_of_state_seo(row):
        return True

    # Street address patterns as name
    if re.match(r"^(us-?\d+\s*&|[a-z]{2}-?\d+\s*&|\d+\s+[nsew]\b)", name, re.I):
        return True
    if re.match(r"^[a-z][\w\s]+,\s+ks$", name, re.I) and not LAW_INDICATORS.search(name):
        return True

    # Medical professionals
    if re.search(r"\b(dds|dmd|md\b|do\b|pa-c\b|np\b|aprn\b|dentist|physician|"
                 r"chiropractic|optometrist|veterinar|orthopaedic|orthopedic|"
                 r"medical center|memorial medical|health center|regional hospital)\b",
                 name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Insurance companies
    if re.search(r"\b(american family insurance|farm bureau insurance|state farm|"
                 r"allstate insurance|farmers insurance|nationwide insurance|"
                 r"insurance service|insurance partner|insurance agency)\b", name, re.I):
        return True

    # CPA / tax prep
    if re.search(r"\b(cpa\b|certified public accountant|h&r block|jackson hewitt)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Chamber of commerce / title company
    if re.search(r"\b(chamber of commerce|title company|title co|county treasurer|"
                 r"county jail\b)\b", name, re.I):
        return True

    # Financial advisors
    if re.search(r"\b(edward jones|financial advisor|lpl financial|merrill lynch|"
                 r"raymond james|morgan stanley|ameriprise|northwestern mutual)\b", name, re.I):
        return True

    # Bail bonds
    if re.search(r"\bbail\s*(bond|bonds|bonding)\b", name, re.I):
        return True

    # Sheriff / police
    if re.search(r"\b(sheriff|police\s+dept|police\s+department|highway\s+patrol)\b", name, re.I):
        return True

    # Consulting firms (not law)
    if re.search(r"\bconsulting\s+(llc|inc|services|group|firm)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Commodities / utilities / wire services
    if re.search(r"\b(commodities|western union|money transfer|wire transfer|"
                 r"utility|electric company|telephone company|"
                 r"construction co|building products|bookkeeping|tax service|"
                 r"mental health|wellness center|parsonage|"
                 r"grain elevator|co-op elevator|feed store|"
                 r"bank\b(?! (loan|fraud|robbery)))\b", name, re.I):
        if not (kscourts and "kscourts.gov" in kscourts):
            return True

    # Energy companies
    if re.search(r"\b(refinery|petroleum|natural gas|pipeline|midstream|"
                 r"energy company|power company|electric cooperative)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # University / college / school employers
    if re.search(r"\b(university|college|community college|technical school|"
                 r"vocational|high school|elementary school|middle school|"
                 r"school district|usd\s+\d{3})\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Hospitals and medical systems
    if re.search(r"\b(hospital|medical center|health system|health care|"
                 r"healthcare|clinic|health center|nursing home|"
                 r"rehabilitation center)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Kansas City references (not central KS)
    if re.search(r"\bkansas\s+city\b", name, re.I):
        area = get_area_code(row.get("phone_number", ""))
        if not area or area not in KS_AREA_CODES:
            return True

    # Ambiguous cities: require local signal
    if city.lower() in AMBIGUOUS_CITIES:
        area = get_area_code(phone)
        if not gbp and (not area or area not in KS_AREA_CODES):
            if not area and not phone:
                return True
            if area and area in NONKS_AREA_CODES:
                return True
            if not phone and not gbp and LAW_INDICATORS.search(name):
                words = name.split()
                if len(words) > 7 or name.endswith("...") or "," in name[20:]:
                    return True

    return False


def _norm_firm(name):
    name = name.lower().strip()
    name = re.sub(r"\bp\.a\.", "pa", name)
    name = re.sub(r"\bp\.c\.", "pc", name)
    name = re.sub(r"\bl\.l\.c\.", "llc", name)
    name = re.sub(r"\bl\.l\.p\.", "llp", name)
    name = re.sub(r"\bp\.l\.l\.c\.", "pllc", name)
    name = re.sub(r"[,;.']", " ", name)
    name = re.sub(
        r"\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|the|and|&|of|at|"
        r"attorney|attorneys|lawyer|lawyers|chartered|chtd|co|inc|corp|company|"
        r"limited|ltd|incorporated|associates?)\b",
        "", name
    )
    return re.sub(r"[^a-z0-9]", "", name)


def _phone_digits(phone):
    d = re.sub(r"[^\d]", "", phone or "")
    return d[-10:] if len(d) >= 10 else None


_DEDUP_STOP = frozenset({
    "law", "firm", "office", "offices", "group", "llc", "llp", "pc", "pa", "pllc",
    "the", "and", "of", "at", "attorney", "attorneys", "lawyer", "lawyers",
    "chartered", "associates", "associate", "legal", "services", "center",
})


def _share_name_token(name1, name2):
    def tokens(n):
        return {w for w in re.findall(r'[a-z]{4,}', n.lower()) if w not in _DEDUP_STOP}
    return bool(tokens(name1) & tokens(name2))


def deduplicate(rows):
    def richness(r):
        return sum(1 for v in r.values() if v and str(v).strip())

    def merge_into(base, other):
        for k, v in other.items():
            if not base.get(k) and v:
                base[k] = v

    by_name_key = {}
    by_phone_key = {}
    kept = []

    for row in rows:
        name = row.get("law_firm_name", "").strip()
        city = row.get("city", "").strip().lower()
        norm = _norm_firm(name)
        name_key = (norm or name.lower()) + "|" + city
        phone_key = None
        ph = _phone_digits(row.get("phone_number", ""))
        if ph:
            phone_key = ph + "|" + city

        if name_key in by_name_key:
            idx = by_name_key[name_key]
            if richness(row) > richness(kept[idx]):
                merge_into(row, kept[idx])
                kept[idx] = row
            else:
                merge_into(kept[idx], row)
            if phone_key and phone_key not in by_phone_key:
                by_phone_key[phone_key] = idx
            continue

        if phone_key and phone_key in by_phone_key:
            idx = by_phone_key[phone_key]
            existing_name = kept[idx].get("law_firm_name", "")
            if _share_name_token(name, existing_name):
                if richness(row) > richness(kept[idx]):
                    merge_into(row, kept[idx])
                    kept[idx] = row
                    by_name_key[name_key] = idx
                else:
                    merge_into(kept[idx], row)
                by_name_key[name_key] = idx
                continue

        idx = len(kept)
        kept.append(row)
        by_name_key[name_key] = idx
        if phone_key:
            by_phone_key[phone_key] = idx

    return kept


def process_county(slug):
    path = DATA_DIR / f"{slug}.csv"
    if not path.exists():
        print(f"  [MISSING] {slug}.csv not found — skipping")
        return 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    original = len(rows)

    rows = [fix_fields(r) for r in rows]
    rows = [clean_website(r) for r in rows]
    rows = [clean_directory_listing(r) for r in rows]

    kept = [r for r in rows if not is_junk(r)]
    removed_junk = original - len(kept)

    before_dedup = len(kept)
    kept = deduplicate(kept)
    removed_dups = before_dedup - len(kept)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(kept)

    print(f"  {slug}: {original} → {len(kept)} (-{removed_junk} junk, -{removed_dups} dups)")
    return len(kept)


def main():
    print("Central Kansas County Cleanup\n" + "=" * 50)
    counts = {}
    for slug in CENTRAL_KS_COUNTIES:
        print(f"\n[{slug}]")
        n = process_county(slug)
        if n is not None:
            counts[slug] = n

    print(f"\n{'='*50}")
    print("Final counts:")
    total = 0
    for slug, n in counts.items():
        print(f"  {slug}: {n}")
        total += n
    print(f"  TOTAL: {total}")


if __name__ == "__main__":
    main()
