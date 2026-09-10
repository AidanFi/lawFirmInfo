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
    "dallas-county-tx": {"name": "Dallas", "state": "TX", "msa": "Dallas-Fort Worth"},
    "tarrant-county-tx": {"name": "Tarrant", "state": "TX", "msa": "Dallas-Fort Worth"},
    "bexar-county-tx": {"name": "Bexar", "state": "TX", "msa": "San Antonio"},
}

# The State Bar's "County" search field does not strictly mean "office is
# physically located in this county" — verified in the wild for Harris:
# searching County=Harris returned ~500 attorneys whose listed city is
# Dallas, Austin, San Antonio, The Woodlands (Montgomery Co.), Sugar Land
# (Fort Bend Co.), even New York/Chicago/London (a national firm's other-
# office attorneys whose bar record wasn't updated, or "county" reflecting
# something else like a bar-district/mailing address). A non-blank city
# outside the allowlist for that county is excluded outright — a county's
# file should not contain firms actually located elsewhere. A BLANK city
# is kept (can't disprove it's local, and the inclusion policy favors
# keeping incomplete-but-plausible entries over guessing). Per-county
# allowlist, since every county needs its own city list.
COUNTY_CITY_ALLOWLIST = {
    "harris-county-tx": {c.lower() for c in [
        "Houston", "Pasadena", "Baytown", "Pearland", "Deer Park", "La Porte",
        "Humble", "Katy", "Spring", "Cypress", "Tomball", "Channelview",
        "South Houston", "Galena Park", "Jacinto City", "Bellaire",
        "West University Place", "Southside Place", "Piney Point Village",
        "Hunters Creek Village", "Hedwig Village", "Bunker Hill Village",
        "Spring Valley Village", "Hilshire Village", "Jersey Village",
        "Friendswood", "Webster", "Seabrook", "Shoreacres", "Morgan's Point",
        "Nassau Bay", "Taylor Lake Village", "El Lago", "Highlands", "Crosby",
        "Huffman", "Atascocita", "Kingwood", "Aldine", "Klein", "Alief",
        "Fresno", "Barker", "Hockley",
    ]},
    "dallas-county-tx": {c.lower() for c in [
        "Dallas", "Irving", "Garland", "Mesquite", "Grand Prairie",
        "Richardson", "Carrollton", "DeSoto", "Cedar Hill", "Duncanville",
        "Lancaster", "Farmers Branch", "Coppell", "Addison",
        "University Park", "Highland Park", "Balch Springs", "Wilmer",
        "Hutchins", "Seagoville", "Sunnyvale", "Rowlett", "Sachse",
        "Glenn Heights", "Ovilla", "Cockrell Hill", "Wylie", "Lewisville",
        "Grapevine", "Combine", "Ferris",
    ]},
    "tarrant-county-tx": {c.lower() for c in [
        "Fort Worth", "Arlington", "North Richland Hills", "Mansfield",
        "Euless", "Bedford", "Hurst", "Haltom City", "Keller", "Southlake",
        "Colleyville", "Grapevine", "Watauga", "Saginaw", "Burleson",
        "Crowley", "Benbrook", "White Settlement", "Forest Hill",
        "Kennedale", "Everman", "River Oaks", "Sansom Park",
        "Westworth Village", "Edgecliff Village", "Lake Worth", "Pantego",
        "Dalworthington Gardens", "Westlake", "Trophy Club", "Blue Mound",
        "Haslet", "Richland Hills",
    ]},
    "bexar-county-tx": {c.lower() for c in [
        "San Antonio", "Alamo Heights", "Balcones Heights", "Castle Hills",
        "China Grove", "Converse", "Elmendorf", "Grey Forest", "Helotes",
        "Hill Country Village", "Hollywood Park", "Kirby", "Leon Valley",
        "Live Oak", "Olmos Park", "Schertz", "Selma", "Shavano Park",
        "Somerset", "St. Hedwig", "Terrell Hills", "Universal City",
        "Von Ormy", "Windcrest", "Fair Oaks Ranch",
    ]},
}
_CITY_ABBR_FIX = {
    "w univ pl": "west university place",
    "jersey vlg": "jersey village",
}


def _normalize_city(city: str) -> tuple[str, str]:
    """Returns (display, lookup_key) — strips trailing ', TX'/', TX 77002'
    /', DC' style suffixes and fixes known abbreviations, WITHOUT changing
    what county a city that legitimately is NOT in the target county
    resolves to."""
    c = (city or "").strip()
    c = re.sub(r",?\s*tx\b.*$", "", c, flags=re.IGNORECASE).strip()
    c = re.sub(r",?\s*dc\b.*$", "", c, flags=re.IGNORECASE).strip()
    key = _CITY_ABBR_FIX.get(c.lower(), c.lower())
    return c, key


def is_in_county(city: str, slug: str) -> bool:
    if not city or not city.strip():
        return True
    _, key = _normalize_city(city)
    return key in COUNTY_CITY_ALLOWLIST.get(slug, set())

# Self-reported "company" values that are not a real firm name — route
# these attorneys to the solo-practitioner bucket instead of merging
# unrelated people into a fake mega-firm.
PLACEHOLDER_COMPANY = {
    "self", "self employed", "self-employed", "selfemployed", "attorney",
    "attorney at law", "attorneys at law", "n a", "na", "none", "retired",
    "mr", "ms", "mrs", "dr", "sole practitioner", "solo practitioner",
    "unemployed", "individual", "private", "private practice", "unknown",
    "not applicable", "in house", "in-house", "law student", "student",
    "government", "unaffiliated", "solo", "solo practitioner", "law office",
    "law offices", "law firm", "the law office", "the law firm",
    "attorney counselor at law", "attorney and counselor at law",
    "counselor at law", "esq", "esquire", "select",
}

# Regex fallback for "no employer reported" variants that don't hit the
# exact set above ("None Reported", "None currently", etc.) — route to
# the solo bucket, same as a blank company field.
PLACEHOLDER_RE = re.compile(r'^(none|n/?a|unknown|not applicable)\b', re.IGNORECASE)

# A purely numeric "company" value (e.g. "1958") is a data-entry error —
# not a real firm name — route to the solo bucket like other placeholders.
NUMERIC_ONLY_RE = re.compile(r'^\d+$')

# An attorney's own street address typed into the "company" field by
# mistake (e.g. "301 Commerce Street, Suite 2001, Fort Worth, TX 76102")
# is not a real firm name — route to the solo bucket like other
# placeholders, same treatment as NUMERIC_ONLY_RE.
ADDRESS_LIKE_RE = re.compile(
    r'^\d+\s+\S+.*\b(street|st|ave|avenue|blvd|drive|dr|road|rd|lane|ln|suite|ste)\b',
    re.IGNORECASE,
)

# Self-declared non-practicing wording ("Retired", "Inactive", "Deceased")
# — still shows "Eligible to practice" bar status, but not a firm and not
# a currently-referable solo attorney either, so drop entirely.
NONPRACTICING_RE = re.compile(r'\bretire[ds]?\b|\binactive\b|\bdeceased\b|\bunaffiliated\b', re.IGNORECASE)

# Government / court / law-school entities — matched independently of
# "law"/"legal"/"attorney" wording, since agency names legitimately
# contain those words (e.g. "District Attorney's Office").
GOVT_PATTERNS = re.compile(
    r'(district\s+attorney|county\s+attorney|city\s+attorney|attorney\s+general|'
    r'district\s+atty\.?\b|county\s+atty\.?\b|'
    r'u\.?\s?s\.?\s+attorney|united\s+states\s+attorney|office\s+of\s+the\s+attorney|'
    r'public\s+defender|assigned\s+counsel|managed\s+counsel|domestic\s+relations|'
    r'county\s+clerk|district\s+clerk|county\s+court\s+at\s+law|'
    r'county\s+criminal\s+court|justice\s+of\s+the\s+peace|'
    r'\bconstable\b|sheriff.?s\s+office|police\s+department|fire\s+department|'
    r'municipal\s+court|probate\s+court|'
    r'child\s+protective\s+services|department\s+of\s+family|'
    r'independent\s+school\s+district|\bisd\b|school\s+district|'
    r'\bcity\s+of\s+\w|\bcounty\s+of\s+\w|'
    r'(harris|dallas|tarrant|bexar)\s+(county|co\.|cty\.?)(?!\s+.*(law|pllc|llp))|\bdallas\s+da\b|'
    r'\bcscd\b|dispute\s+resolution\s+center|'
    r'\bprecinct\s+\d+\b|'
    r'\bdist\.?\s+attys?\.?\s+ofc\b|\bdist\.?\s+atty\b|\bmagistrate\b|'
    r'juvenile\s+(probation|court|services)|family\s+court\s+services|'
    r'\bwater\s+district\b|employees.?\s+retirement\s+fund|'
    r'county\s+commissioner|'
    r'state\s+of\s+texas|texas\s+department|texas\s+legislature|texas\s+workforce|'
    r'texas\s+association\s+of\s+counties|'
    r'\bdepartment\s+of\s+\w|\bdept\.?\s+of\s+\w|\bdep\'t\s+of\s+\w|'
    r'internal\s+revenue\s+service|\birs\b|'
    r'social\s+security\s+administration|\bssa\b|office\s+of\s+hearings|federal\s+bureau|'
    r'u\.?\s?s\.?\s+department|port\s+houston|port\s+authority|port\s+of\s+houston|'
    r'u\.?\s?s\.?\s+district\s+court|united\s+states\s+district\s+court|'
    r'united\s+states\s+courts?\b|united\s+states\s+judiciary|'
    r'southern\s+district\s+of\s+texas|northern\s+district\s+of\s+texas|'
    r'eastern\s+district\s+of\s+texas|western\s+district\s+of\s+texas|'
    r'capital\s+habeas|merit\s+systems\s+protection\s+board|'
    r'army\s+corps?\s+of\s+engineers|'
    r'u\.?\s?s\.?\s+trustee|united\s+states\s+trustee|'
    r'office\s+of\s+the\s+u\.?\s?s\.?\s+trustee|united\s+states\s+courthouse|'
    r'court\s+of\s+appeals|supreme\s+court\s+of\s+texas|texas\s+supreme\s+court|'
    r'college\s+of\s+law|law\s+school|school\s+of\s+law|\buniversity\b|'
    r'\bjudge\b|\bhon\.?\s+[a-z]|\bjudicial\s+(district|court)\b|\bdistrict\s+courts?\b|'
    r'\bcourts?[\s-]+at[\s-]+law\b|'
    r'\bcivil\s+district\b|\bcriminal\s+district\b|\bfamily\s+district\b|'
    r'\bbankruptcy\s+courts?\b|\bbankruptcy\s+ct\b|\biv-d\s+court\b|\bcourt\s+receiver\b|'
    r'\blaw\s+clerk\b|\busao\b|office\s+of\s+(the\s+)?solicitor|'
    r'\bbar\s+association\b|\bcourt\s+reporting\b|\badministrative\s+judicial\s+region\b|'
    r'\btexas\s+business\s+court\b|\bcourt\s+administration\b|\bchildren.?s\s+court\b|'
    r'criminal\s+justice\s+center|\bhcao\b|\bhcdao\b|circuit\s+co?u?rt?\s+of\s+appeals|'
    r'fifth\s+circuit|foster\s+care\s+advocacy|'
    r'\bfdic\b|federal\s+reserve\s+bank|environmental\s+protection\s+agency|'
    r'\bfederal\s+judiciary\b|'
    r'u\.?\s?s\.?\s+securities\s+and\s+exchange\s+commission|'
    r'\bjag\s+corps\b|judge\s+advocate\s+general|'
    r'united\s+states\s+air\s+force|u\.?\s?s\.?\s+air\s+force|'
    r'united\s+states\s+army|united\s+states\s+navy|united\s+states\s+marine|'
    r'small\s+business\s+administration|'
    r'state\s+office\s+of\s+administrative\s+hearings|'
    r'executive\s+office\s+for\s+immigration\s+review|'
    r'national\s+labor\s+relations\s+board)',
    re.IGNORECASE,
)

# Blank-company attorneys (routed to the solo bucket, since we don't know who
# they work for) who happen to sit at a CONFIRMED single-purpose government
# building are almost certainly court/prosecutor/public-defender staff, not
# private practice — verified by checking every entry at each address: these
# five buildings had 0-2 exceptions out of 55-294 entries each, and every
# exception was itself a government/court entity with a company-field typo or
# unusual phrasing (already fixed above), never an actual private firm.
# Deliberately NOT extended to superficially similar downtown addresses that
# turned out to be ordinary mixed-use office towers with a government tenant
# on one floor and real law firms on others (500 Jefferson St has Littler
# Mendelson; 1010 Lamar St has Shepherd Smith Edwards & Kantas LLP; 515 Rusk
# St and 1301 Fannin St are similarly mixed) — blanket-excluding by street
# address there would wrongly drop real firms. Only extend this list after
# verifying zero private-firm tenancy the same way, per county.
# Per-county list of confirmed single-purpose government buildings (street
# prefixes). EMPTY by default for a county until the same per-address
# tenancy check has actually been run for it — never copy Harris's list
# into a new county without doing that check fresh, addresses don't
# transfer between counties.
DEDICATED_GOVT_BUILDINGS = {
    "harris-county-tx": [
        "1201 franklin", "1310 prairie", "1019 congress", "1400 lubbock",
        "4170 martin luther king",
    ],
    "dallas-county-tx": [
        "133 n riverfront", "600 commerce", "1100 commerce", "1500 marilla",
        "525 s griffin", "3315 daniel", "500 elm", "2014 main",
        "2600 lone star", "4050 alpha", "200 n 5th", "825 w irving",
        "106 s harwood",
    ],
    "tarrant-county-tx": [
        "401 w belknap", "401 west belknap", "100 n calhoun",
        "200 e weatherford",
    ],
    "bexar-county-tx": [
        "101 w nueva", "100 dolorosa", "300 dolorosa",
    ],
}


def is_at_dedicated_govt_building(street: str, slug: str) -> bool:
    # Punctuation-insensitive: self-reported addresses vary between
    # "200 E Weatherford" / "200 E. Weatherford" / "200 E. Weatherford St."
    # for the exact same building — strip periods before prefix-matching
    # so one list entry covers all of them.
    street = re.sub(r"\.", "", (street or "").strip().lower())
    if not street:
        return False
    return any(street.startswith(prefix) for prefix in DEDICATED_GOVT_BUILDINGS.get(slug, []))


# In-house-counsel job-title phrasing self-reported as the "company" field —
# only a non-law signal when no real firm-entity suffix is also present
# (guards against false positives like "General Counsel Consulting
# Solutions, PLLC", a real PLLC law firm).
INHOUSE_TITLE_RE = re.compile(
    r'general counsel|managing counsel|deputy counsel|chief legal officer|'
    r'\bsubsidiary of\b',
    re.IGNORECASE,
)

# Insurance carriers and "midstream"/"ventures" energy-investment companies
# — in-house employers, not referral firms. Guarded (unlike the harder
# CORP_NON_LAW_FRAGMENTS below): some insurers retain a captive staff-counsel
# "Law Office of X" that legitimately IS a real law office even though it
# names its sole client, e.g. "Law Office of Fareed Saba (Progressive
# Insurance)" (12 attorneys) — so this only excludes when no actual
# law-office/firm/attorney phrase is also present.
GUARDED_CORP_RE = re.compile(
    r'\bmidstream\b|\bventures\b|progressive insurance|state farm|\bgeico\b|'
    r'liberty mutual|nationwide insurance|farmers insurance|\ballstate\b|'
    r'discover financial|\busaa\b',
    re.IGNORECASE,
)
_EXPLICIT_LAW_OFFICE_RE = re.compile(
    r'law office|law firm|attorneys? at law|\bpllc\b|\bllp\b', re.IGNORECASE,
)

# General rule for the whole class of corporate in-house-counsel employers
# an exact-name list can never fully enumerate: a name ending in a bare
# generic corporate-entity word, with no law indicator anywhere in it, is
# essentially never a real referral law firm — verified in the wild
# (title companies, a distribution company, a trust company, a property
# company all matched this shape and were confirmed non-law). Real TX law
# firms use PLLC/LLP/P.C./P.A. or an explicit "Law"/"Attorney" word.
_GENERIC_CORP_SUFFIX_RE = re.compile(
    r'\b(company|corporation|incorporated|corp\.?|inc\.?|holdings|enterprises)\s*$',
    re.IGNORECASE,
)
_ANY_LAW_INDICATOR_RE = re.compile(
    r'\blaw\b|\blegal\b|\battorney|\bcounsel\b|\bpllc\b|\bllp\b|\bp\.?c\.?\b|\bp\.?a\.?\b|\besq\b|'
    r'(?:&|and)\s*associates\b|professional\s+corporation',
    re.IGNORECASE,
)

# "Law Center" is almost always a university's law school (Southern
# Methodist, Georgetown, etc.) — but verified in the wild that a real
# private firm can stylize itself the same way ("Wright Law Center
# PLLC"), so only flag it as institutional when there's no actual
# firm-entity suffix backing it up.
_LAW_CENTER_RE = re.compile(r'\blaw\s+center\b', re.IGNORECASE)

# "Justice Center" is usually a courthouse/government building name (Tim
# Curry Criminal Justice Center) or a nonprofit legal-aid org (Equal
# Justice Center, Tahirih Justice Center) — but a real solo PLLC can also
# brand itself this way ("Jump Start Legal Justice Center, PLLC", a real
# Dallas civil-rights litigation firm), so only flag it when there's no
# actual firm-entity suffix backing it up.
_JUSTICE_CENTER_RE = re.compile(r'\bjustice\s+center\b', re.IGNORECASE)

# Corporate / institutional in-house-counsel employers — not referral law
# firms. Substring match on distinctive company-name fragments (curated
# from the Harris County dataset; energy-sector heavy since Houston is an
# energy hub).
CORP_NON_LAW_FRAGMENTS = [
    "exxon", "chevron", "shell usa", "shell oil", "conocophillips", "bp america",
    "phillips 66", "marathon oil", "occidental petroleum", "kinder morgan",
    "plains all american", "energy transfer", "targa resources", "nrg energy",
    "technipfmc", "schlumberger", "halliburton", "baker hughes",
    "cheniere energy", "enterprise products", "hilcorp", "motiva",
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
    "saudi aramco", "axens north america", "ceva logistics", "cargill",
    "conga corporation", "contourglobal", "inception fertility",
    "intuitive machines", "net power", "pricewaterhousecoopers",
    "rsm us llp", "quinbrook infrastructure", "grant thornton",
    "bdo usa", "mckinsey", "boston consulting group", "bain & company",
    "repsol renewables", "saexploration", "swift current energy",
    "texas children's", "texas state teachers association", "voltagrid",
    "hewlett packard", "imperial star solar", "first american title",
    "title insurance company", "chicago title", "stewart title",
    "fidelity national title", "airswift", "reladyne", "commonspirit",
    "the women's home", "hca houston", "memorial hermann",
    "coastal prairie conservancy", "the harris center for mental health",
    "charles river associates", "distribution now", "distributionnow",
    "centerpoint energy", "pattern energy", "midland credit management",
    "texas health and human services", "harris central appraisal district",
    "national oilwell varco", "newquest properties", "northern trust",
    "perry homes", "technip energies", "transocean", "chord energy",
    "edp renewables", "jera americas", "equinor", "eor energy services",
    "nextera energy",
    # Dallas-specific in-house/institutional employers found via manual
    # spot-check of the largest no-website rows (a large lawyer count made
    # these easy to spot as non-referral: Goldman Sachs and other banks'
    # in-house counsel groups, hospital-system legal departments, asset-
    # management/investment-fund general counsel — Dallas is a finance/
    # banking and healthcare-system hub the way Houston is an energy hub).
    "goldman sachs", "citibank", "citigroup", "bank of america", "bank of texas",
    "bank ozk", "frost bank", "jpmorgan private bank", "origin bank",
    "plainscapital bank", "pnc bank", "regions bank", "susser bank",
    "bank of nova scotia", "vista bank", "b1 bank", "bmo harris bank",
    "federal home loan bank", "first guaranty bank", "texas regional bank",
    "texas capital bank", "texas capital",
    "sonic healthcare usa", "access healthcare", "ardent health services",
    "ashford hospitality advisors", "baylor health care system",
    "baylor scott & white health", "children's health system of texas",
    "conifer health solutions", "employer direct healthcare",
    "enhabit home health", "parkland health", "scp health",
    "tenet health systems", "texas health resources",
    "texas scottish rite hospital", "christus health",
    "caliber healthcare solutions", "agape home healthcare",
    "health care service corporation", "ut southwestern medical center",
    "cantex capital", "ridgepost capital", "spirit realty capital",
    "relevance capital management", "avad capital",
    "ackerman capital management", "affinius capital",
    "banner oak capital partners", "base capital funding",
    "black river capital", "carlson capital", "dt capital group",
    "evolve capital", "gap capital", "hbk capital management",
    "highland capital management", "insight capital group",
    "international capital, llc", "jpg capital", "k-star asset management",
    "knightvest capital", "longford capital management",
    "ngp energy capital management", "nexpoint advisors",
    "northmarq capital", "outlander capital", "p squared advisors",
    "pgim private capital", "pmb capital investments", "palmwood capital",
    "patent capital group", "preston hollow community capital",
    "sgf capital", "saxum capital partners", "silver spur capital partners",
    "strong capital", "suntx capital partners", "trive capital",
    "uptown capital advisors", "vwh capital management",
    "westmount realty capital", "l&b realty advisors", "dfw advisors",
    "highground advisors", "strata wealth advisors", "tiedemann advisors",
    "longo commercial advisors", "maverick capital",
    "american beacon advisors", "bland garvey wealth advisors",
    "american heart association", "oncor electric delivery",
    "southwest airlines", "title resources", "capital title of texas",
    "hudson advisors", "orix corporation", "bausch health",
    "catholic diocese of dallas", "sw electric",
    "southern glazer's wine and spirits", "state bar of texas",
    # Further Dallas in-house/institutional/investment employers found via
    # systematic manual triage of every 2+-attorney no-website row (WebSearch-
    # verified where the name alone was ambiguous, e.g. confirming "MMC" =
    # a staffing company and "ATI" = a materials manufacturer at the exact
    # address self-reported, not a coincidental abbreviation collision).
    "caris life sciences", "conduent business services", "brinker international",
    "dii asbestos trust", "dallas area rapid transit",
    "federal trade commission", "guidestone", "match group",
    "the beneficient company group", "united surgical partners international",
    "verizon", "dr horton", "fidelity investments", "army and air force exchange",
    "avanci", "benchmark title", "bessemer trust", "copart", "cyrusone",
    "dallas casa", "disability rights texas", "drivetime", "dominion harbor",
    "highlander partners", "kosmos energy", "leeward renewable energy",
    "santander consumer usa", "scout energy partners", "stream data centers",
    "tolleson wealth management", "u.s. anesthesia partners", "work shield",
    "builders firstsource", "vistra energy", "national life group",
    "solis mammography", "zurich north america", "verily life sciences",
    "axle funding", "adamas energy", "austin commercial", "communities foundation of texas",
    "consilio", "corgan", "dairy farmers of america", "deason criminal justice reform center",
    "digital realty", "ecobat", "elemetal", "fannie mae",
    "financial industry regulatory authority", "genesis women's shelter",
    "greystar", "hall group", "hbk investments", "headington companies",
    "hillwood", "international rescue committee", "invitation homes",
    "lincoln property co", "m financial", "m.d. anderson",
    "morgan stanley", "office of the comptroller of the currency",
    "petrus trust company", "riata corporate group", "sunoco",
    "toyota financial services", "turtle creek, a multi-family office",
    "u.s. office of special counsel", "us epa",
    "united states postal service", "vendera resources", "wwex group",
    "willis towers watson", "willow bridge", "xebec realty", "haggar clothing",
    "poly-america", "accenture", "berkshire hathaway automotive",
    "boy scouts of america", "dept homeland security-tsa",
    "draken international", "heidelberg materials", "invited clubs",
    "invited (formerly clubcorp)", "nissan north america", "pioneer natural resources",
    "planet home lending", "primesource building products", "qts data centers",
    "us citizenship and immigration services", "american contractors insurance group",
    "rocktop technologies",
    "b-29 family holdings", "topgolf", "weitzman", "skyview group",
    "raices", "dell technologies", "flexbase technologies",
    "nautilus group", "usaig", "island technology", "cetera financial group",
    "selene finance", "selene title", "marubeni-itochu steel group",
    "legal aid of northwest texas", "legal aid of north west texas",
    "catholic diocese of fort worth", "catholic charities fort worth",
    "dallas fort worth international airport", "fort worth counseling",
    "my health my resources of tarrant county", "mhmr of tarrant county",
    "multipurpose arena fort worth", "trail drive management corp",
    "united way of tarrant county", "tarrant regional water district",
    "american airlines", "bnsf railway", "cook children's health care system",
    "capital one, n.a.", "capital one", "general motors financial",
    "longbridge financial", "public investment fund", "r4 foundation",
    "array technologies", "fusion health", "texas health & human services",
    "texas health and human services", "pickering family foundation",
    "hfw capital partners",
    "federal government", "allied pilots association",
    "andrews preferred holdings", "bok financial", "bw gas & convenience holdings",
    "baylor scott and white health", "cbj financial", "cliff capital group",
    "creative solutions in healthcare", "daimler truck financial services",
    "diamondback industries", "e7 investments", "elbit systems of america",
    "element environmental resources", "federal aviation administration",
    "first command financial services", "frost : banking",
    "happy state bank", "jps health network", "jasper ridge partners",
    "kings branch resources", "longfellow energy", "mazur capital",
    "menalon capital", "mercedes-benz financial services",
    "mount olivet cemetery association", "north texas christian foundation",
    "olive cove partners", "onemain financial", "onemain solutions",
    "pacific legal foundation", "pine wave energy partners",
    "point energy partners", "post oak royalty partners", "q investments",
    "storm guard franchise systems", "toc energy resources",
    "us health group", "united educators association", "valleyview energy",
    "gst manufacturing", "integrated medical solutions",
    "integer health technologies", "albaron partners", "gauge capital",
    "n5b capital", "stetson investments", "trinity portfolio advisors",
    "alcon research", "alcon vision", "state national companies",
    "unleashed brands", "crescent real estate", "tpg global",
    "texas rangers baseball club", "firstcash", "interbank",
    "double eagle energy", "double eagle", "ferrovial construction", "ferrovial",
    "general services administration",
    "brooke army medical center", "cps energy",
    "catholic charities archdiocese of san antonio",
    "the archdiocese of san antonio", "goodwill san antonio",
    "haven for hope of bexar county", "kipp san antonio",
    "opportunity home san antonio", "port san antonio",
    "san antonio board of realtors", "san antonio community law ctr",
    "san antonio fire & police pension fund",
    "san antonio fire and police pension fund",
    "san antonio legal services association", "san antonio water system",
    "seaworld san antonio", "the children's hospital of san antonio foundation",
    "ut health san antonio", "ut san antonio",
    "via metropolitan transit", "texas riogrande legal aid",
    "southwest research institute", "h-e-b, lp", "h-e-b lp",
    "lewis energy group", "zachry group", "broadway bank",
    "alamo colleges district", "defense health agency",
    "group legal services", "las aguilas",
    "global war on terrorism memorial foundation", "panasonic corporation",
    "southland industries", "trane technologies",
    "briggs freeman sotheby's international realty", "neovia logistics",
    "softlayer technologies", "kubota credit corporation", "mlb properties",
    "the george w. bush foundation", "the o'donnell foundation",
    "dha housing solutions", "vistra corp", "level 2 legal solutions",
    "north texas litigation solutions", "employment practices solutions",
]
# Short/ambiguous tokens that need whole-word matching to avoid false positives
CORP_NON_LAW_WHOLE_WORDS = [
    "oxy", "slb", "kbr", "hines", "aramco", "pwc", "cargill", "ubs", "calpine", "bp",
    "dart", "ey", "finra", "lument", "ibm", "epa", "citi", "fossil", "mmc", "ati",
    "bnsf",
]

# Company names that are also common surnames — a whole-word or substring
# match would wrongly catch a PERSON whose name happens to contain the
# word (found in the wild: "Dolores Carina Valero" in Harris County is a
# real attorney with the surname Valero, not the energy company). Only
# exclude when the ENTIRE normalized name equals one of these, or is
# "Valero" + a distinctive corporate word — never a bare substring/whole-
# word match against an otherwise personal-name-shaped string.
_EXACT_NON_LAW_NAMES = {"valero"}
_VALERO_CORP_RE = re.compile(r'\bvalero\s+(energy|way)\b', re.IGNORECASE)

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
    "associates", "assoc", "assocs", "and", "the", "of", "a",
}


_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z])(?=[A-Z])")


def _tokens(name: str) -> list[str]:
    # Split camelCase BEFORE lowercasing (case is what marks the boundary) —
    # verified in the wild that the same real firm gets split into two
    # unmerged clusters when some attorneys self-report "BakerHostetler"
    # and others "Baker Hostetler" / "Baker & Hostetler".
    name = _CAMEL_BOUNDARY_RE.sub(" ", name)
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


# Confirmed-real law firms (via WebSearch, one at a time) whose names use
# a bare "&"/"and" join with an abbreviated corporate suffix (Corp./Assoc/
# Inc.) and no other recognizable law word — the exact shape shared by
# real non-law companies in the same TX county datasets (AT&T Services,
# Dave & Buster's, Bain & Company, Charles Schwab & Co., Anderson &
# Company, American National Bank and Trust Company...). Broadening
# _ANY_LAW_INDICATOR_RE to accept bare "&"/abbreviated suffixes generally
# was tried and rejected: it reintroduced those false negatives. Exact-
# match override is the safe fix — verify each new candidate via
# WebSearch before adding it here, never add on pattern-guess alone.
_CONFIRMED_LAW_FIRM_EXACT = {
    normalize_key(n) for n in (
        "Travis & Inman, A Professional Corp.",
        "Kris Terry & Assoc Inc",
        "Hoffmeyer & Grass, Inc.",
        "Mullen & Mullen, Inc",
    )
}


def is_non_law(name: str) -> bool:
    if not name:
        return False
    if normalize_key(name) in _CONFIRMED_LAW_FIRM_EXACT:
        return False
    if normalize_key(name) in _EXACT_NON_LAW_NAMES:
        return True
    if _VALERO_CORP_RE.search(name):
        return True
    if GOVT_PATTERNS.search(name):
        return True
    if CORP_NON_LAW_RE.search(name):
        return True
    if INHOUSE_TITLE_RE.search(name) and not LAW_FIRM_SUFFIX_RE.search(name):
        return True
    if GUARDED_CORP_RE.search(name) and not _EXPLICIT_LAW_OFFICE_RE.search(name):
        return True
    if _LAW_CENTER_RE.search(name) and not LAW_FIRM_SUFFIX_RE.search(name):
        return True
    if _JUSTICE_CENTER_RE.search(name) and not LAW_FIRM_SUFFIX_RE.search(name):
        return True
    if _GENERIC_CORP_SUFFIX_RE.search(name) and not _ANY_LAW_INDICATOR_RE.search(name):
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
            if is_at_dedicated_govt_building(e.get("street", ""), slug):
                dropped_placeholder += 1
            else:
                solos.append(e)
        elif NONPRACTICING_RE.search(company):
            dropped_placeholder += 1
        elif key in PLACEHOLDER_COMPANY or PLACEHOLDER_RE.search(company) or NUMERIC_ONLY_RE.match(company) or ADDRESS_LIKE_RE.match(company):
            if is_at_dedicated_govt_building(e.get("street", ""), slug):
                dropped_placeholder += 1
            else:
                solos.append(e)
        else:
            raw_groups[key].append(e)

    print(f"{len(raw_groups)} distinct raw company strings before fuzzy-merge, "
          f"{len(solos)} solo/placeholder attorneys, {dropped_placeholder} dropped as non-practicing")

    clusters = cluster_firms(raw_groups)
    print(f"{len(clusters)} firms after fuzzy-merge dedup")

    rows = []
    row_contact_ids_list: list[list[str]] = []
    today = date.today().isoformat()
    dropped_non_law = 0
    dropped_out_of_county = 0

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
        if not is_in_county(city, slug):
            dropped_out_of_county += 1
            continue
        city = _normalize_city(city)[0] or city
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
        row_contact_ids_list.append([m.get("contact_id", "") for m in members if m.get("contact_id")])

    dropped_solo_non_law = 0
    for e in solos:
        name = e.get("name", "").strip()
        if not name:
            continue
        if is_non_law(name):
            dropped_solo_non_law += 1
            continue
        solo_city = e.get("city", "")
        if not is_in_county(solo_city, slug):
            dropped_out_of_county += 1
            continue
        solo_city = _normalize_city(solo_city)[0] or solo_city
        rows.append({
            "law_firm_name": name,
            "website": "",
            "google_business_profile": "",
            "legal_directory_listing": "",
            "city": solo_city,
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
        row_contact_ids_list.append([e["contact_id"]] if e.get("contact_id") else [])

    for r in rows:
        for field in ("law_firm_name", "city", "street_address"):
            r[field] = re.sub(r"\s+", " ", r[field]).strip()

    # Sort rows and their parallel contact-id lists together so the
    # sidecar mapping (written below, keyed by final name|||city) stays
    # correct after reordering.
    order = sorted(range(len(rows)), key=lambda i: (rows[i]["city"], rows[i]["law_firm_name"]))
    rows = [rows[i] for i in order]
    row_contact_ids_list = [row_contact_ids_list[i] for i in order]

    out_path = DATA_DIR / f"{slug}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    sidecar = {
        f"{r['law_firm_name']}|||{r['city']}": ids
        for r, ids in zip(rows, row_contact_ids_list)
    }
    sidecar_path = CACHE_DIR / f"{slug}_row_contact_ids.json"
    sidecar_path.write_text(json.dumps(sidecar, indent=1))

    print(f"Dropped {dropped_non_law} non-law firm clusters, {dropped_solo_non_law} non-law solo entries, "
          f"{dropped_out_of_county} out-of-county entries")
    print(f"Wrote {len(rows)} firm rows to {out_path}")
    return len(rows)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 statebar_to_csv.py <slug>")
        sys.exit(1)
    build_csv(sys.argv[1])
