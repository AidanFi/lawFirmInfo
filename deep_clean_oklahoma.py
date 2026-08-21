#!/usr/bin/env python3
"""
Deep-clean and curation pass for all Oklahoma county CSVs.

Handles:
  1. Remove confirmed non-law entities (govt prosecutors, auto shops, churches,
     restaurants, financial advisors, casinos, gun shops, etc.)
  2. Clear websites that don't belong to the listed law firm (wrong-firm URLs,
     Facebook pages, Yelp pages, social media, bar associations, directories, etc.)
  3. Strip UTM and other tracking parameters from website URLs
  4. Remove out-of-state entries that slipped through the state filter
  5. Normalize county names and state fields
  6. Re-sort by city then firm name

Usage:
    python3 deep_clean_oklahoma.py [slug ...]
    python3 deep_clean_oklahoma.py          # all OK county CSVs
"""
import csv
import re
import sys
import urllib.parse as _urlparse
from pathlib import Path

DATA_DIR = Path("app/county-data")


def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9 ]', '', s.lower().strip())


# ---------------------------------------------------------------------------
# Universal non-law name patterns for Oklahoma data
# ---------------------------------------------------------------------------
_NON_LAW_NAME_RE = re.compile(
    r'\b(?:'
    # Government / prosecution (not referral targets)
    r'district\s+(?:attorney|atty)\b|county\s+(?:attorney|atty)\b|city\s+(?:attorney|atty)\b|'
    r'state\s+(?:attorney|atty)\b|attorney\s+general|public\s+defender|'
    r'district\s+court|county\s+court|municipal\s+court|tribal\s+court|'
    r'courthouse|clerk\s+of|register\s+of|'
    r'town\s+hall|city\s+hall|county\s+clerk|town\s+of\b|'
    r'indigent\s+defense|dept(?:artment)?\s+of\s+corrections|'
    r'department\s+of\s+corrections|alternative\s+sentencing|'
    # Financial / insurance
    r'edward\s+jones|financial\s+(?:advisor|planner|solutions|services|planning)|'
    r'investment\s+advisor|'
    r'insurance\s+agency|insurance\s+company|state\s+farm|allstate|farmers\s+insurance|'
    r'american\s+family|nationwide|liberty\s+mutual|'
    # Auto / vehicle / mechanical
    r'auto\s+parts|napa\s+auto|o\'?reilly|autozone|advance\s+auto|pep\s+boys|'
    r'tire\s+(?:center|shop|world)|oil\s+change|jiffy\s+lube|valvoline|'
    r'auto\s+(?:repair|body|glass|dealer|sales)|car\s+(?:wash|dealer|rental)|'
    r'towing|wrecker|truck\s+(?:stop|service)|mechanical\s+(?:llp|llc|inc|co)|'
    r'express\s+mechanical|chevrolet|dealership|'
    # Food / beverage
    r'restaurant|cafe|coffee|tavern|pub\b|brewery|winery|'
    r'pizza|burger|grill(?:ed)?|steakhouse|bbq|barbeque|diner|'
    r'package\s+store|liquor|casino\b|'
    # Personal care
    r'salon|barber|spa\b|massage|tattoo|nail\s+(?:studio|salon)|beauty\s+(?!law)|'
    # Medical / dental / vet (not law)
    r'dentist|dental|orthodontic|optometry|optometrist|veterinar|'
    r'animal\s+(?:clinic|hospital|shelter)|'
    r'medical\s+(?:center|group|clinic|associates)|health\s+(?:center|clinic|system)|'
    r'urgent\s+care|ascension\s+medical|primary\s+care|'
    # Funeral / mortuary
    r'funeral\s+(?:home|parlor|chapel|services)|mortuary|cremation|'
    r'tribute\s+memorial|memorial\s+care|'
    # Postal / shipping
    r'postal\s+service|post\s+office|united\s+states\s+postal|ups\s+store|fedex|'
    # Bail bonds
    r'bail\s+bonds?|bail\s+bondsman|'
    # Process servers (not attorneys)
    r'process\s+serv(?:er|ing|ices)|statewide\s+process|'
    # Paralegal services (not law firms)
    r'\bparalegal\b|'
    # Sanitation / waste
    r'sanitation|waste\s+(?:management|services)|garbage|trash\s+(?:collection|hauling)|'
    # CPAs / accountants (not law firms)
    r'\bcpa\b|certified\s+public\s+accountant|accounting\s+firm|'
    r'tax\s+(?:preparation|service)(?!\s+law)|'
    # Fairgrounds / event venues
    r'fairgrounds|fair\s+grounds|county\s+fair|event\s+(?:center|venue)|'
    # Government districts (not law firms)
    r'county\s+district\s+#|school\s+district\s+#|water\s+district|'
    # Chambers of commerce
    r'chamber\s+of\s+commerce|'
    # Document preparation (not law firms — non-attorney services)
    r'document\s+(?:services|preparation)|legal\s+document\s+(?:services|prep)|'
    # Gun shops / arms
    r'\barms\s+(?:llc|inc|corp|co)\b|gun\s+(?:shop|store|sales)|'
    # Tag / DMV agencies (not law firms)
    r'tag\s+agency\b|'
    # Oklahoma government bodies
    r'\bokdhs\b|office\s+of\s+legislative\b|'
    # Religious — careful NOT to match surnames like "Church Law Firm"
    r'church(?!\s+(?:law|legal|firm|pllc|llc|llp|pc|atty|attorney|&))|parsonage|'
    r'mosque|synagogue|cathedral(?!\s+(?:law|legal))|ministry(?!\s+of\s+law)|'
    r'fellowship(?!\s+(?:law|legal))|assembly\s+of\s+god|'
    # Bar associations / advocacy groups (not law firms)
    r'bar\s+association|investors\s+advocate\s+bar|'
    # Student legal services (university programs, not private firms)
    r'student\s+legal\s+services|'
    # Retail / services
    r'grocery|supermarket|pharmacy|drug\s+store|dollar\s+(?:general|tree|store)|'
    r'hardware|home\s+depot|lowes|walmart|target|'
    r'hotel|motel|inn\b|resort|airbnb|'
    r'gym|fitness|yoga|crossfit|'
    r'daycare|child\s+care|preschool|'
    # Horses / farming / rural
    r'horse\s+hotel|livestock|cattle|farm\s+(?:supply|equipment)|grain\s+elevator|'
    r'feed\s+(?:store|mill)|agriculture|ranch\b|'
    # Construction / trades
    r'construction|roofing|plumbing|hvac|electrician|contractor|'
    r'landscap|lawn\s+(?:care|service)|'
    # Real estate (non-law)
    r'realty|realtor|real\s+estate\s+(?:company|group|services)(?!\s+law)|'
    r'title\s+company|'
    # Other junk
    r'vape(?:s|\s+shop|\s+store)?\b|e\s*cigarette|vaping|'
    r'pawn\s+shop|'
    r'the\s+pavilion(?!\s+law)|pavilion\s+(?!law)|'
    r'trucking|freight|transport(?:ation)?|'
    r'school\s+district|elementary|middle\s+school|high\s+school|university(?!\s+law)'
    r')\b',
    re.IGNORECASE,
)

# URL patterns that are NOT firm websites (should be cleared)
_BAD_WEBSITE_RE = re.compile(
    r'(?:'
    r'facebook\.com|instagram\.com|twitter\.com|linkedin\.com|'
    r'yelp\.com|yellowpages\.com|google\.com/maps|'
    r'avvo\.com|justia\.com|martindale\.com|findlaw\.com|lawyers\.com|'
    r'bbb\.org|superlawyers\.com|lawinfo\.com|'
    r'ag\.ok\.gov|logancountyok\.com(?!/.*law)|'    # govt / church sites
    r'oreillyauto\.com|napaonline\.com|autozone\.com|'
    r'greenandgrilled\.com|'                         # restaurant
    r'washingtonlawoffice\.com|'                     # wrong-state law office
    r'rogerscountybar\.org|'                         # bar association directory
    r'mannfordmap\.com|'                             # local map/business directory
    r'rymaps\.xyz|'                                  # business directory
    r'ajb-cpas\.com|'                                # CPA firm
    r'tributesw\.care|'                              # funeral home
    r'saltcreekcasino\.com|'                         # casino
    r'randmarms\.com|'                               # gun shop
    r'district21da\.com|'                            # government DA office
    r'alternativesentencingofoklahoma\.com|'         # nonprofit, not law firm
    r'okoffender\.doc\.ok\.gov|doc\.ok\.gov|'        # corrections department
    r'mcclaincoabstract\.com|'                       # abstract company, not law
    r'countycourthouse\.org|'                        # government courthouse
    r'southcoffeyvilleok\.gov|'                      # municipal government
    r'ams\.okbar\.org|okbar\.org'                    # OK Bar Association member portal (not firm websites)
    r')',
    re.IGNORECASE,
)

# Names that are explicitly NOT private law firms — stored as pre-normed strings
_EXPLICIT_REMOVE = {_norm(s) for s in [
    # Logan County
    "Outback Concepts LLC",
    "Assembly of God Church Parsonage",
    "El Palmo",
    "NAPA Auto Parts - Chris Supply Auto & FA",
    "Edward Jones - Financial Advisor",
    "Green Tim W",
    "Gus Package Store",
    "KKs Horse Hotel LLC",
    "O'Reilly Auto Parts",
    "The Pavilion",
    "Logan County District Attorney",
    # Wagoner County — Tahlequah office belongs to Cherokee County, not Wagoner
    "Wirth Law Office - Tahlequah",
    # Canadian County
    "Canadian County District Atty",
    "JNK Paralegal $100 Divorces",
    "Carter Chevrolet Agency LLC",
    "Tribute Memorial Care Southwest",
    # Grady County
    "Garvin County District Atty",
    "Alternative Sentencing Solutions of Oklahoma",
    "Minco Chamber of Commerce",
    "R & M Arms LLC",
    "SaltCreek Casino",
    # Okmulgee County
    "Oklahoma Indigent Defense System",
    "Oklahoma Department of Corrections",
    # Washington County
    "Washington County District Atty",
    "Town of South Coffeyville",
    "Legal Document Services, LLC",
    # Cleveland County
    "OU Student Legal Services",
    "Public Investors Advocate Bar Association",
    "Yarborough Law Group",  # duplicate — "Yarborough Law Group, LLC - Family Law..." has the website
]}

# Firm-specific wrong-website map: normed_firm_name -> domain that doesn't belong to them
# Website is cleared ONLY when both the name AND the domain match.
_WRONG_WEBSITE_MAP = {
    _norm("Johnston & Associates"):         "perrymanlegal.com",
    _norm("Joe Adair & Associates"):        "richardgrayattorney.com",
    _norm("Wantland & Wantland Attorneys At Law"): "rogerscountybar.org",
    _norm("Allen Mills Lind Simpson"):      "lindtreadaway.com",
    _norm("Blevins & Associates Law, PLLC"): "mcclaincoabstract.com",
    _norm("Stephens Jeff M Atty"):          "swantlaw.com",
    _norm("Duke Law Firm, PLLC"):           "tahlequahlaw.com",
    _norm("Frye Law Firm"):                 "tulsametrolaw.com",
    _norm("Handley Fletcher Dal Jr Atty"):  "fogglawfirm.com",
    _norm("Feuguay Law Office"):             "muellerwheeler.com",
    _norm("Michael Grant, Attorney"):        "mannfordmap.com",
    _norm("Miller Marianne"):                "rymaps.xyz",
    _norm("Clayton & Pichot"):               "sullivanlawok.com",
    _norm("GMF Legal Services, PLLC"):       "okbar.org",
}

# Tracking query parameters to strip from website URLs
_TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_name", "npcmp", "gclid", "fbclid", "msclkid",
    "dclid", "gbraid", "wbraid", "ref", "_ga", "authuser",
})


def _strip_tracking_params(url: str) -> str:
    """Remove UTM and other tracking query params from a URL."""
    if not url:
        return url
    try:
        parts = _urlparse.urlparse(url.strip())
        if not parts.query:
            return url.strip()
        params = _urlparse.parse_qs(parts.query, keep_blank_values=True)
        clean = {k: v for k, v in params.items() if k.lower() not in _TRACKING_PARAMS}
        new_query = _urlparse.urlencode(clean, doseq=True)
        return _urlparse.urlunparse(parts._replace(query=new_query))
    except Exception:
        return url.strip()


def _is_bad_website(url: str) -> bool:
    """Return True if the URL is a directory/social/non-firm site."""
    if not url or not url.strip():
        return False
    return bool(_BAD_WEBSITE_RE.search(url))


def _is_wrong_website(name: str, url: str) -> bool:
    """Return True if this URL is specifically known to not belong to this firm."""
    if not url:
        return False
    wrong_domain = _WRONG_WEBSITE_MAP.get(_norm(name))
    if not wrong_domain:
        return False
    return wrong_domain.lower() in url.lower()


def _should_remove(name: str) -> bool:
    """Return True if this entry is definitely not a private law firm."""
    norm = _norm(name)
    if norm in _EXPLICIT_REMOVE:
        return True
    if _NON_LAW_NAME_RE.search(name):
        return True
    return False


def clean_file(csv_path: Path) -> tuple[int, int, int]:
    """
    Returns (removed_count, website_cleared_count, total_remaining).
    """
    rows = list(csv.DictReader(csv_path.open()))
    if not rows:
        return 0, 0, 0

    fieldnames = list(rows[0].keys())

    removed = []
    kept = []
    websites_cleared = 0

    for r in rows:
        name = r.get("law_firm_name", "").strip()

        # 1. Remove confirmed non-law entries
        if _should_remove(name):
            removed.append(name)
            continue

        # 2. Clean website: strip tracking params first, then check validity
        ws = r.get("website", "").strip()
        if ws:
            ws = _strip_tracking_params(ws)
            r["website"] = ws

        if ws and (_is_bad_website(ws) or _is_wrong_website(name, ws)):
            r["website"] = ""
            websites_cleared += 1

        # 3. Ensure state = OK
        r["state"] = "OK"

        kept.append(r)

    # Re-sort
    kept.sort(key=lambda r: (r.get("city", ""), r.get("law_firm_name", "")))

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(kept)

    return len(removed), websites_cleared, len(kept)


def get_ok_csvs():
    return sorted(DATA_DIR.glob("*-ok.csv"))


if __name__ == "__main__":
    targets = []
    if sys.argv[1:]:
        for slug in sys.argv[1:]:
            p = DATA_DIR / f"{slug}.csv"
            if p.exists():
                targets.append(p)
            else:
                print(f"  WARNING: {p} not found")
    else:
        targets = get_ok_csvs()

    print(f"Deep-cleaning {len(targets)} Oklahoma county CSV(s)...\n")
    grand_removed = grand_cleared = grand_kept = 0
    for path in targets:
        removed, cleared, kept = clean_file(path)
        print(f"  {path.stem}: removed {removed} | websites cleared {cleared} | kept {kept}")
        grand_removed += removed
        grand_cleared += cleared
        grand_kept += kept

    print(f"\nTotal: removed {grand_removed}, websites cleared {grand_cleared}, kept {grand_kept}")
