#!/usr/bin/env python3
"""
Cleanup for 13 new SE Kansas counties.
Removes non-law businesses, government entities, keyword dumps,
and other junk that the Google Places pipeline picked up.
"""
import csv, re, json
from pathlib import Path

DATA_DIR = Path(__file__).parent / "app/county-data"
MANIFEST_PATH = DATA_DIR / "manifest.json"

SE_KS_COUNTIES = [
    "allen-county-ks",
    "bourbon-county-ks",
    "chautauqua-county-ks",
    "cherokee-county-ks",
    "coffey-county-ks",
    "crawford-county-ks",
    "elk-county-ks",
    "greenwood-county-ks",
    "labette-county-ks",
    "montgomery-county-ks",
    "neosho-county-ks",
    "wilson-county-ks",
    "woodson-county-ks",
]

# Kansas area codes
KS_AREA_CODES = {"316", "620", "785", "913"}

# Out-of-state area codes that strongly suggest non-KS firm
NONKS_AREA_CODES = {
    "954", "239", "561", "305", "407",  # Florida
    "214", "972", "469", "713", "832",  # Texas
    "516", "212", "718", "646",         # New York
    "727", "813", "941", "786",         # Florida (more)
    "702", "775",                        # Nevada
    "303", "720",                        # Colorado
    "312", "773",                        # Illinois
    "404", "770",                        # Georgia
    "602", "480", "623",                 # Arizona
    "617", "508",                        # Massachusetts
    "414", "262",                        # Wisconsin
    "513", "216",                        # Ohio
    "678",                               # Georgia
    "402", "531",                        # Nebraska
    "612", "651", "763", "952",          # Minnesota
    "614", "440", "740", "419",          # Ohio
    "215", "610", "717", "412", "267",    # Pennsylvania
    "248", "313", "734", "517",          # Michigan
    "401",                               # Rhode Island
    "203", "860",                        # Connecticut
    "202", "301", "240",                 # Washington DC / Maryland
    "703", "571", "804", "757",          # Virginia
    "503", "971",                        # Oregon
    "206", "253", "425",                 # Washington
    "415", "510", "408",                 # California Bay Area
    "310", "323", "213",                 # Los Angeles
    "512", "737",                        # Austin TX (in addition to Dallas codes)
    "418", "514",                        # Quebec (wrong country)
    "816", "573", "636",                 # Missouri (816=KC MO, 573/636=central MO)
    "416", "647", "437", "905",          # Ontario, Canada (Toronto)
    "604", "778", "236", "250",          # British Columbia, Canada
    "201", "609", "856", "732", "848",   # New Jersey
    "336", "704", "919", "828",          # North Carolina
    "319", "515", "563",                 # Iowa
    "844", "855", "866", "877", "888",  # Toll-free (often out-of-state)
    "800",                               # Toll-free
    "760", "442",                        # Palm Springs / Inland Empire CA
    "443", "410",                        # Baltimore / Maryland
    "805",                               # Ventura County CA
    "315",                               # Syracuse NY
    "902",                               # Nova Scotia Canada
    "225",                               # Baton Rouge LA
    "801", "385",                        # Salt Lake City / Utah
    "918",                               # Tulsa OK
    "479", "501", "870",                 # Arkansas
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
    r"excavat|welding|manufacturing|midstream|pipeline|energy|oil & gas|"
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
    r"park\b(?! st| ave| dr| rd| pl| blvd| way| ct|s | avenue)|"
    r"museum|library|"
    r"family services|social services|poverty task force|"
    r"consulting llc(?! law| legal)|consulting services|"
    r"towing & recovery"
    r")\b",
    re.IGNORECASE
)

# Exact names to always remove (lowercase)
REMOVE_EXACT = {
    # Non-law businesses
    "thrive allen county",
    "haire patrick cabinet maker",
    "irishman excavating & trucking llc",
    "magellan midstream partners",
    "papa's shop dba papa's custom furniture",
    "st joseph parish office",
    "cam's custom goods llc",
    "counseling therapy",
    "microtronics, llc",
    "parkers towing & recovery, llc",
    "petals by pam",
    "tfi family services inc",
    "apt law offices",  # Allen Poverty Task Force, not a law firm
    "tecchio consulting llc",
    "gregory gilbert e",  # website is wardmfg.com (manufacturing)
    "gunn park (fort scott)",
    "krrp ks rocks park (fort scott)",
    "krrp ks rocks park",
    "uniontown united methodist church parsonage",
    "saint paul mission township fire department",
    "st paul high school",
    "st paul supermarket",
    "st paul water treatment plant",
    "moran city clerk",
    "moran city clerk",
    # Government entities / non-law kscourts employers (always remove even if kscourts-sourced)
    "allen county attorney", "allen county courthouse",
    "bourbon county attorney", "bourbon county courthouse", "bourbon county district court",
    "bourbon county attorney's office",
    "chautauqua county attorney", "chautauqua county",  # raw county name = non-law kscourts employer
    "14th judicial district - state of kansas",  # 918 area code (Tulsa OK), not a KS firm
    "cherokee county attorney", "cherokee county district court",
    "crossland construction co., inc.",  # construction company (Cherokee)
    "american bank",  # Cherokee
    "coffey county attorney", "coffey county district court",
    "crawford county attorney", "crawford county district court",
    "elk county attorney",
    "greenwood county attorney", "greenwood county district court",
    "labette county attorney",
    "montgomery county attorney",
    "neosho county attorney",
    "wilson county attorney", "wilson county district court",
    "woodson county attorney", "woodson county courthouse",
    "state of kansas",  # generic government employer (Crawford, Montgomery)
    "pittsburg state university",  # university employer (Crawford)
    "community health center of se kansas, inc",  # medical center employer (Crawford)
    "community health center of southeast kansas, inc.",
    "watco",  # railroad company (Crawford)
    "tfi family services inc",  # foster care agency (Allen)
    # Elk county junk (triggered by "Howard KS" Google Places results)
    "attorney howard sanger",  # Palm Springs CA (760 area code)
    "common law marriage & estate planning challenges",  # MD article (443 area code)
    "contact the law offices of john h. howard",  # CA contact page (805 area code)
    "international lawyer resources",  # MD attorney directory (410 area code)
    # Montgomery county junk
    "custom innovations paint and body", "the tribal domicile", "bug busters usa llc",
    "american family insurance - linda frazier", "coffeyville orthopaedics p.a.",
    "farm bureau insurance", "local attorney: index", "lawn works", "sek leaf & lawn",
    "sharper images llc", "indy pumpkin patch and corn maze", "hillcrest inn",
    "united states postal service", "elk county district court clerk",
    "elk county and greenwood county attorney", "foster law office",  # wrong county
    "southeast kansas public defender office", "mongomery county attorney's office",
    "montgomery county courthouse", "medicalodges inc",
    "personal injury law firm in las vegas, nv by jamel perry at ...",  # Las Vegas NV firm via Google SEO
    "ingersoll law office: experienced personal injury lawyer",  # 913 number → Johnson County, not Independence/Montgomery
    # Greenwood county junk
    "acrisure midwest partners insurance services llc", "harvest house food pantry",
    "lacerra, dickson, hoover, & rogers pllc, little rock, ar",
    # Wilson county junk
    "jeff hull's paving & seal coating", "prairie nut hut",
    "sek public defender",  # wrong county (Chanute = Neosho County)
    "newkirk, dennis and buckles inc",  # Independence MO-area, not Wilson County
    "wilco veterinary clinic pa", "wilson county attorney office",
    "wilson county data processing", "scoops ice cream parlor and sandwich shop",
    "nancy j. ingle",  # Pittsburg/Crawford County attorney listed in Wilson
    # Crawford county junk
    "aaron's lawn serivce",  # misspelled "service", lawn care not law
    "edward battitori",  # corrupted city field ("Cypress Street Cherokee"), dup of Battitori Edward J Law Office
    "arma health and reahb", "arma ballpark", "cutie patootie crafts",
    "exterior escapes llc.", "frontenac parks & recreation",
    "monica r kellogg cpa llc", "steve gintner painting",
    "crawford county attorney's office", "crawford county. kansas",
    "crawford county judicial center",
    "girard retail & pawn", "h&r block", "tadpole painting",
    "j b's lawns & trees", "jake's fireworks inc", "lawson auto service",
    "nate's lawn & landscape", "pittsburg law enforcement center",
    "the lawn", "commerce trust", "loy kurtis i attorney, attorney, 511 s georgia st",
    "names and numbers", "murphy's wheat law - wildcat district",
    "young williams child support services", "young williams", "young williams cse",
    "atropos arms llc", "jennifer brunetti",
    # Neosho county junk
    "hi-lo ind. inc", "kansas department for children & families",
    "sherwin-williams paint store", "st paul clinic", "st paul tire & lube",
    "big ed's steak house", "thayer city office",
    "legal, banking & insurance",
    "catherine lerner - murphy law group, llc",  # Philadelphia PA firm (267 area code) triggered by "Erie" KS
    "guides at georgetown law library: u.s. law, research process ...",  # DC law library, not a KS firm
    "wllbert & towner pa",  # typo variant of Wilbert & Towner; Pittsburg firm wrongly placed in Chanute
    "thuston linus",  # dup of "Linus A. Thuston, Attorney at Law" (kscourts); different phones can't auto-dedup
    "neosho memorial medical center",  # medical center kscourts employer
    # Labette county junk (Parsons KS triggered out-of-state results)
    "bartlett co-op association - bartlett",  # grain cooperative
    "usd 505 chetopa - st paul",  # school district
    "contact syracuse medical malpractice attorney janet izzo",  # Syracuse NY (315 area code)
    "danielle m. hall – institute for well-being in law",  # law wellness org, not KS firm
    "danielle m. hall - institute for well-being in law",
    "judge david waxse - legal talk network",  # podcast bio page
    "labette county law enforcement center and e.o.c.",  # law enforcement building
    "syracuse estate planning attorney frederick p. davies explains key ...",  # NY article headline
    "taylor news papers",  # newspaper
    "advance america",  # payday loan company
    "angela walker, k.c. - mdw law",  # Nova Scotia Canada firm (902 area code)
    "express inn parsons",  # hotel
    "firms - scg legal",  # Baton Rouge LA directory (225 area code)
    "green country ford of parsons",  # car dealership
    "kitchen pass",  # restaurant
    "memorial lawn cemetery",  # cemetery
    "utah state list: elder law, medicaid, and estate planning services",  # Utah directory (801 area code)
    # Crawford county additional junk
    "henry law firm",  # address field contains review text, no city/zip
    "jason peak - laxalt law group",  # Nevada firm; address contains image filename
    # Greenwood county junk (keyword-dump addresses that bypassed the check)
    "attorney zackery e. reynolds",  # address = review testimonial text
    "farris legal services llc: home",  # nav artifact name + garbage address
    # Cherokee county junk (duplicates)
    "gene barrett law office",  # same as Barrett Law Office, same phone
    "gay parita",
    "pool side paradise",
    "peddler's junction",
    "spring river mental health & wellness, inc.",
    "tamko building products llc",
    "galena bookkeeping & tax service",
    "first baptist parsonage",
    "district magistrate judge",
    "cherokee county attorney'soffice",
    "kingrey-kellum agency, inc.",
    "diana marshall, pa",  # physician's assistant (mercy.net profile), not an attorney
    # Montgomery county additional junk
    "reynolds daniel m",  # Daniel Reynolds is a member of Emert Chubb LLC; firm name is the referral target
    "patricia a. horton, pa",  # medical professional (ascension.org profile), not an attorney
    "kritz paul m",  # attorney name used as firm name; correct firm is Hall Levy DeVore Bell Ott & Kritz, PA
    # Chautauqua county junk
    "western union",
    "wagnon commodities",
    # Woodson county junk
    "silverado's",
    "cva services llc",
    "country junction inc",
    "cross eyed catfish trading co.",
    "natural pathways",
    "woodson county title",
    "island parad-ice",
    "attorney criminal defense",
    # Coffey county junk
    "trimble & maclaskey oil, llc",
    "sticks & weeds n' wallpaper things",
    "united parcel service of america",
    "tropical paradice",
    "green pastures",
    "coffey county attorney's office",
    "coffey county jail",
    "american institutes for research",
    "sedan assembly-god parsonage",
    "comanche county sheriff's office",
    "chautauqua county sheriff's office",
    "chautauqua county",
    "boulton law firm - kansas city",
    "apt law offices, attorney, 219 south st",
    # SEO/directory pages
    "law firm trust account / iolta resources",
    "services - hf law",
    "able bail bonding",
    "dr. jennifer l. johnson, dnp, aprn johnson legal nurse",
    "nationwide personal injury & ssd law services",
    "scott marshall injury attorneys",
    "garfunkel wild",
    # Other
    "edward jones - financial advisor",
    "edward jones",
    "st paul mission township fire department",
}

# Non-law websites to clear (but keep the entry if it has kscourts or GBP)
NON_LAW_DOMAINS = {
    "magellanlp.com", "sjpmv.org", "stpaulmarketks.com", "morancity.org",
    "thriveallencounty.org", "irishmanexc.com", "petalsbypamfloral.com",
    "microtronicscontrols.com", "usd505.org", "edwardjones.com",
    "facebook.com", "ksrockspark.com", "lawfirmvelocity.com",
    "czladiesconnection.com", "thelegaldescription.com", "stpaul-ks.net",
    "wardmfg.com", "aptv.org", "ablebailbonding.com", "garfunkelwild.com",
    "bourboncountyks.org",  # county government website
    "chieftain.com",  # newspaper
    "odvip9.cc",  # spam domain (Loy Law Firm, Crawford)
    "jobsbr.info",  # job posting site (Schneider Law Offices, Montgomery)
    "mercy.net",  # hospital/medical (Diana Marshall PA, Cherokee)
    "ascension.org",  # hospital system (Patricia Horton PA, Montgomery)
    "healthcare.ascension.org",
}

# Websites that are directory pages / news articles (not the firm's actual site)
DIRECTORY_WEBSITE_PATTERNS = re.compile(
    r"(martindale\.com|avvo\.com|findlaw\.com|justia\.com|"
    r"lawyer-map\.com|experience\.com|lawyers\.com|"
    r"superlawyers\.com|yelp\.com|yellowpages\.com|"
    r"thetexasattorney\.com|fortlauderdalecriminalattorneyblog\.com|"
    r"scottjbrookpa\.com|goodpeopledogetarrested\.com|"
    r"lexvisio\.com|bencmartin\.com|scottrmarshall\.com|"
    r"chieftain\.com|thelegaldescription\.com|"
    r"lawyersdirs\.com|legaltalknetwork\.com)",
    re.IGNORECASE
)

# Generic directory landing pages that should be cleared from legal_directory_listing
# (not a specific attorney profile, just the generic listing page)
GENERIC_DIRECTORY_URLS = re.compile(
    r"martindale\.com/by-location/|"
    r"avvo\.com/find-a-lawyer/|"
    r"findlaw\.com/lawyer/|"
    r"justia\.com/lawyers/\w+-lawyers/?$",
    re.IGNORECASE
)

def get_area_code(phone):
    """Extract area code from phone number."""
    digits = re.sub(r"[^\d]", "", phone or "")
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) >= 10:
        return digits[:3]
    return None

def clean_website(row):
    """Remove directory pages, news articles, and non-law websites."""
    website = row.get("website", "").strip()
    if not website:
        return row
    domain = re.sub(r"https?://(?:www\.)?", "", website.lower()).split("/")[0]
    if domain in NON_LAW_DOMAINS or DIRECTORY_WEBSITE_PATTERNS.search(website):
        row = dict(row)
        row["website"] = ""
    return row


def clean_directory_listing(row):
    """Clear generic/landing-page URLs from legal_directory_listing (keep specific profiles)."""
    listing = row.get("legal_directory_listing", "").strip()
    if not listing:
        return row
    # Clear martindale general listing pages (not attorney-specific profiles)
    if GENERIC_DIRECTORY_URLS.search(listing):
        row = dict(row)
        row["legal_directory_listing"] = ""
    return row


def fix_fields(row):
    """Fix known bad field values: tracking emails, phone fragments, misassigned websites."""
    row = dict(row)
    name_lc = _normalize_quotes(row.get("law_firm_name", "")).lower().strip()

    # Clear tracking/spam emails
    email = row.get("email", "")
    if email and re.search(r"(leadrouter|wixpress|sentry|tracking|noreply@)", email, re.I):
        row["email"] = ""

    # Fix phone fragments missing area code (e.g., "231-9873" in a KS county)
    phone = row.get("phone_number", "").strip()
    if re.match(r"^\d{3}-\d{4}$", phone):
        county = row.get("county", "").lower()
        if county in {"allen", "bourbon", "chautauqua", "cherokee", "coffey",
                      "crawford", "elk", "greenwood", "labette", "montgomery",
                      "neosho", "wilson", "woodson"}:
            row["phone_number"] = f"(620) {phone}"

    # Clear website misassigned to a different attorney's firm
    if name_lc == "charles gentry law office":
        website = row.get("website", "")
        if "zackeryreynolds" in website or "reynolds" in website.lower():
            row["website"] = ""
            row["email"] = ""  # email was also Reynolds'

    # Strip tracking parameters from website URLs
    website = row.get("website", "")
    if website and "?" in website:
        clean = re.sub(r"\?[^#]*", "", website)
        if len(clean) > 10:
            row["website"] = clean

    return row

def is_keyword_dump_address(street):
    """Return True if street address looks like scraped text rather than an address."""
    if not street or not street.strip():
        return False
    s = street.strip()
    # Street addresses have digits and short content
    if len(s) > 100:
        return True
    # Image filenames or hex hashes embedded in address
    if re.search(r"\.(jpg|jpeg|png|gif|pdf|webp|mp4)\)", s, re.I):
        return True
    if re.search(r"\b[a-f0-9]{20,}\b", s):  # long hex string (file hash)
        return True
    # Contains obvious non-address content
    if re.search(r"\b(award|million|billion|lawsuit|settlement|verdict|"
                 r"damages|plaintiff|defendant|case|appeal|result)\b", s, re.I):
        return True
    if re.search(r"^\d+[MBK]\s+", s, re.I):  # "$50M" style
        return True
    # Testimonial / review text (may start with a digit like "0")
    if re.search(r"\b(treated my|my sister|years? of practice|over \d+ years|"
                 r"highly recommend|excellent service|great attorney|"
                 r"applications? while|reforms? program|race.gender neutral|"
                 r"small business program|characteristics under)\b", s, re.I):
        return True
    # Looks like a sentence (multiple words, no digits after first 2 chars)
    if len(s) > 40 and not re.search(r"\d", s[2:]):
        words = s.split()
        if len(words) > 5:
            return True
    return False

def is_out_of_state_seo(row):
    """Return True if entry looks like an out-of-state SEO spam entry."""
    kscourts = row.get("legal_directory_listing", "").strip()
    gbp = row.get("google_business_profile", "").strip()
    name = row.get("law_firm_name", "").strip()
    phone = row.get("phone_number", "").strip()
    street = row.get("street_address", "").strip()

    # Never remove kscourts entries
    if kscourts and "kscourts.gov" in kscourts:
        return False

    # Check for out-of-state area codes
    area = get_area_code(phone)
    if area and area in NONKS_AREA_CODES:
        # Has out-of-state phone + no GBP → likely spam
        if not gbp:
            return True

    # Check for keyword dump addresses
    if is_keyword_dump_address(street):
        return True

    # Name contains out-of-state city references
    if re.search(r"\b(coral springs|boca raton|fort lauderdale|miami|sarasota|"
                 r"naples|dallas|houston|austin|san antonio|new york|manhattan|"
                 r"chicago|los angeles|seattle|portland|denver|phoenix|"
                 r"minneapolis|atlanta|boston|charlotte|las vegas|"
                 r"philadelphia|washington d\.?c\.?|baltimore)\b", name, re.I):
        return True

    # Name looks like a blog post, article, or news headline
    if re.search(r"\b(page \d+ of \d+|blog|article|post|notice|hearing|"
                 r"publication|pdf|names \w+ attorneys|names new partner|"
                 r"reviews from|developments|spotlight|community mourns|"
                 r"named .+ vp|named .+ director|named .+ officer)\b", name, re.I):
        return True

    # Name contains "/" — typically a website navigation path or URL fragment
    if "/" in name and not re.search(r"\b(law|legal|attorney|counsel)\b", name, re.I):
        return True

    # Names starting with quotes (news headlines)
    if name.startswith(("'", '"', "“", "‘")):
        return True

    # Introducing/Joining announcement headlines
    if re.search(r"^(introducing our|courtney .+ joins|.+ joins the center|"
                 r".+ named to|.+ receives award)\b", name, re.I):
        return True

    # "Little Rock, AR" or similar out-of-state suffix in name
    if re.search(r",\s*(little rock|fayetteville|rogers|bentonville),?\s*ar\b", name, re.I):
        return True

    # Name starts with a navigation label (SEO scrape artifact)
    if re.match(r"^(home|contact|offices|services|lawyers|attorneys|results|about|"
                r"practice areas?|family law|state &|litigation|meet us|our attorneys|"
                r"meet the team|join us|careers|resources)\s*[-–]", name, re.I):
        return True

    # Name with "–" separator (often directory entries)
    if " – " in name and not any(x in name.lower() for x in ["law", "attorney", "legal"]):
        return True

    # Name explicitly mentions out-of-state city/state
    if re.search(r"\b(ohio|pennsylvania|nebraska|minnesota|connecticut|rhode island|"
                 r"michigan|oregon|washington state|new jersey|illinois)\b", name, re.I):
        return True
    if re.search(r"\bcolumbus,?\s*ohio\b", name, re.I):
        return True
    # "Erie PA Lawyers" or similar city-state combos in name
    if re.search(r"\berie\s+pa\s+(lawyers|attorneys|law)\b", name, re.I):
        return True
    # Address fragments embedded in firm name (e.g., "Atty, 511 S Georgia St")
    if re.search(r",\s*(attorney|atty)[,\s].*\d+\s+[NSEW]\b", name, re.I):
        return True
    # News/award headlines
    if re.search(r"\b(awarded\s+\d{4}|backs?\s+\w+\s+law|clamp\s+down|"
                 r"seeks?\s+to\b|mourns\s+loss|named\s+(stormont|vail|health))\b", name, re.I):
        return True

    return False

def _normalize_quotes(s):
    """Normalize curly/typographic apostrophes and quotes to ASCII."""
    return s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')


def is_junk(row):
    name = _normalize_quotes(row.get("law_firm_name", "").strip())
    name_lc = name.lower()
    kscourts = row.get("legal_directory_listing", "").strip()
    gbp = row.get("google_business_profile", "").strip()
    website = row.get("website", "").strip()
    city = row.get("city", "").strip()
    phone = row.get("phone_number", "").strip()

    # Remove non-Latin / obviously foreign entries — no exceptions
    if re.search(r"[äöüÄÖÜßéèêàùâîôûç]|rechtsanwalt|rechtsanwälte|kanzlei|"
                 r"fachanwalt|notar\b|desa|bengkel|balai|warnet|bukit|rumah\b|"
                 r"desa lawiran|firman\b(?! law| legal)", name, re.I):
        return True

    # REMOVE_EXACT always wins — even for kscourts entries
    if name_lc in REMOVE_EXACT:
        return True

    # Government courts / county entities — remove even from kscourts
    if re.match(r"^\w[\w\s]+\s+county\s+(district\s+court(?:house)?|courthouse|attorney'?s?\s+office?|attorney|magistrate\s+judge|judicial\s+center)\s*$", name, re.I):
        return True
    # City/municipality employers (e.g., "City of Parsons")
    if re.match(r"^city\s+of\s+\w[\w\s]*$", name, re.I) and not LAW_INDICATORS.search(name):
        return True
    if re.search(r"\b(\d+th|\d+st|\d+nd|\d+rd|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth)\s+judicial\s+district\b", name, re.I):
        if not re.search(r"\b(public defender|defender|legal services|legal aid)\b", name, re.I):
            return True
    if re.match(r"^\w[\w\s]+county\s*$", name, re.I) and not LAW_INDICATORS.search(name):
        return True

    # Always keep kscourts-sourced entries (after name-based filters above)
    if kscourts and "kscourts.gov" in kscourts:
        return False

    # Remove entries with no city that aren't verifiably Kansas
    if not city and not kscourts:
        area = get_area_code(phone)
        ph_digits = re.sub(r"[^\d]", "", phone or "")
        is_us_format = bool(re.match(r"^1?[2-9]\d{9}$", ph_digits)) if ph_digits else False
        if not is_us_format or (area and area not in KS_AREA_CODES):
            return True  # No city + non-KS/non-US phone = international result
        if not area and not gbp:
            return True  # No city + no phone + no GBP = can't verify locality

    # Remove SEO/keyword patterns in name
    if re.search(
        r"(^results\s*[-–]|^services\s*[-–]|^contact\s*[-–]|^about\s*[-–]|"
        r"page \d+ of \d+|^\w+ county attorney$|^\w+ county courthouse$|"
        r"^pdf |^publication notice|^attorney suspended)",
        name, re.I
    ):
        return True

    # Webpage title patterns: "Practice Area, Area, Area in City/Region"
    if re.search(r"\bin\s+(southeast|se\s+ks|eastern|western|southern|northern)\b", name, re.I):
        if not re.match(r"^(law office|office of|the \w+ law)", name, re.I):
            return True

    # Remove if name is > 80 chars (likely a description, not a firm name)
    if len(name) > 80:
        return True

    # Remove obvious non-law businesses (by pattern, unless it has law indicators)
    if NON_LAW_PATTERNS.search(name) and not LAW_INDICATORS.search(name):
        return True

    # Remove government non-law entities
    if re.search(r"\b(city clerk|city hall|fire department|fire dept|"
                 r"water treatment|police department|county clerk|"
                 r"school district|county courthouse)\b",
                 name, re.I) and not LAW_INDICATORS.search(name):
        return True

    # Remove out-of-state SEO spam
    if is_out_of_state_seo(row):
        return True

    # Remove entries that are just street addresses or locations
    if re.match(r"^(us-?\d+\s*&|[a-z]{2}-?\d+\s*&|\d+\s+[nsew]\b)", name, re.I):
        return True
    if re.match(r"^[a-z][\w\s]+,\s+ks$", name, re.I) and not LAW_INDICATORS.search(name):
        return True

    # Medical professionals (not attorneys)
    if re.search(r"\b(dds|dmd|md\b|do\b|pa-c\b|np\b|aprn\b|dentist|physician|"
                 r"chiropractic|optometrist|veterinar|orthopaedic|orthopedic|"
                 r"medical center|memorial medical|health center)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Insurance companies (not law firms)
    if re.search(r"\b(american family insurance|farm bureau insurance|state farm|"
                 r"allstate insurance|farmers insurance|nationwide insurance|"
                 r"insurance service|insurance partner)\b", name, re.I):
        return True

    # Seal coating / paving / auto body
    if re.search(r"\b(seal coating|paving|auto body|paint & body)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Pest control
    if re.search(r"\b(pest control|bug busters|exterminator|orkin)\b", name, re.I):
        return True

    # Data processing / postal service
    if re.search(r"\b(data processing|postal service|ups\b|fedex|usps)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Academic / government programs (not law firms)
    if re.search(r"\b(institute for research|legal series|intake services|"
                 r"advocacy program|legal disclaimer|legal intake|"
                 r"community supervision|public policy\b|armed forces legal "
                 r"assistance|afla)\b", name, re.I):
        return True

    # Liquor / retail / service businesses
    if re.search(r"\b(liquor|package store|daycare|day care|party rentals|"
                 r"seed co|oil llc|oil company|auction|auctions|gifts|"
                 r"gift shop|grooming|counseling center|posh\b|fireworks|"
                 r"pawn shop|pawn\b|auto service|auto body|painting|"
                 r"paint store|steak house|steakhouse|tire & lube|"
                 r"cpa llc|cpa pc)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # CPA / tax prep firms (not attorneys)
    if re.search(r"\b(cpa\b|certified public accountant|h&r block|jackson hewitt|"
                 r"h & r block)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Chamber of commerce / title company / treasurer (not referral targets)
    if re.search(r"\b(chamber of commerce|title company|title co|county treasurer|"
                 r"county jail\b|bepress)\b", name, re.I):
        return True

    # Financial advisors
    if re.search(r"\b(edward jones|financial advisor|lpl financial|merrill lynch|"
                 r"raymond james|morgan stanley|wells fargo advisor|"
                 r"ameriprise|northwest mutual)\b", name, re.I):
        return True

    # Bail bonds
    if re.search(r"\bbail\s*(bond|bonds|bonding)\b", name, re.I):
        return True

    # Consulting firms (not law)
    if re.search(r"\bconsulting\s+(llc|inc|services|group|firm)\b", name, re.I):
        if not LAW_INDICATORS.search(name):
            return True

    # Sheriff / police (law enforcement, not referral targets)
    if re.search(r"\b(sheriff|police\s+dept|police\s+department|highway\s+patrol)\b", name, re.I):
        return True

    # Government courts / judicial district
    if re.search(r"\b(\d+th|\d+st|\d+nd|\d+rd)\s+judicial\s+district\b", name, re.I):
        return True
    if re.match(r"^\w+\s+county\s+(district\s+court|courthouse)\s*$", name, re.I):
        return True

    # Raw county/city name with no qualifier
    if re.match(r"^\w[\w\s]+county\s*$", name, re.I) and not LAW_INDICATORS.search(name):
        return True

    # Commodities / utilities / wire services / non-law businesses
    if re.search(r"\b(commodities|western union|money transfer|wire transfer|"
                 r"utility|electric company|telephone company|"
                 r"construction co|building products|bookkeeping|tax service|"
                 r"mental health|wellness center|parsonage|"
                 r"peddler|antique|pool side|poolside|"
                 r"insurance agency|insurance co|"
                 r"bank\b(?! (loan|fraud|robbery)))\b", name, re.I):
        if not (kscourts and "kscourts.gov" in kscourts):
            return True

    # Out-of-state city in firm name (Kansas City is MO, not a SE KS city)
    if re.search(r"\bkansas\s+city\b", name, re.I):
        # Only remove if phone has no KS area code
        area = get_area_code(row.get("phone_number", ""))
        if not area or area not in KS_AREA_CODES:
            return True

    # Require some local signal for entries from ambiguous city names
    # (Toronto, Burlington, Erie, etc. are cities in multiple states/countries)
    AMBIGUOUS_CITIES = {"toronto", "burlington", "erie", "independence", "chanute",
                        "parsons", "fredonia", "howard", "yates center"}
    if city.lower() in AMBIGUOUS_CITIES:
        area = get_area_code(phone)
        # Must have KS area code OR GBP to confirm local presence
        if not gbp and (not area or area not in KS_AREA_CODES):
            # No local signal at all → remove regardless of law keywords
            if not area and not phone:
                return True
            # Has law indicator but no local anchor → still remove if suspicious
            if area and area in NONKS_AREA_CODES:
                return True
            # Has law indicator with no phone and no GBP → remove (could be webpage title)
            if not phone and not gbp and LAW_INDICATORS.search(name):
                # Allow only if name looks like a firm (no sentence structure)
                words = name.split()
                if len(words) > 7 or name.endswith("...") or "," in name[20:]:
                    return True

    return False

def _norm_firm(name):
    name = name.lower().strip()
    # Normalize punctuated abbreviations before word-boundary removal
    name = re.sub(r"\bp\.a\.", "pa", name)
    name = re.sub(r"\bp\.c\.", "pc", name)
    name = re.sub(r"\bl\.l\.c\.", "llc", name)
    name = re.sub(r"\bl\.l\.p\.", "llp", name)
    name = re.sub(r"\bp\.l\.l\.c\.", "pllc", name)
    name = re.sub(r"[,;.']", " ", name)  # strip punctuation before word matching
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
    """Return True if both names share a significant (4+ char, non-stop) word."""
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

    by_name_key = {}  # norm_name|city → row index
    by_phone_key = {}  # phone10|city → row index
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

        # Check name-based dedup
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

        # Check phone-based dedup (same phone + city = same entity)
        if phone_key and phone_key in by_phone_key:
            idx = by_phone_key[phone_key]
            existing_name = kept[idx].get("law_firm_name", "")
            # Merge if names share a significant token (bidirectional, 4+ chars, non-stop-word)
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

    # Fix known bad field values (emails, phone fragments, misassigned websites)
    rows = [fix_fields(r) for r in rows]

    # Clean bad websites and generic directory listing URLs
    rows = [clean_website(r) for r in rows]
    rows = [clean_directory_listing(r) for r in rows]

    # Remove junk
    kept = [r for r in rows if not is_junk(r)]
    removed_junk = original - len(kept)

    # Deduplicate
    before_dedup = len(kept)
    kept = deduplicate(kept)
    removed_dups = before_dedup - len(kept)

    # Write back
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(kept)

    print(f"  {slug}: {original} → {len(kept)} "
          f"(-{removed_junk} junk, -{removed_dups} dups)")
    return len(kept)

def main():
    print("SE Kansas County Cleanup\n" + "="*50)
    counts = {}
    for slug in SE_KS_COUNTIES:
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
