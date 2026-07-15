#!/usr/bin/env python3
"""
Comprehensive NPI provider scraper for all KS counties without providers data.
Scrapes chiropractors and physical therapists from the NPPES NPI registry.
Website enrichment: domain guessing (fast) + DDG phone search (slow).

Usage:
  python3 providers_ks_all.py               # run all counties
  python3 providers_ks_all.py --county saline-county-ks
  python3 providers_ks_all.py --enrich-only # skip NPI scrape, only re-enrich

Progress is saved after each county so the script can be safely interrupted.
"""
import argparse
import csv
import json
import re
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

DATA_DIR = Path("app/county-data")
MANIFEST_PATH = DATA_DIR / "manifest.json"
NPI_URL = "https://npiregistry.cms.hhs.gov/api/"

FIELDNAMES = [
    "provider_name", "website", "phone_number", "provider_type",
    "city", "state", "county", "street_address", "zip_code",
    "email", "npi_number",
]

PROVIDER_TYPES = {
    "chiropractor": "Chiropractor",
    "physical therapist": "Physical Therapist",
}

# ─────────────────────────────────────────────────────────────────────────────
# ALL 92 KS COUNTIES WITHOUT PROVIDERS DATA
# ─────────────────────────────────────────────────────────────────────────────
ALL_KS_COUNTIES = {

    # ── Batch 2: major new KS counties ──────────────────────────────────────
    "sedgwick-county-ks": {
        "county": "Sedgwick County", "state": "KS", "msa": "Wichita",
        "cities": [
            "Wichita", "Derby", "Andover", "Haysville", "Valley Center",
            "Bel Aire", "Mulvane", "Clearwater", "Cheney", "Maize",
            "Goddard", "Park City", "Mount Hope", "Garden Plain",
            "Andale", "Viola", "Colwich", "Eastborough", "Kechi",
            "Bentley", "Sedgwick",
        ],
    },
    "riley-county-ks": {
        "county": "Riley County", "state": "KS", "msa": "Manhattan",
        "cities": [
            "Manhattan", "Riley", "Ogden", "Leonardville",
            "Randolph", "Stockdale", "Cleburne", "Zeandale",
        ],
    },
    "ellis-county-ks": {
        "county": "Ellis County", "state": "KS", "msa": "",
        "cities": [
            "Hays", "Ellis", "Victoria", "Catharine", "Munjor",
            "Schoenchen", "Walker", "Antonino", "Pfeifer",
        ],
    },
    "finney-county-ks": {
        "county": "Finney County", "state": "KS", "msa": "",
        "cities": [
            "Garden City", "Holcomb", "Deerfield", "Pierceville", "Kalvesta",
        ],
    },
    "geary-county-ks": {
        "county": "Geary County", "state": "KS", "msa": "",
        "cities": [
            "Junction City", "Milford", "Fort Riley", "Grandview Plaza",
            "Wakefield", "Ogden",
        ],
    },
    "ford-county-ks": {
        "county": "Ford County", "state": "KS", "msa": "",
        "cities": [
            "Dodge City", "Ford", "Spearville", "Bucklin",
            "Bloom", "Offerle", "Wright",
        ],
    },
    "seward-county-ks": {
        "county": "Seward County", "state": "KS", "msa": "",
        "cities": ["Liberal", "Kismet", "Arkalon", "Plains"],
    },
    "sumner-county-ks": {
        "county": "Sumner County", "state": "KS", "msa": "Wichita",
        "cities": [
            "Wellington", "Caldwell", "Argonia", "Belle Plaine",
            "Conway Springs", "South Haven", "Oxford", "Mulvane",
            "Milan", "Mayfield",
        ],
    },
    "atchison-county-ks": {
        "county": "Atchison County", "state": "KS", "msa": "",
        "cities": [
            "Atchison", "Effingham", "Muscotah", "Huron",
            "Lancaster", "Monrovia", "Potter",
        ],
    },
    "pottawatomie-county-ks": {
        "county": "Pottawatomie County", "state": "KS", "msa": "Manhattan",
        "cities": [
            "Wamego", "St. Marys", "St Marys", "Westmoreland",
            "Olsburg", "Louisville", "St. George", "St George",
            "Emmett", "Havensville", "Belvue",
        ],
    },
    "crawford-county-ks": {
        "county": "Crawford County", "state": "KS", "msa": "",
        "cities": [
            "Pittsburg", "Frontenac", "Girard", "Columbus",
            "Galena", "Cherokee", "McCune", "Scammon", "Arma",
            "Mulberry", "Walnut", "Farlington", "Hepler",
        ],
    },

    # ── Batch 3: mid-size KS counties ───────────────────────────────────────
    "butler-county-ks": {
        "county": "Butler County", "state": "KS", "msa": "Wichita",
        "cities": [
            "El Dorado", "Augusta", "Andover", "Rose Hill", "Leon",
            "Benton", "Towanda", "Potwin", "Burns", "Cassoday",
            "Latham", "Douglass", "Eureka", "Whitewater", "Elbing",
            "Severy", "Chelsea",
        ],
    },
    "lyon-county-ks": {
        "county": "Lyon County", "state": "KS", "msa": "",
        "cities": [
            "Emporia", "Allen", "Americus", "Hartford", "Reading",
            "Admire", "Olpe", "Lebo", "Neosho Rapids",
        ],
    },
    "cowley-county-ks": {
        "county": "Cowley County", "state": "KS", "msa": "",
        "cities": [
            "Winfield", "Arkansas City", "Udall", "Burden", "Dexter",
            "Cambridge", "Atlanta", "Oxford", "Maple City",
        ],
    },
    "pratt-county-ks": {
        "county": "Pratt County", "state": "KS", "msa": "",
        "cities": ["Pratt", "Iuka", "Comet", "Preston", "Sawyer", "Cullison"],
    },
    "jackson-county-ks": {
        "county": "Jackson County", "state": "KS", "msa": "",
        "cities": [
            "Holton", "Mayetta", "Whiting", "Netawaka", "Soldier",
            "Circleville", "Hoyt", "Delia",
        ],
    },
    "brown-county-ks": {
        "county": "Brown County", "state": "KS", "msa": "",
        "cities": [
            "Hiawatha", "Horton", "Sabetha", "Fairview", "Reserve",
            "Everest", "Willis",
        ],
    },
    "thomas-county-ks": {
        "county": "Thomas County", "state": "KS", "msa": "",
        "cities": ["Colby", "Brewster", "Rexford", "Menlo"],
    },
    "doniphan-county-ks": {
        "county": "Doniphan County", "state": "KS", "msa": "",
        "cities": ["Troy", "Elwood", "Highland", "White Cloud", "Wathena", "Severance"],
    },
    "nemaha-county-ks": {
        "county": "Nemaha County", "state": "KS", "msa": "",
        "cities": ["Seneca", "Sabetha", "Centralia", "Baileyville", "Wetmore"],
    },
    "stevens-county-ks": {
        "county": "Stevens County", "state": "KS", "msa": "",
        "cities": ["Hugoton", "Moscow", "Satanta"],
    },
    "norton-county-ks": {
        "county": "Norton County", "state": "KS", "msa": "",
        "cities": ["Norton", "Almena", "Lenora", "Clayton"],
    },
    "pawnee-county-ks": {
        "county": "Pawnee County", "state": "KS", "msa": "",
        "cities": ["Larned", "Burdett", "Rozel", "Garfield"],
    },
    "scott-county-ks": {
        "county": "Scott County", "state": "KS", "msa": "",
        "cities": ["Scott City", "Modoc"],
    },
    "morris-county-ks": {
        "county": "Morris County", "state": "KS", "msa": "",
        "cities": ["Council Grove", "Dunlap", "Dwight", "White City"],
    },
    "wabaunsee-county-ks": {
        "county": "Wabaunsee County", "state": "KS", "msa": "",
        "cities": ["Alma", "Eskridge", "Maple Hill", "Wabaunsee"],
    },
    "chase-county-ks": {
        "county": "Chase County", "state": "KS", "msa": "",
        "cities": ["Cottonwood Falls", "Strong City", "Matfield Green"],
    },
    "washington-county-ks": {
        "county": "Washington County", "state": "KS", "msa": "",
        "cities": ["Washington", "Haddam", "Barnes", "Clifton", "Greenleaf", "Palmer"],
    },
    "republic-county-ks": {
        "county": "Republic County", "state": "KS", "msa": "",
        "cities": ["Belleville", "Courtland", "Scandia", "Cuba", "Narka"],
    },
    "jewell-county-ks": {
        "county": "Jewell County", "state": "KS", "msa": "",
        "cities": ["Mankato", "Jewell", "Esbon", "Formoso"],
    },
    "smith-county-ks": {
        "county": "Smith County", "state": "KS", "msa": "",
        "cities": ["Smith Center", "Gaylord", "Athol"],
    },
    "decatur-county-ks": {
        "county": "Decatur County", "state": "KS", "msa": "",
        "cities": ["Oberlin", "Norcatur", "Clayton"],
    },
    "phillips-county-ks": {
        "county": "Phillips County", "state": "KS", "msa": "",
        "cities": ["Phillipsburg", "Logan", "Agra", "Prairie View"],
    },
    "neosho-county-ks": {
        "county": "Neosho County", "state": "KS", "msa": "",
        "cities": ["Chanute", "Erie", "Thayer", "Galesburg", "Parsons", "St. Paul", "St Paul"],
    },
    "wilson-county-ks": {
        "county": "Wilson County", "state": "KS", "msa": "",
        "cities": ["Fredonia", "Altoona", "Buffalo", "Benedict", "Coyville", "Fall River"],
    },

    # ── Batch 4: tiny NW/SW KS counties ─────────────────────────────────────
    "anderson-county-ks": {
        "county": "Anderson County", "state": "KS", "msa": "",
        "cities": ["Garnett", "Greeley", "Westphalia", "Colony", "Kincaid", "Harris"],
    },
    "barber-county-ks": {
        "county": "Barber County", "state": "KS", "msa": "",
        "cities": ["Medicine Lodge", "Sharon", "Kiowa", "Hazelton", "Sun City"],
    },
    "rooks-county-ks": {
        "county": "Rooks County", "state": "KS", "msa": "",
        "cities": ["Stockton", "Plainville", "Woodston", "Palco"],
    },
    "trego-county-ks": {
        "county": "Trego County", "state": "KS", "msa": "",
        "cities": ["WaKeeney", "Wakeeney", "Collyer", "Ogallah"],
    },
    "grant-county-ks": {
        "county": "Grant County", "state": "KS", "msa": "",
        "cities": ["Ulysses", "Surprise"],
    },
    "osborne-county-ks": {
        "county": "Osborne County", "state": "KS", "msa": "",
        "cities": ["Osborne", "Downs", "Portis", "Natoma"],
    },
    "sherman-county-ks": {
        "county": "Sherman County", "state": "KS", "msa": "",
        "cities": ["Goodland", "Kanorado"],
    },
    "graham-county-ks": {
        "county": "Graham County", "state": "KS", "msa": "",
        "cities": ["Hill City", "Bogue", "Morland", "Edmond"],
    },
    "greeley-county-ks": {
        "county": "Greeley County", "state": "KS", "msa": "",
        "cities": ["Tribune", "Horace"],
    },
    "hamilton-county-ks": {
        "county": "Hamilton County", "state": "KS", "msa": "",
        "cities": ["Syracuse", "Coolidge", "Kendall"],
    },
    "clark-county-ks": {
        "county": "Clark County", "state": "KS", "msa": "",
        "cities": ["Ashland", "Minneola"],
    },
    "lane-county-ks": {
        "county": "Lane County", "state": "KS", "msa": "",
        "cities": ["Dighton", "Healy"],
    },
    "morton-county-ks": {
        "county": "Morton County", "state": "KS", "msa": "",
        "cities": ["Elkhart", "Richfield", "Rolla"],
    },
    "gray-county-ks": {
        "county": "Gray County", "state": "KS", "msa": "",
        "cities": ["Cimarron", "Ingalls", "Copeland"],
    },
    "meade-county-ks": {
        "county": "Meade County", "state": "KS", "msa": "",
        "cities": ["Meade", "Fowler", "Plains"],
    },
    "rawlins-county-ks": {
        "county": "Rawlins County", "state": "KS", "msa": "",
        "cities": ["Atwood", "McDonald"],
    },
    "haskell-county-ks": {
        "county": "Haskell County", "state": "KS", "msa": "",
        "cities": ["Sublette", "Satanta", "Santa Fe"],
    },
    "logan-county-ks": {
        "county": "Logan County", "state": "KS", "msa": "",
        "cities": ["Oakley", "Winona", "Russell Springs"],
    },
    "sheridan-county-ks": {
        "county": "Sheridan County", "state": "KS", "msa": "",
        "cities": ["Hoxie", "Lucerne"],
    },
    "wallace-county-ks": {
        "county": "Wallace County", "state": "KS", "msa": "",
        "cities": ["Sharon Springs", "Weskan"],
    },
    "cheyenne-county-ks": {
        "county": "Cheyenne County", "state": "KS", "msa": "",
        "cities": ["St. Francis", "Saint Francis", "St Francis", "Wheeler"],
    },
    "comanche-county-ks": {
        "county": "Comanche County", "state": "KS", "msa": "",
        "cities": ["Coldwater", "Protection"],
    },
    "hodgeman-county-ks": {
        "county": "Hodgeman County", "state": "KS", "msa": "",
        "cities": ["Jetmore", "Hanston"],
    },
    "edwards-county-ks": {
        "county": "Edwards County", "state": "KS", "msa": "",
        "cities": ["Kinsley", "Offerle", "Lewis"],
    },
    "kiowa-county-ks": {
        "county": "Kiowa County", "state": "KS", "msa": "",
        "cities": ["Greensburg", "Haviland", "Belvidere", "Mullinville"],
    },
    "rush-county-ks": {
        "county": "Rush County", "state": "KS", "msa": "",
        "cities": ["La Crosse", "LaCrosse", "Bison", "Alexander"],
    },
    "stanton-county-ks": {
        "county": "Stanton County", "state": "KS", "msa": "",
        "cities": ["Johnson", "Johnson City"],
    },
    "kearny-county-ks": {
        "county": "Kearny County", "state": "KS", "msa": "",
        "cities": ["Lakin", "Deerfield", "Hartland"],
    },
    "ness-county-ks": {
        "county": "Ness County", "state": "KS", "msa": "",
        "cities": ["Ness City", "Utica", "Bazine", "Ransom"],
    },
    "gove-county-ks": {
        "county": "Gove County", "state": "KS", "msa": "",
        "cities": ["Quinter", "Grainfield", "Grinnell", "Monument"],
    },

    # ── Central KS (17 counties from scraper config) ─────────────────────────
    "barton-county-ks": {
        "county": "Barton County", "state": "KS", "msa": "",
        "cities": [
            "Great Bend", "Ellinwood", "Hoisington", "Claflin", "Albert",
            "Olmitz", "Pawnee Rock", "Galatia",
        ],
    },
    "clay-county-ks": {
        "county": "Clay County", "state": "KS", "msa": "",
        "cities": [
            "Clay Center", "Wakefield", "Green", "Clifton", "Morganville",
            "Leonardville", "Idana",
        ],
    },
    "cloud-county-ks": {
        "county": "Cloud County", "state": "KS", "msa": "",
        "cities": [
            "Concordia", "Miltonvale", "Glasco", "Clyde", "Jamestown",
            "Aurora", "Tipton",
        ],
    },
    "dickinson-county-ks": {
        "county": "Dickinson County", "state": "KS", "msa": "",
        "cities": [
            "Abilene", "Chapman", "Solomon", "Herington", "Detroit",
            "Hope", "Enterprise", "Woodbine", "Navarre",
        ],
    },
    "ellsworth-county-ks": {
        "county": "Ellsworth County", "state": "KS", "msa": "",
        "cities": [
            "Ellsworth", "Kanopolis", "Wilson", "Lorraine", "Holyrood",
        ],
    },
    "harvey-county-ks": {
        "county": "Harvey County", "state": "KS", "msa": "Wichita",
        "cities": [
            "Newton", "Halstead", "Hesston", "Burrton", "Sedgwick",
            "Walton", "North Newton",
        ],
    },
    "kingman-county-ks": {
        "county": "Kingman County", "state": "KS", "msa": "",
        "cities": [
            "Kingman", "Norwich", "Nashville", "Cunningham", "Zenda",
            "Penalosa", "Murdock",
        ],
    },
    "lincoln-county-ks": {
        "county": "Lincoln County", "state": "KS", "msa": "",
        "cities": [
            "Lincoln", "Sylvan Grove", "Barnard", "Beverly", "Vesper", "Luray",
        ],
    },
    "marion-county-ks": {
        "county": "Marion County", "state": "KS", "msa": "",
        "cities": [
            "Marion", "Hillsboro", "Peabody", "Florence", "Burns",
            "Durham", "Goessel", "Lost Springs", "Ramona",
        ],
    },
    "mcpherson-county-ks": {
        "county": "McPherson County", "state": "KS", "msa": "",
        "cities": [
            "McPherson", "Mc Pherson", "Lindsborg", "Marquette", "Inman",
            "Canton", "Moundridge", "Galva", "Buhler",
        ],
    },
    "mitchell-county-ks": {
        "county": "Mitchell County", "state": "KS", "msa": "",
        "cities": [
            "Beloit", "Cawker City", "Glen Elder", "Tipton", "Hunter", "Simpson",
        ],
    },
    "ottawa-county-ks": {
        "county": "Ottawa County", "state": "KS", "msa": "",
        "cities": [
            "Minneapolis", "Delphos", "Tescott", "Bennington", "Culver",
        ],
    },
    "reno-county-ks": {
        "county": "Reno County", "state": "KS", "msa": "Wichita",
        "cities": [
            "Hutchinson", "South Hutchinson", "Nickerson", "Pretty Prairie",
            "Haven", "Partridge", "Turon", "Yoder", "Arlington", "Sylvia",
        ],
    },
    "rice-county-ks": {
        "county": "Rice County", "state": "KS", "msa": "",
        "cities": [
            "Lyons", "Sterling", "Little River", "Chase", "Alden", "Bushton",
        ],
    },
    "russell-county-ks": {
        "county": "Russell County", "state": "KS", "msa": "",
        "cities": [
            "Russell", "Lucas", "Dorrance", "Gorham", "Bunker Hill", "Paradise",
        ],
    },
    "saline-county-ks": {
        "county": "Saline County", "state": "KS", "msa": "Salina",
        "cities": [
            "Salina", "Assaria", "Brookville", "Gypsum", "Mentor",
            "New Cambria", "Smolan",
        ],
    },
    "stafford-county-ks": {
        "county": "Stafford County", "state": "KS", "msa": "",
        "cities": [
            "Saint John", "St. John", "Stafford", "Macksville", "Seward",
            "Hudson", "Zenith",
        ],
    },

    # ── Additional SE KS counties (in manifest but not in batch scripts) ─────
    "allen-county-ks": {
        "county": "Allen County", "state": "KS", "msa": "",
        "cities": ["Iola", "Humboldt", "Gas", "Moran", "LaHarpe", "Savonburg", "Elsmore"],
    },
    "bourbon-county-ks": {
        "county": "Bourbon County", "state": "KS", "msa": "",
        "cities": ["Fort Scott", "Uniontown", "Bronson", "Fulton", "Mapleton", "Garland"],
    },
    "chautauqua-county-ks": {
        "county": "Chautauqua County", "state": "KS", "msa": "",
        "cities": ["Sedan", "Cedar Vale", "Hewins", "Elgin"],
    },
    "cherokee-county-ks": {
        "county": "Cherokee County", "state": "KS", "msa": "",
        "cities": ["Columbus", "Galena", "Baxter Springs", "Weir", "Riverton", "Treece"],
    },
    "coffey-county-ks": {
        "county": "Coffey County", "state": "KS", "msa": "",
        "cities": ["Burlington", "New Strawn", "Lebo", "Waverly", "Gridley"],
    },
    "elk-county-ks": {
        "county": "Elk County", "state": "KS", "msa": "",
        "cities": ["Howard", "Elk Falls", "Grenola", "Longton"],
    },
    "greenwood-county-ks": {
        "county": "Greenwood County", "state": "KS", "msa": "",
        "cities": ["Eureka", "Fall River", "Madison", "Hamilton", "Climax", "Virgil"],
    },
    "labette-county-ks": {
        "county": "Labette County", "state": "KS", "msa": "",
        "cities": ["Parsons", "Oswego", "Chetopa", "Altamont", "Mound Valley", "Dennis"],
    },
    "montgomery-county-ks": {
        "county": "Montgomery County", "state": "KS", "msa": "",
        "cities": ["Independence", "Coffeyville", "Cherryvale", "Elk City", "Caney", "Havana"],
    },
    "woodson-county-ks": {
        "county": "Woodson County", "state": "KS", "msa": "",
        "cities": ["Yates Center", "Toronto", "Neodesha"],
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# WEBSITE FILTERING
# ─────────────────────────────────────────────────────────────────────────────
_BAD_DOMAINS = frozenset({
    "healthgrades.com", "zocdoc.com", "vitals.com", "ratemds.com", "webmd.com",
    "doximity.com", "yelp.com", "yellowpages.com", "superpages.com", "whitepages.com",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com", "google.com",
    "bing.com", "wikipedia.org", "youtube.com", "bbb.org", "manta.com", "mapquest.com",
    "npiprofile.com", "npino.com", "npinumber.org", "npidb.org", "medicare.gov",
    "cms.gov", "npiregistry.cms.hhs.gov", "usnews.com", "castleconnolly.com",
    "sharecare.com", "duckduckgo.com", "chirodirectory.com", "apta.org", "acatoday.org",
    "findachiropractor.com", "chiromatrix.com", "chiropractic.org", "doctor.com",
    "wellness.com", "healthline.com", "psychologytoday.com", "psychology-today.com",
    "birdeye.com", "practicefusion.com", "merchantcircle.com", "ezlocal.com",
    "showmelocal.com", "citysearch.com", "insiderpages.com", "chamberofcommerce.com",
    "angieslist.com", "homeadvisor.com", "thumbtack.com", "care.com",
    "therapyfinder.com", "therapist.com", "goodtherapy.org", "indeed.com",
    "glassdoor.com", "trustpilot.com", "foursquare.com",
})

_DDG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _norm_domain(url: str) -> str:
    if not url:
        return ""
    try:
        return re.sub(r"^www\.", "", urlparse(url.strip()).netloc.lower())
    except Exception:
        return ""


def _is_bad(url: str) -> bool:
    if not url or not url.startswith("http"):
        return True
    d = _norm_domain(url)
    return any(d == b or d.endswith("." + b) for b in _BAD_DOMAINS)


# ─────────────────────────────────────────────────────────────────────────────
# PHONE / NAME UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _clean_phone(raw: str) -> str:
    if not raw:
        return ""
    digits = re.sub(r"[^\d]", "", raw)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] == "1":
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return raw


def _clean_zip(raw: str) -> str:
    digits = re.sub(r"[^\d]", "", raw or "")
    return digits[:5] if len(digits) >= 5 else digits


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _is_org(name: str) -> bool:
    if re.search(r"\b(llc|pllc|inc|pa|ltd|corp)\b", name, re.I):
        return True
    org_kw = (r"\b(chiropractic|physical\s+therapy|therapy|center|clinic|group|"
              r"associates|health|wellness|rehab|sports|spine|family|care|institute|"
              r"services|practice|network|back|joint)\b")
    return bool(re.search(org_kw, name, re.I))


# ─────────────────────────────────────────────────────────────────────────────
# NPI API
# ─────────────────────────────────────────────────────────────────────────────

def _extract_npi_record(result: dict) -> dict:
    basic = result.get("basic", {})
    addresses = result.get("addresses", [])
    enum_type = result.get("enumeration_type", "")

    if enum_type == "NPI-2":
        name = basic.get("organization_name", "").strip().title()
    else:
        first = basic.get("first_name", "").strip()
        middle = basic.get("middle_name", "").strip()
        last = basic.get("last_name", "").strip()
        cred = basic.get("credential", "").strip()
        name_parts = [p for p in [first, middle, last] if p]
        name = " ".join(name_parts).title()
        if cred and cred not in ("--", ""):
            name = f"{name}, {cred}"

    addr = next(
        (a for a in addresses if a.get("address_purpose") == "LOCATION"),
        addresses[0] if addresses else {},
    )

    city = addr.get("city", "").strip().title()
    state = addr.get("state", "").strip().upper()
    street = addr.get("address_1", "").strip().title()
    if addr.get("address_2"):
        street = f"{street} {addr['address_2'].strip().title()}".strip()
    zip_code = _clean_zip(addr.get("postal_code", ""))
    phone = _clean_phone(addr.get("telephone_number", ""))
    npi = result.get("number", "")

    return {
        "provider_name": name,
        "website": "",
        "phone_number": phone,
        "provider_type": "",
        "city": city,
        "state": state,
        "county": "",
        "street_address": street,
        "zip_code": zip_code,
        "email": "",
        "npi_number": npi,
    }


def fetch_npi(taxonomy_query: str, state: str, city: str) -> list[dict]:
    results = []
    skip = 0
    limit = 200
    while True:
        params = {
            "version": "2.1",
            "taxonomy_description": taxonomy_query,
            "state": state,
            "city": city,
            "limit": limit,
            "skip": skip,
        }
        try:
            r = requests.get(NPI_URL, params=params, timeout=20)
            data = r.json()
        except Exception as e:
            print(f"      NPI error ({city}): {e}")
            break
        if data.get("Errors"):
            break
        batch = data.get("results", [])
        if not batch:
            break
        results.extend(batch)
        if len(batch) < limit:
            break
        skip += limit
        time.sleep(0.3)
    return results


def scrape_county_npi(slug: str, cfg: dict) -> list[dict]:
    county_name = cfg["county"]
    state = cfg["state"]
    cities = cfg["cities"]

    seen_npis: set[str] = set()
    seen_names: set[str] = set()
    records: list[dict] = []

    for taxonomy_query, display_type in PROVIDER_TYPES.items():
        print(f"  [{display_type}]", flush=True)
        for city in cities:
            raw = fetch_npi(taxonomy_query, state, city)
            added = 0
            for res in raw:
                rec = _extract_npi_record(res)
                npi = rec["npi_number"]
                name = rec["provider_name"]
                if npi and npi in seen_npis:
                    continue
                nkey = _norm(name)
                if nkey in seen_names:
                    continue
                if rec["state"] not in (state, ""):
                    continue
                rec["provider_type"] = display_type
                rec["county"] = county_name
                if npi:
                    seen_npis.add(npi)
                seen_names.add(nkey)
                records.append(rec)
                added += 1
            if added:
                print(f"    {city}: +{added}", flush=True)
            time.sleep(0.2)

    return records


# ─────────────────────────────────────────────────────────────────────────────
# WEBSITE ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────

def _head_resolves(url: str) -> bool:
    try:
        r = requests.head(url, timeout=6, allow_redirects=True, verify=False,
                          headers={"User-Agent": _DDG_HEADERS["User-Agent"]})
        return r.status_code < 400
    except Exception:
        return False


def _name_to_domain_candidates(name: str, city: str) -> list[str]:
    cleaned = re.sub(r"\b(llc|pllc|inc|pa|ltd|corp|d\.b\.a\.)\b", "", name, flags=re.I).strip()
    cleaned = re.sub(r",\s*(D\.?C\.?|DPT|PT|LPT|MSPT|MPT|DC)\s*$", "", cleaned, flags=re.I).strip()
    base = re.sub(r"[^a-z0-9\s]", "", cleaned.lower())
    base = re.sub(r"\s+", "", base).strip()
    if not base or len(base) < 4:
        return []
    city_slug = re.sub(r"[^a-z0-9]", "", city.lower())
    candidates = []
    for ext in (".com", ".net", ".org"):
        candidates.append(f"https://{base}{ext}")
        candidates.append(f"https://www.{base}{ext}")
    if len(base) < 20:
        candidates.append(f"https://{base}{city_slug}.com")
        candidates.append(f"https://{city_slug}{base}.com")
    return candidates


def pass_domain_guess(records: list[dict]) -> int:
    targets = [r for r in records if not r["website"] and _is_org(r["provider_name"])]
    print(f"  Domain guessing: {len(targets)} org records...", flush=True)
    found = 0
    for rec in targets:
        for url in _name_to_domain_candidates(rec["provider_name"], rec["city"]):
            if _head_resolves(url):
                rec["website"] = url
                found += 1
                break
        time.sleep(0.1)
    print(f"    Found: {found}", flush=True)
    return found


_STARTPAGE_SESSION = requests.Session()
_STARTPAGE_SESSION.headers.update(_DDG_HEADERS)

_SKIP_WORDS = frozenset({
    "chiropractic", "chiropractor", "therapy", "therapist", "therapies",
    "physical", "center", "clinic", "group", "health", "wellness", "rehab",
    "rehabilitation", "sports", "spine", "spinal", "family", "care", "institute",
    "services", "practice", "network", "acupuncture", "massage", "fitness",
    "orthopedic", "medical", "injury", "back", "pain", "movement", "motion",
    "performance", "manual", "balance", "core", "active", "integrated", "advanced",
    "premier", "elite", "professional", "optimal", "comprehensive", "county",
    "associates",
})


def _clean_name_for_search(name: str) -> str:
    out = re.sub(r",?\s*\b(llc|pllc|inc|pa|ltd|corp|dba)\b", "", name, flags=re.I)
    out = re.sub(r",?\s*\b(d\.?c\.?|dpt|pt|lpt|mspt|mpt|dc|md)\b", "", out, flags=re.I)
    return re.sub(r"\s+", " ", out).strip().strip(",").strip()


def _domain_relevant(url: str, name: str, city: str) -> bool:
    if re.search(r"\.(pdf|zip|doc|jpg|png)$", url, re.I):
        return False
    domain = re.sub(r"^www\.", "", urlparse(url).netloc.lower()).replace("-", "").replace(".", "")
    cleaned = _clean_name_for_search(name).lower()
    name_words = [w for w in re.split(r"\s+", cleaned) if len(w) >= 5 and w not in _SKIP_WORDS]
    if any(w in domain for w in name_words):
        return True
    city_slug = re.sub(r"[^a-z0-9]", "", city.lower())
    if len(city_slug) >= 4 and city_slug in domain:
        if re.match(r"^(chiropractor|physicaltherapist|physicaltherapy|physio|backpain)", domain):
            return False
        return True
    return False


def _startpage_search(name: str, city: str, state: str, ptype: str) -> str:
    cleaned = _clean_name_for_search(name)
    if not cleaned:
        return ""
    query = f"{cleaned} {ptype} {city} {state}"
    url = f"https://www.startpage.com/sp/search?q={quote_plus(query)}&language=english"
    try:
        r = _STARTPAGE_SESSION.get(url, timeout=15)
        if r.status_code != 200:
            return ""
        # Use regex — Startpage encodes result hrefs in a way BeautifulSoup misses
        seen: set[str] = set()
        for href in re.findall(r'href=["\']?(https?://[^"\'> &]+)', r.text):
            if "startpage.com" in href:
                continue
            href = href.replace("&amp;", "&")
            if not _is_bad(href) and href not in seen:
                seen.add(href)
                if _domain_relevant(href, name, city):
                    return href
        return ""
    except Exception:
        return ""


def pass_startpage(records: list[dict]) -> int:
    targets = [r for r in records if not r.get("website", "").strip() and _is_org(r["provider_name"])]
    print(f"  Startpage search: {len(targets)} org records...", flush=True)
    found = 0
    for i, rec in enumerate(targets):
        site = _startpage_search(
            rec["provider_name"], rec["city"], rec["state"], rec["provider_type"].lower()
        )
        if site:
            rec["website"] = site
            found += 1
        time.sleep(3.5)
        if (i + 1) % 25 == 0:
            print(f"    {i+1}/{len(targets)} — {found} found", flush=True)
    print(f"    Done: {found}", flush=True)
    return found


def pass_share_address_phone(records: list[dict]) -> int:
    addr_to_web: dict[tuple, str] = {}
    phone_to_web: dict[str, str] = {}
    for r in records:
        web = r.get("website", "").strip()
        if not web:
            continue
        addr = re.sub(r"\s+", " ", (r.get("street_address", "") or "").lower().strip())
        zip_ = (r.get("zip_code", "") or "")[:5]
        if addr and zip_:
            addr_to_web[(addr, zip_)] = web
        ph = re.sub(r"[^\d]", "", r.get("phone_number", "") or "")
        if len(ph) == 10:
            phone_to_web[ph] = web

    found = 0
    for r in records:
        if r.get("website", "").strip():
            continue
        addr = re.sub(r"\s+", " ", (r.get("street_address", "") or "").lower().strip())
        zip_ = (r.get("zip_code", "") or "")[:5]
        web = addr_to_web.get((addr, zip_), "")
        if not web:
            ph = re.sub(r"[^\d]", "", r.get("phone_number", "") or "")
            web = phone_to_web.get(ph, "") if len(ph) == 10 else ""
        if web:
            r["website"] = web
            found += 1
    print(f"  Address/phone sharing: +{found}", flush=True)
    return found


# ─────────────────────────────────────────────────────────────────────────────
# FILE I/O
# ─────────────────────────────────────────────────────────────────────────────

def _write(path: Path, records: list[dict]):
    records_sorted = sorted(records, key=lambda r: (r["provider_type"], r["city"], r["provider_name"]))
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(records_sorted)


def _read(path: Path) -> list[dict]:
    return list(csv.DictReader(path.open()))


def _update_manifest(slug: str, cfg: dict, count: int):
    manifest = json.loads(MANIFEST_PATH.read_text())
    providers_slug = f"providers-{slug}"
    existing = {e["slug"]: e for e in manifest["counties"]}
    if providers_slug not in existing:
        manifest["counties"].append({
            "slug": providers_slug,
            "name": cfg["county"],
            "state": "KS",
            "firm_count": count,
            "last_updated": "2026-07-15",
            "msa": cfg.get("msa", ""),
        })
    else:
        existing[providers_slug]["firm_count"] = count
        existing[providers_slug]["last_updated"] = "2026-07-15"
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def process_county(slug: str, cfg: dict, enrich_only: bool = False):
    path = DATA_DIR / f"providers-{slug}.csv"
    sep = "=" * 55
    print(f"\n{sep}", flush=True)
    print(f"{cfg['county']} ({slug})", flush=True)
    print(sep, flush=True)

    if enrich_only and path.exists():
        records = _read(path)
        print(f"  Enrich only: {len(records)} existing records", flush=True)
    elif path.exists() and not enrich_only:
        print(f"  Already exists — re-enriching only", flush=True)
        records = _read(path)
    else:
        print(f"  NPI scrape...", flush=True)
        records = scrape_county_npi(slug, cfg)
        print(f"  Raw: {len(records)} providers", flush=True)
        _write(path, records)

    before = sum(1 for r in records if r.get("website", "").strip())

    # Enrichment passes — share after each so individuals inherit clinic websites
    pass_domain_guess(records)
    pass_share_address_phone(records)
    _write(path, records)

    pass_startpage(records)          # orgs only; 3.5s delay, relevance-filtered
    pass_share_address_phone(records)
    _write(path, records)

    after = sum(1 for r in records if r.get("website", "").strip())
    has_phone = sum(1 for r in records if r.get("phone_number", "").strip())
    chiro = sum(1 for r in records if r.get("provider_type") == "Chiropractor")
    pt = sum(1 for r in records if r.get("provider_type") == "Physical Therapist")

    print(f"\n  FINAL: {len(records)} providers | Chiro: {chiro} | PT: {pt}", flush=True)
    print(f"  Phone: {has_phone}/{len(records)} | Website: {before}→{after}", flush=True)

    _update_manifest(slug, cfg, len(records))
    return len(records)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", help="Only process this county slug")
    parser.add_argument("--enrich-only", action="store_true",
                        help="Skip NPI scrape, only run website enrichment on existing CSVs")
    parser.add_argument("--skip-done", action="store_true",
                        help="Skip counties that already have providers CSVs")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.county:
        if args.county not in ALL_KS_COUNTIES:
            print(f"Unknown county: {args.county}")
            print("Available:", list(ALL_KS_COUNTIES.keys()))
            sys.exit(1)
        counties = {args.county: ALL_KS_COUNTIES[args.county]}
    else:
        counties = ALL_KS_COUNTIES

    grand_total = 0
    done = 0
    for slug, cfg in counties.items():
        path = DATA_DIR / f"providers-{slug}.csv"
        if args.skip_done and path.exists() and not args.enrich_only:
            existing = _read(path)
            print(f"  SKIP {slug} ({len(existing)} records already)", flush=True)
            grand_total += len(existing)
            done += 1
            continue
        try:
            n = process_county(slug, cfg, enrich_only=args.enrich_only)
            grand_total += n
            done += 1
        except KeyboardInterrupt:
            print("\n\nInterrupted. Progress saved.", flush=True)
            break
        except Exception as e:
            print(f"  ERROR {slug}: {e}", flush=True)
            continue

    print(f"\n{'='*55}", flush=True)
    print(f"Done: {done} counties, {grand_total} total providers", flush=True)


if __name__ == "__main__":
    main()
