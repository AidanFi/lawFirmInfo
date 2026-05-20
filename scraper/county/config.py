COUNTY_DEFINITIONS = {
    "johnson": {
        "name": "Johnson County",
        "state": "KS",
        "slug": "johnson-county-ks",
        "msa": "Kansas City",
        "cities": [
            "Overland Park", "Olathe", "Shawnee", "Lenexa", "Leawood",
            "Prairie Village", "Merriam", "Mission", "Gardner", "Spring Hill",
            "De Soto", "Edgerton", "Roeland Park", "Fairway", "Westwood",
            "Lake Quivira", "Mission Hills", "Mission Woods", "Westwood Hills",
        ],
    },
    "wyandotte": {
        "name": "Wyandotte County",
        "state": "KS",
        "slug": "wyandotte-county-ks",
        "msa": "Kansas City",
        "cities": [
            "Kansas City", "Bonner Springs", "Edwardsville",
            "Lake Quivira",
        ],
        "zip_codes": [
            "66101", "66102", "66103", "66104", "66105", "66106",
            "66109", "66111", "66112", "66115", "66117", "66118",
            "66012", "66113",
        ],
        "extra_search_terms": [
            "Kansas City Kansas",
            "KCK",
        ],
    },
    "st_louis_city": {
        "name": "St. Louis City",
        "state": "MO",
        "slug": "st-louis-city-mo",
        "msa": "St. Louis",
        "cities": ["St. Louis"],
        "zip_codes": [
            "63101", "63102", "63103", "63104", "63106", "63107",
            "63108", "63109", "63110", "63111", "63112", "63113",
            "63115", "63116", "63118", "63120", "63139", "63147",
        ],
        "extra_search_terms": [
            "Saint Louis Missouri",
            "St Louis MO",
            "downtown St. Louis MO",
        ],
    },
    "st_louis_county": {
        "name": "St. Louis County",
        "state": "MO",
        "slug": "st-louis-county-mo",
        "msa": "St. Louis",
        "cities": [
            "Ballwin", "Berkeley", "Black Jack", "Breckenridge Hills",
            "Brentwood", "Bridgeton", "Calverton Park", "Charlack",
            "Chesterfield", "Clayton", "Clarkson Valley", "Cool Valley",
            "Country Club Hills", "Country Life Acres", "Creve Coeur",
            "Crystal Lake Park", "Dellwood", "Des Peres", "Edmundson",
            "Ellisville", "Fenton", "Ferguson", "Flordell Hills",
            "Florissant", "Frontenac", "Glendale", "Grantwood Village",
            "Green Park", "Greendale", "Hanley Hills", "Hazelwood",
            "Hillsdale", "Huntleigh", "Jennings", "Kirkwood", "Ladue",
            "Lakeshire", "Mackenzie", "Manchester", "Maplewood",
            "Marlborough", "Maryland Heights", "Moline Acres", "Normandy",
            "Norwood Court", "Oakland", "Olivette", "Overland", "Pagedale",
            "Pasadena Hills", "Pasadena Park", "Pine Lawn",
            "Richmond Heights", "Riverview", "Rock Hill", "Shrewsbury",
            "Sunset Hills", "Sycamore Hills", "Town and Country",
            "Twin Oaks", "University City", "Valley Park", "Velda City",
            "Velda Village Hills", "Vinita Park", "Vinita Terrace",
            "Warson Woods", "Webster Groves", "Wellston", "Westwood",
            "Wilbur Park", "Wildwood", "Winchester", "Woodson Terrace",
            "Affton", "Lemay", "Mehlville", "Oakville", "Spanish Lake",
        ],
        "zip_codes": [
            "63005", "63011", "63017", "63021", "63022", "63024",
            "63025", "63026", "63031", "63033", "63034", "63038",
            "63040", "63041", "63042", "63043", "63044", "63045",
            "63049", "63069", "63074", "63088", "63105", "63114",
            "63117", "63119", "63121", "63122", "63123", "63124",
            "63125", "63126", "63127", "63128", "63129", "63130",
            "63131", "63132", "63133", "63134", "63135", "63136",
            "63137", "63138", "63141", "63143", "63144", "63146",
        ],
        "extra_search_terms": [
            "Clayton MO attorney",
            "Kirkwood MO attorney",
            "Chesterfield MO attorney",
            "Florissant MO attorney",
        ],
    },
}

PRIORITY_SCORES = {
    "Criminal Defense": 5,
    "DUI": 5,
    "Personal Injury": 5,
    "Medical Malpractice": 5,
    "Workers' Compensation": 5,
    "Workers Compensation": 5,
    "Sexual Assault": 4,
    "Family Law": 4,
    "General Practice": 4,
    "Employment Law": 3,
    "Nursing Home": 3,
    "Civil Litigation": 3,
    "Insurance Defense": 3,
    "Divorce": 3,
    "Estate Planning": 2,
    "Probate": 2,
    "Bankruptcy": 2,
    "Real Estate": 2,
    "Real Estate Law": 2,
    "Business Law": 2,
    "Immigration": 2,
}

DEFAULT_PRIORITY = 1
GENERAL_PRIORITY = 4

SEARCH_QUERIES = ["law firm", "attorney", "lawyer"]

FOURSQUARE_LEGAL_CATEGORIES = "52f2ab2ebcbc57f1066b8b3f,63be6904847c3692a84b9b6b"


def get_priority(practice_area: str) -> int:
    if not practice_area or practice_area == "General":
        return GENERAL_PRIORITY
    return PRIORITY_SCORES.get(practice_area, DEFAULT_PRIORITY)


def get_county_config(county_key: str) -> dict:
    config = COUNTY_DEFINITIONS.get(county_key)
    if not config:
        available = ", ".join(COUNTY_DEFINITIONS.keys())
        raise ValueError(f"Unknown county '{county_key}'. Available: {available}")
    return config
