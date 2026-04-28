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
