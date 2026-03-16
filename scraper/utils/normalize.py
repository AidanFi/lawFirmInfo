import re
from rapidfuzz import process, fuzz

CANONICAL_PRACTICE_AREAS = [
    "Personal Injury", "Family Law", "Criminal Defense", "Estate Planning",
    "Probate", "Workers' Compensation", "Bankruptcy", "Business Law",
    "Real Estate", "Immigration", "Employment Law", "Medical Malpractice",
    "Insurance Defense", "Social Security Disability", "Civil Litigation",
    "Tax Law", "Intellectual Property", "Environmental Law", "Elder Law",
    "DUI", "Divorce", "Child Custody", "Wrongful Death", "Civil Rights", "Zoning"
]

_SUFFIX_PATTERN = re.compile(
    r'\b(llc|llp|lp|pllc|pc|p\.?a\.?|law firm|law group|law office|law|'
    r'attorney at law|attorneys at law|attorney)\b',
    re.IGNORECASE
)
_ASSOC_PATTERN = re.compile(r'\b(and|&)\b', re.IGNORECASE)
_PUNCT_PATTERN = re.compile(r"[^\w\s]")
_MULTI_SPACE = re.compile(r'\s+')


def normalize_firm_name(name: str) -> str:
    """Return a normalized firm name for fuzzy dedup comparison."""
    s = name.lower()
    s = _SUFFIX_PATTERN.sub('', s)
    s = _ASSOC_PATTERN.sub('', s)
    s = _PUNCT_PATTERN.sub('', s)
    s = _MULTI_SPACE.sub(' ', s).strip()
    return s


def are_same_firm(name_a: str, name_b: str, threshold: int = 85) -> bool:
    """Return True if two firm names refer to the same firm."""
    norm_a = normalize_firm_name(name_a)
    norm_b = normalize_firm_name(name_b)
    score = fuzz.token_sort_ratio(norm_a, norm_b)
    return score >= threshold


def normalize_practice_area(raw: str) -> str:
    """Map a raw practice area string to the closest canonical area."""
    result = process.extractOne(
        raw,
        CANONICAL_PRACTICE_AREAS,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=72
    )
    if result:
        return result[0]
    return raw.strip().title()
