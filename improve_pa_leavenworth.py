#!/usr/bin/env python3
"""Deep practice area re-scraping for 'General' entries with websites — Leavenworth County KS."""

import csv
import re
import time
import warnings
from pathlib import Path

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

CSV = Path("app/county-data/leavenworth-county-ks.csv")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PRACTICE_KEYWORDS = {
    "Personal Injury": [
        "personal injury", "car accident", "auto accident", "slip and fall",
        "premises liability", "wrongful death", "injury attorney", "injury lawyer",
        "accident attorney", "accident lawyer", "catastrophic injury", "brain injury",
        "spinal cord", "burn injury", "products liability", "dog bite",
        "motorcycle accident", "truck accident", "bicycle accident",
        "pedestrian accident", "rideshare", "construction accident", "negligence",
    ],
    "Family Law": [
        "family law", "divorce", "child custody", "child support", "spousal support",
        "alimony", "paternity", "adoption", "guardianship", "domestic relations",
        "marital", "separation agreement", "parenting plan", "visitation",
        "protective order", "restraining order", "prenuptial",
    ],
    "Criminal Defense": [
        "criminal defense", "criminal law", "dui defense", "dwi defense",
        "felony", "misdemeanor", "drug charge", "drug offense", "assault",
        "theft", "burglary", "robbery", "homicide", "murder defense",
        "sex crime", "domestic violence defense", "expungement", "criminal record",
        "probation violation", "court martial", "military defense",
    ],
    "DUI": [
        "dui", "dwi", "drunk driving", "driving under influence",
        "driving while intoxicated",
    ],
    "Estate Planning": [
        "estate planning", "wills", "trusts", "living trust", "revocable trust",
        "irrevocable trust", "probate", "estate administration", "power of attorney",
        "healthcare directive", "advance directive", "living will",
        "trust administration", "asset protection", "legacy planning",
    ],
    "Business Law": [
        "business law", "corporate law", "commercial law", "business attorney",
        "business formation", "contract", "mergers and acquisitions",
        "partnership", "llc formation", "corporate governance", "shareholder",
        "franchise law", "commercial transactions", "trademark", "copyright",
    ],
    "Employment Law": [
        "employment law", "labor law", "wrongful termination", "discrimination",
        "harassment", "hostile work environment", "wage and hour", "overtime",
        "fmla", "ada", "title vii", "retaliation", "whistleblower",
        "non-compete", "employment contract",
    ],
    "Immigration": [
        "immigration", "visa", "green card", "citizenship", "naturalization",
        "deportation", "removal defense", "asylum", "work permit", "daca",
        "h-1b", "family immigration", "immigration attorney",
    ],
    "Bankruptcy": [
        "bankruptcy", "chapter 7", "chapter 11", "chapter 13", "debt relief",
        "debt discharge", "insolvency", "foreclosure defense",
        "wage garnishment", "financial restructuring",
    ],
    "Real Estate": [
        "real estate law", "real estate attorney", "property law",
        "closing attorney", "title insurance", "easement", "zoning",
        "land use", "landlord tenant", "commercial real estate",
    ],
    "Social Security Disability": [
        "social security", "disability", "ssdi", "ssi", "disability benefits",
        "disability claim", "disability appeal",
    ],
    "Medical Malpractice": [
        "medical malpractice", "medical negligence", "surgical error",
        "misdiagnosis", "birth injury", "hospital negligence", "nursing home",
    ],
    "Workers' Compensation": [
        "workers compensation", "workers' compensation", "work injury",
        "workplace injury", "work accident", "workers comp",
    ],
    "Military Law": [
        "military law", "court martial", "ucmj", "military defense",
        "military attorney", "military lawyer", "jag", "discharge upgrade",
        "va benefits", "veterans",
    ],
    "Civil Litigation": [
        "civil litigation", "civil trial", "civil disputes", "general litigation",
        "dispute resolution", "mediation", "arbitration", "commercial litigation",
    ],
    "Tax Law": [
        "tax law", "tax attorney", "tax lawyer", "irs", "tax audit",
        "tax dispute", "tax planning",
    ],
}

PRACTICE_AREA_PAGES = [
    "/practice-areas", "/practice-area", "/services", "/areas-of-practice",
    "/our-practice", "/what-we-do", "/legal-services", "/expertise",
    "/specialties", "/areas-of-law", "/practice", "/about",
]


def detect_practice_area(text: str) -> str | None:
    text_lower = text.lower()
    scores: dict[str, int] = {}
    for pa, keywords in PRACTICE_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > 0:
            scores[pa] = score
    if not scores:
        return None
    return max(scores, key=lambda k: scores[k])


def fetch_text(url: str, timeout: int = 10) -> str:
    for verify in (True, False):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "lxml")
                for tag in soup(["script", "style", "nav", "footer"]):
                    tag.decompose()
                return soup.get_text(separator=" ", strip=True)
        except Exception:
            pass
    return ""


def scrape_firm(website: str) -> str | None:
    base = website.rstrip("/")
    text = fetch_text(base)
    pa = detect_practice_area(text)
    if pa:
        return pa
    for path in PRACTICE_AREA_PAGES:
        time.sleep(0.3)
        text = fetch_text(base + path)
        if text:
            pa = detect_practice_area(text)
            if pa:
                return pa
    return None


rows = list(csv.DictReader(CSV.open()))
fieldnames = list(rows[0].keys())

targets = [r for r in rows if r.get("practice_area", "").strip().lower() == "general"
           and r.get("website", "").strip()]

print(f"Re-scraping {len(targets)} General entries with websites...")
updated = 0

for i, row in enumerate(targets, 1):
    pa = scrape_firm(row["website"].strip())
    if pa:
        for r in rows:
            if r["law_firm_name"] == row["law_firm_name"] and r["website"] == row["website"]:
                r["practice_area"] = pa
                break
        updated += 1
    if i % 5 == 0:
        print(f"  Progress: {i}/{len(targets)}, updated {updated}")
    time.sleep(0.5)

print(f"\nUpdated {updated}/{len(targets)} practice areas")

with CSV.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)

rows_final = list(csv.DictReader(CSV.open()))
total = len(rows_final)
general = sum(1 for r in rows_final if r.get("practice_area", "").lower() == "general")
print(f"Final General PA: {general}/{total} ({general/total*100:.1f}%)")
