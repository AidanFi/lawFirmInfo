#!/usr/bin/env python3
"""Remove non-law-firm entries from all county CSV files."""
import csv
import glob
import os
import re
import shutil
from datetime import datetime

COUNTY_DATA_DIR = '/Users/aidanfields/lawFirmInfo/app/county-data'
BACKUP_DIR = f'/Users/aidanfields/lawFirmInfo/data/backups/pre_law_only_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

# If name matches these → definitely a law firm/attorney, always keep
LAW_INDICATORS = re.compile(
    r'\b(law\s+(firm|office|group|center|clinic|pllc|llc|pc|pa|lpa|offices)|'
    r'attorney|attorneys|atty|lawyer|lawyers|'
    r'legal\s+(group|center|clinic|services|aid|counsel|defense|advocates|support|representation|team|clinic)|'
    r'litigation|counsel(?:or|ing)?|esquire|esq\.|llp|pllc|'
    r'legal\s+support|legal\s+aid|public\s+defender|district\s+attorney|prosecutor)\b',
    re.IGNORECASE
)

# If name matches these (and no law indicator) → remove
NON_LAW_PATTERNS = re.compile(
    r'\b('
    # Banks
    r'national\s+bank|savings\s+bank|federal\s+savings|community\s+bank|'
    r'federal\s+credit\s+union|credit\s+union|'
    r'bank\s+of\s+america|pnc\s+bank|umb\s+bank|busey\s+bank|'
    r'enterprise\s+bank|first\s+federal\s+bank|central\s+national\s+bank|'
    r'commerce\s+bank|stearns\s+bank|midland\s+loan\s+services|'
    r'capitol\s+federal|first\s+business\s+bank|outdoor\s+bank|union\s+state\s+bank|'
    # Medical/Health
    r'hospital|medical\s+center|health\s+system|healthcare|'
    r'pharmacy|pharmacies|'
    r'veterinary|vet\s+clinic|animal\s+hospital|'
    r'cancer\s+center|home\s+health|mental\s+health\s+center|behavioral\s+health|'
    r'health\s+foundation|health\s+systems\s+inc|'
    # Insurance
    r'insurance\s+company|insurance\s+group|insurance\s+services|'
    r'insurance\s+solutions|insurance\s+agency|insurance,\s+inc|'
    # Schools
    r'public\s+schools|school\s+district|elementary\s+school|state\s+university|'
    # Real estate
    r'realty\b|realtor\b|real\s+estate\s+(company|group|services|investments|advisors)|'
    # Construction
    r'construction\s+(company|group|inc|llc|corp|services)|sports\s+construction|'
    # Food/Retail
    r'restaurant\s+group|food\s+bank|retail\s+liquor|veterinary\s+products|'
    # Government non-legal
    r'sheriff.s\s+office|police\s+department|social\s+security\s+administration|city\s+hall|'
    # Courts (not law firms)
    r'bankruptcy\s+court|judicial\s+district\s+court|district\s+court\s+judge|'
    r'district\s+court\s+of|district\s+court\s+trustee|district\s+court\s+of\s+ks|'
    # Financial services
    r'financial\s+group|financial\s+partners|financial\s+services|'
    r'financial\s+advisors|financial\s+planning|financial\s+management|'
    r'financial\s+solutions|financial\s+llc|financial\s+representative|financial\s+corp'
    r')\b',
    re.IGNORECASE
)

# Specific entries to remove regardless of patterns
SPECIFIC_REMOVES = {
    "PDF Bankrupt company settles with fraudster's law firm",
    "Tax and Accounting Services for Lawyers and Law Firms",
    "Motsinger CPA Tax & Accounting", "Bender & Company CPAs, PC",
    "Synergy Financial Partners", "Financial Partners Group",
    "Blazing Star Financial", "Baystone Financial LLC", "Allworth Financial",
    "Ranson Financial Group LLC", "Caliber Financial Services, Inc.", "World financial",
    "Cordley Elementary School", "City Hall", "Baker University", "Kansas University",
    "Ottawa University", "Pittsburg State University", "Emporia State University",
    "Washburn University", "University Of Kansas", "University of Kansas",
    "University Of KS Medical Center", "University Of KS Hospital Authority",
    "University of Kansas Health System", "University of Kansas Hospital Authority",
    "University of Kansas System, Inc.", "The University of Kansas Cancer Center",
    "The University of Kansas Health System", "The University of Kansas Heath System",
    "University Of KS Medical Center", "KU Medical Center",
    "AdventHealth", "AdventHealth Shawnee Mission", "Shawnee Mission Medical Center",
    "Stormont Vail Health", "Stormont Vail Health Foundation", "Sumner Mental Health Center",
    "KVC Health Systems Inc", "Dechra Veterinary Products", "Wildcat Veterinary Clinic",
    "Quest Diagnostics Clinical Laboratories Inc.", "Spectrum Home Health Inc",
    "Agiliti Health", "Alliance for a Healthy Kansas",
    "American Academy of Family Physicians", "American/Global Medical Response",
    "Defense Health Agency", "U.S. Department of Health and Human Services",
    "801 Restaurant Group", "Burgardt Retail Liquor LLC", "Salina Area Emergency Food Bank",
    "Reaser Construction", "Mammoth Sports Construction, LLC",
    "Delta Dental Of Kansas", "Delta Dental of Kansas", "Omnicare Pharmacy",
    "Topeka Public Schools", "Olathe Public Schools USD #233",
    "Shawnee Mission School District", "Unified School District No. 229",
    "St. Mary's Academy and College", "UMKC School of Law", "WU School Of Law",
    "Washburn School Of Law", "Washburn Univ. Law School", "Washburn University School of Law",
    "University of Kansas School of Law", "School Of Law",
    "Central Park Christian Church", "Church Of The Nazarene, Inc",
    "Church of the Nazarene", "Church of the Resurrection",
    "Social Security Administration", "Social Security Administration Office of Hearings Operations",
    "Leavenworth County Sheriff's Office", "Sedgwick County Sheriff's Office",
    "Shawnee County Sheriff's Office",
    "US Bankruptcy Court", "U. S. Bankruptcy Court", "U.S. Bankruptcy Court",
    "U.S. District Court of KS", "U.S. District Court of Kansas",
    "Nationwide Insurance", "Standard Insurance Company", "Ameritrust Insurance Group",
    "Aeris Insurance Solutions", "SILAC Insurance Company", "Conrade Insurance Group",
    "Relation Insurance Services", "Relation Insurance, Inc", "Relation Insurance, Inc.",
    "Travelers Insurance Co", "United Healthcare", "United Health Group Co: Optum Rx",
    "Hudson Insurance Group",
    "Stephanie Bulcock REALTOR - The Collective Compass",
    "First Washington Realty", "National Realty Advisors LLC",
    "Coldwell Banker Commercial - Griffith & Blair",
    "Dawn Lawson - COUNTRY Financial representative",
    "Rehab Robyn Assaf", "Sarah Bird, DNP - PALM Health",
    "University of Kansas, Office of the General Counsel",
    "Clinical Growth Strategies LLC", "Clinical Reference Laboratory Inc",
    "Midland Loan Services, a Division of PNC Bank NA",
    "Midland Loan Services, a div of PNC Bank NA", "Midland Loan Services",
    "BOK Financial", "CommunityAmerica Federal Credit Union",
    "Cancer Center of Kansas", "Capitol Federal", "Capitol Federal Savings",
    "Capitol Federal Savings Bank", "Central National Bank",
    "10th Judicial District Court",
    "10th Judicial District Court of Kansas, Johnson County",
    "District Court", "District Court Judge",
    "District Court of the First Judicial District",
    "District Court Judge 6th Judicial District",
    "District Court Trustee 29th Judicial District",
    "District Court Judge, State of Kansas",
    "Johnson County District Court Trustee",
    "Johnson County District Court Trustee's Office",
    "Fort Hays State University - General Counsel",
    "National Police Accountability Project",
    "McCaffree Financial Corp",
    "Enterprise Bank & Trust",
    "First Business Bank",
    "First Federal Bank of Kansas City",
    "Stearns Bank",
    "First National Bank of Hutchinson",
    "Hutchinson Regional Medical Center, Inc.",
    "Kansas State University Research Foundation",
    "Meritrust Credit Union",
    "Kansas Board Of Pharmacy",
    "ITCOY Home Health Care, LLC",
    "Lawson Elementary School",
    "PNC Bank, NA / PNC Real Estate",
}


def should_remove(name):
    if name in SPECIFIC_REMOVES:
        return True
    has_law = bool(LAW_INDICATORS.search(name))
    has_non_law = bool(NON_LAW_PATTERNS.search(name))
    return has_non_law and not has_law


def process_file(path, backup_dir):
    removed = []
    kept = []

    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            name = row.get('law_firm_name', '').strip()
            if should_remove(name):
                removed.append(name)
            else:
                kept.append(row)

    if not removed:
        return []

    # Back up original
    fname = os.path.basename(path)
    shutil.copy2(path, os.path.join(backup_dir, fname))

    # Write cleaned version
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    return removed


def main():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    csv_files = sorted(glob.glob(os.path.join(COUNTY_DATA_DIR, '*.csv')))
    total_removed = 0

    for path in csv_files:
        removed = process_file(path, BACKUP_DIR)
        if removed:
            fname = os.path.basename(path)
            print(f"\n{fname}: removed {len(removed)}")
            for name in removed:
                print(f"  - {name}")
            total_removed += len(removed)

    print(f"\n{'='*60}")
    print(f"Total removed: {total_removed} entries across {len(csv_files)} files")
    print(f"Backups saved to: {BACKUP_DIR}")


if __name__ == '__main__':
    main()
