#!/usr/bin/env python3
"""Justia practice-area × city sweep for smaller counties."""
import re, csv, time, sys
from pathlib import Path
from curl_cffi import requests as creq
from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "app/county-data"
FIELDNAMES = ['law_firm_name','website','google_business_profile','legal_directory_listing',
              'city','state','county','phone_number','email','practice_area',
              'street_address','zip_code','msa','priority','number_of_lawyers']

PRACTICE_AREAS = [
    'agricultural-law','animal-dog-law','antitrust-law','appeals-appellate','arbitration-mediation',
    'asbestos-mesothelioma','bankruptcy','business-law','cannabis-marijuana-law','civil-rights',
    'collections','communications-internet-law','construction-law','consumer-law','criminal-law',
    'divorce','domestic-violence','dui-dwi','education-law','elder-law','employment-law',
    'energy-oil-gas-law','entertainment-sports-law','environmental-law','estate-planning',
    'family-law','foreclosure-defense','government-administrative-law','health-care-law',
    'immigration-law','insurance-claims','insurance-defense','intellectual-property',
    'international-law','juvenile-law','landlord-tenant','legal-malpractice','maritime-law',
    'medical-malpractice','military-law','municipal-law','native-american-law',
    'nursing-home-abuse','patents','personal-injury','probate','products-liability',
    'real-estate-law','securities-law','social-security-disability','stockbroker-investment-fraud',
    'tax-law','trademarks','traffic-tickets','white-collar-crime','workers-compensation',
]

# City → (county_slug, county_name, valid_cities_set)
CITY_TARGETS = [
    # Leavenworth County
    ('leavenworth', 'leavenworth-county-ks', 'Leavenworth', {'leavenworth','lansing','basehor','tonganoxie','linwood','easton','fort leavenworth'}),
    ('lansing', 'leavenworth-county-ks', 'Leavenworth', {'leavenworth','lansing','basehor','tonganoxie','linwood','easton','fort leavenworth'}),
    # Miami County
    ('paola', 'miami-county-ks', 'Miami', {'paola','osawatomie','louisburg','fontana'}),
    ('osawatomie', 'miami-county-ks', 'Miami', {'paola','osawatomie','louisburg','fontana'}),
    ('louisburg', 'miami-county-ks', 'Miami', {'paola','osawatomie','louisburg','fontana'}),
    # Linn County
    ('pleasanton', 'linn-county-ks', 'Linn', {'pleasanton','la cygne','mound city','prescott','blue mound'}),
    ('la-cygne', 'linn-county-ks', 'Linn', {'pleasanton','la cygne','mound city','prescott','blue mound'}),
    ('mound-city', 'linn-county-ks', 'Linn', {'pleasanton','la cygne','mound city','prescott','blue mound'}),
    # Wyandotte County (additional coverage)
    ('kansas-city', 'wyandotte-county-ks', 'Wyandotte', {'kansas city','bonner springs','edwardsville'}),
    ('bonner-springs', 'wyandotte-county-ks', 'Wyandotte', {'kansas city','bonner springs','edwardsville'}),
]

def normalize(name):
    name = name.lower().strip()
    name = re.sub(r'\b(law|firm|office|offices|group|llc|llp|pc|pa|pllc|plc|&|and|the|attorney|attorneys|at|of|lawyer|lawyers|chartered|chtd)\b', '', name)
    return re.sub(r'[^a-z0-9]', '', name)

def load_fresh(county_slug):
    path = DATA_DIR / f'{county_slug}.csv'
    rows = list(csv.DictReader(open(path)))
    seen = {normalize(r.get('law_firm_name','')) + '|' + r.get('city','').lower() for r in rows}
    return rows, seen

def save_merged(county_slug, new_entries):
    disk_rows, disk_seen = load_fresh(county_slug)
    adds = [e for e in new_entries if (normalize(e['law_firm_name']) + '|' + e['city'].lower()) not in disk_seen]
    if not adds:
        return 0
    path = DATA_DIR / f'{county_slug}.csv'
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(disk_rows + adds)
    return len(adds)

def extract_cards(soup, valid_cities, county_name, seen):
    new = []
    for card in soup.find_all(class_=re.compile(r'jld-card')):
        name_el = card.find('a', title=True)
        if not name_el:
            continue
        name = name_el.get('title','').replace(' - Profile', '').strip()
        profile_url = name_el.get('href', '')
        text = card.get_text(separator='|', strip=True)
        
        city_m = re.search(r'\|([A-Za-z][A-Za-z\s\.]+),\s*KS\s+(?:Attorney|Lawyer)', text)
        if city_m:
            city = city_m.group(1).strip()
        else:
            city_m2 = re.search(r'([A-Za-z][A-Za-z\s\.]+),\s*KS\b', text)
            city = city_m2.group(1).strip() if city_m2 else ''
        
        if not city or city.lower() not in valid_cities:
            continue
        
        firm_m = re.search(r'\|([^|]+(?:Law|Legal|Attorneys?|Firm|Group|LLC|LLP|PC|PA)[^|]*)\|', text)
        firm = firm_m.group(1).strip() if firm_m else name
        
        phone_m = re.search(r'\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}', text)
        phone = phone_m.group() if phone_m else ''
        
        key = normalize(firm) + '|' + city.lower()
        if key in seen or not normalize(firm):
            continue
        seen.add(key)
        new.append({
            'law_firm_name': firm,
            'website': '', 'google_business_profile': '', 'legal_directory_listing': profile_url,
            'city': city, 'state': 'KS', 'county': county_name,
            'phone_number': phone, 'email': '', 'practice_area': 'General',
            'street_address': '', 'zip_code': '', 'msa': 'Kansas City',
            'priority': '2', 'number_of_lawyers': '',
        })
    return new

def main():
    session = creq.Session(impersonate='chrome120')
    
    county_new = {}
    county_seen = {}
    for _, county_slug, _, _ in CITY_TARGETS:
        if county_slug not in county_new:
            county_new[county_slug] = []
            _, seen = load_fresh(county_slug)
            county_seen[county_slug] = seen
    
    total = 0
    for city_slug, county_slug, county_name, valid_cities in CITY_TARGETS:
        city_total = 0
        seen = county_seen[county_slug]
        
        for pa in PRACTICE_AREAS:
            url = f'https://www.justia.com/lawyers/{pa}/kansas/{city_slug}/'
            for page in range(1, 6):
                req_url = url if page == 1 else f'{url}?page={page}'
                try:
                    r = session.get(req_url, timeout=15)
                except:
                    break
                if r.status_code == 429:
                    print(f'  429 on {pa}/{city_slug} p{page}, sleeping 60s')
                    time.sleep(60)
                    continue
                if r.status_code != 200:
                    break
                
                soup = BeautifulSoup(r.text, 'lxml')
                new = extract_cards(soup, valid_cities, county_name, seen)
                county_new[county_slug].extend(new)
                city_total += len(new)
                
                if len(new) > 0 and page < 5:
                    if not soup.find('a', string=re.compile(r'Next|next')):
                        break
                    time.sleep(0.8)
                else:
                    break
                time.sleep(0.5)
            
            time.sleep(0.3)
        
        # Save incrementally per city
        if county_new[county_slug]:
            saved = save_merged(county_slug, county_new[county_slug])
            if saved > 0:
                print(f'{city_slug} ({county_slug}): +{saved} saved')
                total += saved
            county_new[county_slug] = []  # clear after save
        else:
            print(f'{city_slug}: 0 new')
        
        time.sleep(2)
    
    print(f'\nTotal new entries: {total}')

if __name__ == '__main__':
    main()
