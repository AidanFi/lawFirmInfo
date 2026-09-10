#!/usr/bin/env python3
"""
Harvest individual attorney detail pages from the State Bar of Texas
member directory (MemberDirectoryDetail.cfm?ContactID=N) for every
contact_id referenced in <slug>_row_contact_ids.json (built by
statebar_to_csv.py).

Each detail page can carry a self-reported "VISIT WEBSITE" link — the
attorney's own website, entered directly into their bar profile. This is
authoritative (came from the attorney, not a guess) and a much stronger
source than domain-guessing. It can also carry explicit practice areas.

Usage: python3 texasbar_detail_harvest.py <slug> [--workers N]
"""
import argparse
import json
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

CACHE_DIR = Path("data/county")
DETAIL_URL = (
    "https://www.texasbar.com/AM/Template.cfm?Section=Find_A_Lawyer"
    "&template=/Customsource/MemberDirectory/MemberDirectoryDetail.cfm&ContactID={cid}"
)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

_CONTACT_SECTION_RE = re.compile(r'<h5>Contact Information</h5>.*?</div>', re.S)
_WEBSITE_RE = re.compile(r'href="(https?://[^"]+)"\s*target="_blank"\s*>\s*VISIT WEBSITE', re.IGNORECASE)
_PRACTICE_AREAS_RE = re.compile(r'<strong>Practice Areas:\s*</strong>\s*([^<]+)')


def fetch_one(contact_id: str) -> dict:
    url = DETAIL_URL.format(cid=contact_id)
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code != 200:
                return {"contact_id": contact_id, "website": "", "practice_areas": ""}
            text = r.text
            website = ""
            m = _CONTACT_SECTION_RE.search(text)
            if m:
                wm = _WEBSITE_RE.search(m.group(0))
                if wm:
                    # Some attorneys enter multiple URLs in the single
                    # website field (e.g. "http://a.com; www.b.com; www.c.com")
                    # — the bar's template dumps that raw into the href, so
                    # take just the first one.
                    raw = wm.group(1).strip()
                    website = re.split(r"[;\s,]+", raw)[0]
            pa_m = _PRACTICE_AREAS_RE.search(text)
            practice_areas = pa_m.group(1).strip() if pa_m else ""
            return {"contact_id": contact_id, "website": website, "practice_areas": practice_areas}
        except Exception:
            time.sleep(1 + attempt)
    return {"contact_id": contact_id, "website": "", "practice_areas": ""}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--workers", type=int, default=25)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    sidecar_path = CACHE_DIR / f"{args.slug}_row_contact_ids.json"
    sidecar = json.loads(sidecar_path.read_text())
    all_ids = sorted({cid for ids in sidecar.values() for cid in ids})
    if args.limit:
        all_ids = all_ids[: args.limit]
    print(f"{len(all_ids)} unique contact_ids to fetch")

    out_path = CACHE_DIR / f"{args.slug}_detail_cache.json"
    results = {}
    if out_path.exists():
        results = json.loads(out_path.read_text())
        remaining = [cid for cid in all_ids if cid not in results]
        print(f"Resuming: {len(results)} already fetched, {len(remaining)} remaining")
        all_ids = remaining

    done = 0
    website_found = 0
    save_every = 500

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(fetch_one, cid): cid for cid in all_ids}
        for fut in as_completed(futures):
            res = fut.result()
            results[res["contact_id"]] = {"website": res["website"], "practice_areas": res["practice_areas"]}
            if res["website"]:
                website_found += 1
            done += 1
            if done % 200 == 0:
                print(f"  progress: {done}/{len(all_ids)} | websites found: {website_found}")
            if done % save_every == 0:
                out_path.write_text(json.dumps(results, indent=1))

    out_path.write_text(json.dumps(results, indent=1))
    print(f"\nDone. {website_found} websites found among {len(all_ids)} fetched this run.")
    print(f"Total cached: {len(results)}")


if __name__ == "__main__":
    main()
