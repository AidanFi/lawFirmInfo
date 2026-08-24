"""Add offers_auto / offers_home columns to all 4 insurance CSVs.

offers_auto is Yes for every existing row (that was our original inclusion
filter). offers_home is inferred from well-established carrier product-line
facts: virtually every captive P&C carrier and independent multi-line
agency in these files also writes homeowners insurance, EXCEPT a small,
identifiable set of non-standard/high-risk-auto-only specialists that do
not carry a home product at all.
"""
import csv
import glob

# Non-standard/high-risk auto-only specialists: no homeowners product.
NO_HOME_MARKERS = [
    "non-standard auto",
    "sr-22",
    "sr22",
    "freeway insurance",
    "first chicago insurance",
]

COLS_NEW = ["agent_name", "agency_name", "company", "agent_type", "offers_auto", "offers_home",
            "website", "phone_number", "email", "street_address", "city", "state", "county",
            "zip_code", "date_pulled", "source"]


def offers_home(company: str) -> str:
    c = (company or "").lower()
    for marker in NO_HOME_MARKERS:
        if marker in c:
            return "No"
    return "Yes"


def main():
    for path in sorted(glob.glob("app/county-data/insurance-*-county-ks.csv")):
        with open(path) as f:
            rows = list(csv.DictReader(f))

        for r in rows:
            r["offers_auto"] = "Yes"
            r["offers_home"] = offers_home(r.get("company"))

        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLS_NEW)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in COLS_NEW})

        no_home = sum(1 for r in rows if r["offers_home"] == "No")
        print(f"{path}: {len(rows)} rows, {no_home} marked offers_home=No")


if __name__ == "__main__":
    main()
