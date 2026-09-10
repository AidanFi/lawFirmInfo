"""Extract a phone number and email address from an already-fetched law
firm webpage's HTML text. Used alongside law_domain_guess.py so a single
page fetch can yield website + phone + email together.
"""
import re

_NOISE_BLOCK_RE = re.compile(
    r'<script\b[^>]*>.*?</script>|<style\b[^>]*>.*?</style>|<!--.*?-->',
    re.IGNORECASE | re.DOTALL,
)


def _strip_noise(text: str) -> str:
    """Remove <script>/<style>/HTML-comment blocks before scanning for
    contact info — verified in the wild that a font's embedded copyright
    notice inside a <style> block (e.g. a Google Fonts @font-face license
    comment naming the font's author) can look exactly like a real mailto
    match, misattributing a font designer's email to the firm."""
    return _NOISE_BLOCK_RE.sub(" ", text)


_PHONE_RE = re.compile(
    r"(?:\+?1[\s\-.]?)?\(?(\d{3})\)?[\s\-.]{1,2}(\d{3})[\s\-.]{1,2}(\d{4})\b"
)

_MAILTO_RE = re.compile(r'mailto:([^"\'?\s>]+)', re.IGNORECASE)
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

_BAD_EMAIL_DOMAINS = (
    "sentry.io", "wixpress.com", "godaddy.com", "example.com", "w3.org",
    "schema.org", "yourdomain.com", "domain.com", "email.com", "yoursite.com",
    "wordpress.com", "gravatar.com", "cloudflare.com", "google.com",
    "googlemail.com", "sentry-next.io", ".png", ".jpg", ".jpeg", ".gif", ".svg",
    ".webp", "2x.png", "wixapps.net",
    "ejemplo.com", "correo.com", "tudominio.com", "exemplo.com",  # translated placeholder domains
    "mysite.com",  # Wix's default unconfigured-template email domain
    "townsquareinteractive.com",  # a website-builder/marketing vendor's own support inbox, not the firm's
)

# Placeholder local-parts seen across template sites in multiple languages
# (English/Spanish/Portuguese) — a domain-based check alone missed
# "tucorreo@ejemplo.com" ("your-email@example.com"), a template default
# left unfilled on a real firm's site.
_BAD_EMAIL_LOCALPARTS = (
    "tucorreo", "correo", "youremail", "your-email", "yourname", "testemail",
    "test", "example", "sample", "placeholder", "seunome", "seuemail",
    "anymail",
)

_BAD_EMAIL_EXACT = ("john@doe.com", "jane@doe.com", "john@example.com")

_BAD_PHONE_PREFIXES = ("000", "111", "123", "555")


def extract_phone(text: str, prefer_context: str = "") -> str:
    """Return a plausible US phone number, preferring one that appears near
    `prefer_context` (e.g. "houston") if given."""
    text = _strip_noise(text)
    candidates = []
    for m in _PHONE_RE.finditer(text):
        area, exch, line = m.groups()
        if area[0] in "01" or exch[0] in "01":
            continue
        if area in _BAD_PHONE_PREFIXES or exch in _BAD_PHONE_PREFIXES:
            continue
        candidates.append((m.start(), f"{area}-{exch}-{line}"))

    if not candidates:
        return ""

    if prefer_context:
        lower = text.lower()
        ctx_positions = [m.start() for m in re.finditer(re.escape(prefer_context.lower()), lower)]
        # Tight window first (an office-contact block is usually compact —
        # city/address/phone within a couple hundred characters of each
        # other); only widen if nothing qualifies, to avoid pulling in an
        # unrelated distant office's number on a multi-office firm's page.
        for window in (300, 2000):
            for ctx_pos in ctx_positions:
                nearby = sorted(candidates, key=lambda c: abs(c[0] - ctx_pos))
                if nearby and abs(nearby[0][0] - ctx_pos) < window:
                    return nearby[0][1]

    # Dedup while preserving first-seen order, prefer the most frequently
    # repeated number (usually the firm's real main line, vs. a one-off
    # fax/other number mentioned once).
    from collections import Counter
    counts = Counter(c[1] for c in candidates)
    return counts.most_common(1)[0][0]


def extract_email(text: str, firm_domain: str = "") -> str:
    """Return a plausible contact email, preferring one on the firm's own
    domain and a generic-looking mailbox (info@/contact@/office@) over an
    individual attorney's personal address."""
    text = _strip_noise(text)
    found = set()
    for m in _MAILTO_RE.finditer(text):
        addr = m.group(1).split("?")[0].strip()
        if "@" in addr:
            found.add(addr.lower())
    for m in _EMAIL_RE.finditer(text):
        found.add(m.group(0).lower())

    candidates = []
    for addr in found:
        if addr in _BAD_EMAIL_EXACT:
            continue
        local_part, _, domain = addr.partition("@")
        if any(bad in domain for bad in _BAD_EMAIL_DOMAINS):
            continue
        if local_part in _BAD_EMAIL_LOCALPARTS:
            continue
        if any(addr.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
            continue
        candidates.append(addr)

    if not candidates:
        return ""

    def score(addr: str) -> tuple:
        domain = addr.split("@")[-1]
        on_firm_domain = firm_domain and firm_domain in domain
        generic = addr.split("@")[0] in (
            "info", "contact", "office", "admin", "intake", "inquiries", "hello", "mail",
        )
        return (on_firm_domain, generic)

    candidates.sort(key=score, reverse=True)
    return candidates[0]
