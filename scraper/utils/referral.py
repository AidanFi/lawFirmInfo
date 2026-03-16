COMPETITOR = {"Personal Injury", "Litigation", "Trial Law", "Wrongful Death"}
HIGH = {"Family Law", "Criminal Defense", "Estate Planning", "Probate",
        "Workers' Compensation", "Bankruptcy", "Social Security Disability"}
MEDIUM = {"Business Law", "Real Estate", "Immigration", "Employment Law",
          "Medical Malpractice", "Insurance Defense"}


def calculate_referral_score(practice_areas: list[str]) -> str:
    areas = set(practice_areas)
    if areas & COMPETITOR:
        return "competitor"
    if areas & HIGH:
        return "high"
    if areas & MEDIUM:
        return "medium"
    return "low"
