from scraper.utils.referral import calculate_referral_score

def test_competitor():
    assert calculate_referral_score(["Personal Injury"]) == "competitor"

def test_high():
    assert calculate_referral_score(["Family Law"]) == "high"

def test_medium():
    assert calculate_referral_score(["Business Law"]) == "medium"

def test_low():
    assert calculate_referral_score(["Tax Law"]) == "low"

def test_competitor_wins_over_high():
    assert calculate_referral_score(["Personal Injury", "Family Law"]) == "competitor"

def test_high_wins_over_medium():
    assert calculate_referral_score(["Family Law", "Business Law"]) == "high"

def test_empty_is_low():
    assert calculate_referral_score([]) == "low"
