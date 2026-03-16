from scraper.utils.normalize import normalize_firm_name, normalize_practice_area, are_same_firm

class TestNormalizeFirmName:
    def test_lowercase(self):
        assert normalize_firm_name("SMITH LAW FIRM") == "smith"

    def test_strips_llc(self):
        assert normalize_firm_name("Smith & Associates LLC") == "smith associates"

    def test_strips_llp(self):
        assert normalize_firm_name("Johnson LLP") == "johnson"

    def test_strips_pc(self):
        assert normalize_firm_name("Williams Law PC") == "williams"

    def test_strips_law_firm(self):
        assert normalize_firm_name("Davis Law Firm") == "davis"

    def test_strips_law_office(self):
        assert normalize_firm_name("Miller Law Office") == "miller"

    def test_strips_attorneys_at_law(self):
        assert normalize_firm_name("Jones Attorneys at Law") == "jones"

    def test_strips_punctuation(self):
        assert normalize_firm_name("Brown & Green, P.A.") == "brown green"

    def test_strips_and_variations(self):
        assert normalize_firm_name("White and Black Associates") == "white black associates"

    def test_strips_pllc(self):
        assert normalize_firm_name("Taylor PLLC") == "taylor"

class TestAreSameFirm:
    def test_exact_match(self):
        assert are_same_firm("Smith & Associates LLC", "Smith and Associates") is True

    def test_different_firms(self):
        assert are_same_firm("Johnson Law Group", "Williams Law Firm") is False

    def test_partial_overlap_not_same(self):
        assert are_same_firm("Kansas City Law Group", "Lawrence Law Group") is False

    def test_slight_typo(self):
        assert are_same_firm("Smithe & Associates", "Smith & Associates") is True

class TestNormalizePracticeArea:
    def test_exact_match(self):
        assert normalize_practice_area("Personal Injury") == "Personal Injury"

    def test_case_insensitive(self):
        assert normalize_practice_area("personal injury") == "Personal Injury"

    def test_fuzzy_workers_comp(self):
        assert normalize_practice_area("Workers Comp") == "Workers' Compensation"

    def test_fuzzy_workers_compensation(self):
        assert normalize_practice_area("Workers' Compensation Law") == "Workers' Compensation"

    def test_unknown_returns_title_case(self):
        result = normalize_practice_area("Exotic Niche Law")
        assert result == "Exotic Niche Law"
