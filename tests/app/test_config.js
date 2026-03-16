// config.js uses `if (typeof module !== "undefined") module.exports = {...}` so require() works
const { PRACTICE_COMPATIBILITY, calculateReferralScore } = require("../../app/config.js");

test("calculateReferralScore returns competitor for PI", () => {
  expect(calculateReferralScore(["Personal Injury"], "Personal Injury")).toBe("competitor");
});

test("calculateReferralScore returns high for Family Law when user is PI", () => {
  expect(calculateReferralScore(["Family Law"], "Personal Injury")).toBe("high");
});

test("calculateReferralScore competitor wins over high", () => {
  expect(calculateReferralScore(["Personal Injury", "Family Law"], "Personal Injury")).toBe("competitor");
});

test("calculateReferralScore returns low for unmatched", () => {
  expect(calculateReferralScore(["Tax Law"], "Personal Injury")).toBe("low");
});

test("calculateReferralScore works for Family Law user", () => {
  expect(calculateReferralScore(["Family Law"], "Family Law")).toBe("competitor");
  expect(calculateReferralScore(["Criminal Defense"], "Family Law")).toBe("high");
});

test("PRACTICE_COMPATIBILITY has all 8 practices", () => {
  const keys = Object.keys(PRACTICE_COMPATIBILITY);
  expect(keys).toContain("Personal Injury");
  expect(keys).toContain("Bankruptcy");
  expect(keys.length).toBe(8);
});
