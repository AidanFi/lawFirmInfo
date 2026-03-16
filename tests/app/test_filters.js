const { applyFilters } = require("../../app/filters.js");

const FIRMS = [
  { id: "1", name: "Smith Law", practiceAreas: ["Family Law"], address: { city: "Wichita", county: "Sedgwick" }, referralScore: "high" },
  { id: "2", name: "Jones PI", practiceAreas: ["Personal Injury"], address: { city: "Topeka", county: "Shawnee" }, referralScore: "competitor" },
  { id: "3", name: "Brown Estate", practiceAreas: ["Estate Planning"], address: { city: "Wichita", county: "Sedgwick" }, referralScore: "high" },
];
const USER_DATA = { starred: { "1": true }, status: { "2": "partner" } };

test("no filters returns all firms", () => {
  const f = { search: "", referralScores: ["competitor","high","medium","low"], practiceAreas: [], city: "", county: "", contactStatuses: ["uncontacted","reached_out","partner","not_interested"], starredOnly: false };
  expect(applyFilters(FIRMS, f, USER_DATA).length).toBe(3);
});

test("search by name filters correctly", () => {
  const f = { search: "smith", referralScores: ["competitor","high","medium","low"], practiceAreas: [], city: "", county: "", contactStatuses: ["uncontacted","reached_out","partner","not_interested"], starredOnly: false };
  expect(applyFilters(FIRMS, f, USER_DATA).length).toBe(1);
});

test("referral score filter works", () => {
  const f = { search: "", referralScores: ["high"], practiceAreas: [], city: "", county: "", contactStatuses: ["uncontacted","reached_out","partner","not_interested"], starredOnly: false };
  const result = applyFilters(FIRMS, f, USER_DATA);
  expect(result.length).toBe(2);
  expect(result.every(f => f.referralScore === "high")).toBe(true);
});

test("practice area filter works", () => {
  const f = { search: "", referralScores: ["competitor","high","medium","low"], practiceAreas: ["Family Law"], city: "", county: "", contactStatuses: ["uncontacted","reached_out","partner","not_interested"], starredOnly: false };
  const result = applyFilters(FIRMS, f, USER_DATA);
  expect(result.length).toBe(1);
  expect(result[0].id).toBe("1");
});

test("city filter works", () => {
  const f = { search: "", referralScores: ["competitor","high","medium","low"], practiceAreas: [], city: "Topeka", county: "", contactStatuses: ["uncontacted","reached_out","partner","not_interested"], starredOnly: false };
  expect(applyFilters(FIRMS, f, USER_DATA).length).toBe(1);
});

test("starred only filter works", () => {
  const f = { search: "", referralScores: ["competitor","high","medium","low"], practiceAreas: [], city: "", county: "", contactStatuses: ["uncontacted","reached_out","partner","not_interested"], starredOnly: true };
  const result = applyFilters(FIRMS, f, USER_DATA);
  expect(result.length).toBe(1);
  expect(result[0].id).toBe("1");
});

test("contact status filter works", () => {
  const f = { search: "", referralScores: ["competitor","high","medium","low"], practiceAreas: [], city: "", county: "", contactStatuses: ["partner"], starredOnly: false };
  const result = applyFilters(FIRMS, f, USER_DATA);
  expect(result.length).toBe(1);
  expect(result[0].id).toBe("2");
});
