const { generateCSV, downloadCSV } = require("../../app/export.js");

const FIRMS = [{
  id: "1", name: "Smith Law", practiceAreas: ["Family Law", "Criminal Defense"],
  address: { street: "123 Main", city: "Wichita", county: "Sedgwick", zip: "67202" },
  phone: "(316) 555-0100", email: "smith@example.com", website: "https://smith.com",
  referralScore: "high"
}];
const USER_DATA = { starred: { "1": true }, notes: { "1": "Great contact" }, status: { "1": "partner" } };

test("generateCSV returns string", () => {
  const csv = generateCSV(FIRMS, USER_DATA);
  expect(typeof csv).toBe("string");
});

test("CSV has header row", () => {
  const csv = generateCSV(FIRMS, USER_DATA);
  const firstLine = csv.split("\n")[0];
  expect(firstLine).toContain("Name");
  expect(firstLine).toContain("Practice Areas");
  expect(firstLine).toContain("Notes");
});

test("CSV has correct data row", () => {
  const csv = generateCSV(FIRMS, USER_DATA);
  expect(csv).toContain("Smith Law");
  expect(csv).toContain("Family Law; Criminal Defense");
  expect(csv).toContain("Great contact");
  expect(csv).toContain("partner");
  expect(csv).toContain("yes");  // starred
});

test("CSV escapes quotes in notes", () => {
  const ud = { starred: {}, notes: { "1": 'Has "quotes"' }, status: {} };
  const csv = generateCSV(FIRMS, ud);
  expect(csv).toContain('Has ""quotes""');
});
