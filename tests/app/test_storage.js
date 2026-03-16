const { getStar, setStar, getNote, setNote, getStatus, setStatus, getSettings, saveSettings, getAllUserData } = require("../../app/storage.js");

beforeEach(() => localStorage.clear());

test("getStar returns false when not set", () => {
  expect(getStar("firm-1")).toBe(false);
});

test("setStar persists", () => {
  setStar("firm-1", true);
  expect(getStar("firm-1")).toBe(true);
});

test("getNote returns empty string when not set", () => {
  expect(getNote("firm-1")).toBe("");
});

test("setNote persists", () => {
  setNote("firm-1", "great firm");
  expect(getNote("firm-1")).toBe("great firm");
});

test("getStatus returns uncontacted by default", () => {
  expect(getStatus("firm-1")).toBe("uncontacted");
});

test("setStatus persists", () => {
  setStatus("firm-1", "partner");
  expect(getStatus("firm-1")).toBe("partner");
});

test("getSettings returns defaults", () => {
  const s = getSettings();
  expect(s.defaultView).toBe("cards");
  expect(s.myPractice).toBe("Personal Injury");
});

test("saveSettings persists", () => {
  saveSettings({ defaultView: "table", myPractice: "Family Law" });
  expect(getSettings().defaultView).toBe("table");
});

test("getAllUserData returns combined object", () => {
  setStar("f1", true);
  setNote("f1", "note");
  setStatus("f1", "partner");
  const data = getAllUserData();
  expect(data.starred["f1"]).toBe(true);
  expect(data.notes["f1"]).toBe("note");
  expect(data.status["f1"]).toBe("partner");
});
