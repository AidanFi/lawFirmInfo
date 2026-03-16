const KEYS = {
  starred: "ks_law_starred",
  notes:   "ks_law_notes",
  status:  "ks_law_status",
  settings:"ks_law_settings",
};

function _get(key) { try { return JSON.parse(localStorage.getItem(key) || "{}"); } catch { return {}; } }
function _set(key, val) { localStorage.setItem(key, JSON.stringify(val)); }

function getStar(id) { return !!_get(KEYS.starred)[id]; }
function setStar(id, val) { const d = _get(KEYS.starred); d[id] = val; _set(KEYS.starred, d); }

function getNote(id) { return _get(KEYS.notes)[id] || ""; }
function setNote(id, val) { const d = _get(KEYS.notes); d[id] = val; _set(KEYS.notes, d); }

function getStatus(id) { return _get(KEYS.status)[id] || "uncontacted"; }
function setStatus(id, val) { const d = _get(KEYS.status); d[id] = val; _set(KEYS.status, d); }

function getSettings() {
  const d = _get(KEYS.settings);
  return { defaultView: d.defaultView || "cards", myPractice: d.myPractice || "Personal Injury" };
}
function saveSettings(settings) { _set(KEYS.settings, settings); }

function getAllUserData() {
  return { starred: _get(KEYS.starred), notes: _get(KEYS.notes), status: _get(KEYS.status) };
}

if (typeof module !== "undefined") module.exports = { getStar, setStar, getNote, setNote, getStatus, setStatus, getSettings, saveSettings, getAllUserData };
