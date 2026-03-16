const CSV_HEADERS = [
  "Name", "Practice Areas", "City", "County", "Phone", "Email", "Website",
  "Referral Match", "Contact Status", "Starred", "Notes", "Street Address", "Zip"
];

function _esc(val) {
  return '"' + String(val == null ? "" : val).replace(/"/g, '""') + '"';
}

function generateCSV(firms, userData) {
  const rows = [CSV_HEADERS.map(h => _esc(h)).join(",")];
  for (const f of firms) {
    rows.push([
      f.name,
      (f.practiceAreas || []).join("; "),
      f.address.city,
      f.address.county,
      f.phone || "",
      f.email || "",
      f.website || "",
      f.referralScore,
      (userData.status && userData.status[f.id]) || "uncontacted",
      (userData.starred && userData.starred[f.id]) ? "yes" : "no",
      (userData.notes && userData.notes[f.id]) || "",
      f.address.street,
      f.address.zip,
    ].map(_esc).join(","));
  }
  return rows.join("\n");
}

function downloadCSV(firms, userData, filename) {
  const csv = generateCSV(firms, userData);
  const blob = new Blob([csv], { type: "text/csv" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename || "kansas-law-firms.csv";
  a.click();
  URL.revokeObjectURL(a.href);
}

if (typeof module !== "undefined") module.exports = { generateCSV, downloadCSV };
