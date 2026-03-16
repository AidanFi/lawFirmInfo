let _map = null;
let _markerLayer = null;

const SCORE_COLORS = {
  high: "#22c55e",
  medium: "#f59e0b",
  low: "#6b7280",
  competitor: "#ef4444",
};

function initMap() {
  if (_map) return;
  _map = L.map("map").setView([38.5, -98.0], 7);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a> contributors',
    maxZoom: 18,
  }).addTo(_map);
  _markerLayer = L.markerClusterGroup();
  _map.addLayer(_markerLayer);
}

function renderMapPins(firms, userData, onStarToggle, onStatusChange) {
  if (!_map) initMap();
  _markerLayer.clearLayers();

  const unmapped = [];

  for (const firm of firms) {
    if (!firm.coordinates) { unmapped.push(firm); continue; }

    const color = SCORE_COLORS[firm.referralScore] || SCORE_COLORS.low;
    const isStarred = userData.starred && userData.starred[firm.id];
    const status = (userData.status && userData.status[firm.id]) || "uncontacted";

    const marker = L.circleMarker([firm.coordinates.lat, firm.coordinates.lng], {
      radius: 8,
      fillColor: color,
      color: isStarred ? "#f59e0b" : "#fff",
      weight: isStarred ? 3 : 1.5,
      opacity: 1,
      fillOpacity: 0.85,
    });

    const popupHtml = `
      <div style="min-width:180px;font-size:13px">
        <strong>${firm.name}</strong><br>
        <span style="color:#94a3b8;font-size:11px">${(firm.practiceAreas || []).join(", ") || "—"}</span><br>
        <span style="color:#64748b;font-size:11px">${firm.address.city} · ${firm.phone || "—"}</span><br>
        ${firm.website ? `<a href="${firm.website}" target="_blank" style="font-size:11px">Website →</a><br>` : ""}
        <div style="margin-top:6px;display:flex;align-items:center;justify-content:space-between">
          <button onclick="window.__mapToggleStar('${firm.id}')" style="background:none;border:none;cursor:pointer;font-size:16px">${isStarred ? "★" : "☆"}</button>
          <select onchange="window.__mapSetStatus('${firm.id}', this.value)" style="font-size:11px">
            ${["uncontacted","reached_out","partner","not_interested"].map(s =>
              `<option value="${s}"${status === s ? " selected" : ""}>${s.replace(/_/g," ")}</option>`
            ).join("")}
          </select>
        </div>
      </div>`;

    marker.bindPopup(popupHtml);
    _markerLayer.addLayer(marker);
  }

  return unmapped;
}

// Expose callbacks for popup buttons
window.__mapToggleStar = function(id) {};
window.__mapSetStatus = function(id, status) {};

function invalidateMapSize() {
  if (_map) setTimeout(() => _map.invalidateSize(), 100);
}
