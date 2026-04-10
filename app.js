(function () {
  // ── State ──────────────────────────────────────────────────────────────────
  const settings = getSettings();
  const PAGE_SIZE = 100;
  let state = {
    allFirms: [],
    filteredFirms: [],
    filters: {
      search: "",
      referralScores: ["competitor", "high", "medium", "low"],
      practiceAreas: [],
      city: "",
      county: "",
      contactStatuses: ["uncontacted", "reached_out", "partner", "not_interested"],
      starredOnly: false,
      hasWebsite: false,
      hasPracticeArea: false,
      hasContact: false,
    },
    view: settings.defaultView || "cards",
    tab: "directory",
    sort: { col: "referralScore", dir: "desc" },
    expandedCardId: null,
    expandedRowId: null,
    displayCount: PAGE_SIZE,
  };
  let userData = getAllUserData();

  const SCORE_ORDER = { competitor: 1, high: 2, medium: 3, low: 4 };

  // ── Init ───────────────────────────────────────────────────────────────────
  function init() {
    // Check both: onerror flag (unreliable on file://) AND direct existence check (reliable)
    if (window.__firmsLoadError || typeof FIRMS_DATA === "undefined" || !FIRMS_DATA.firms || FIRMS_DATA.firms.length === 0) {
      document.getElementById("no-data").classList.remove("hidden");
      return;
    }

    document.getElementById("app-body").classList.remove("hidden");
    state.allFirms = FIRMS_DATA.firms.map(f => ({
      ...f,
      referralScore: calculateReferralScore(f.practiceAreas, settings.myPractice),
    }));

    populateSidebarOptions();
    applySettingsToUI();
    bindEvents();
    rerender();
    updateDataInfo();
  }

  // ── Populate sidebar dropdowns/checkboxes ──────────────────────────────────
  function populateSidebarOptions() {
    const allAreas = [...new Set(state.allFirms.flatMap(f => f.practiceAreas))].sort();
    const container = document.getElementById("practice-checkboxes");
    container.innerHTML = allAreas.map(a =>
      `<label><input type="checkbox" name="practice" value="${a}"> ${a}</label>`
    ).join("");

    const cities = [...new Set(state.allFirms.map(f => f.address.city))].filter(Boolean).sort();
    const citySelect = document.getElementById("city-filter");
    cities.forEach(c => { const o = document.createElement("option"); o.value = c; o.textContent = c; citySelect.appendChild(o); });

    const counties = [...new Set(state.allFirms.map(f => f.address.county))].filter(Boolean).sort();
    const countySelect = document.getElementById("county-filter");
    counties.forEach(c => { const o = document.createElement("option"); o.value = c; o.textContent = c; countySelect.appendChild(o); });
  }

  function applySettingsToUI() {
    document.getElementById("my-practice").value = settings.myPractice;
    const isCards = settings.defaultView !== "table";
    document.getElementById("settings-cards-btn").classList.toggle("active", isCards);
    document.getElementById("settings-table-btn").classList.toggle("active", !isCards);
    setView(settings.defaultView || "cards");
  }

  // ── Event binding ──────────────────────────────────────────────────────────
  function bindEvents() {
    // Tabs
    document.querySelectorAll(".tab").forEach(btn =>
      btn.addEventListener("click", () => switchTab(btn.dataset.tab))
    );

    // Search
    document.getElementById("search").addEventListener("input", e => {
      state.filters.search = e.target.value;
      rerender();
    });

    // Score checkboxes
    document.querySelectorAll('[name="score"]').forEach(cb =>
      cb.addEventListener("change", () => {
        state.filters.referralScores = [...document.querySelectorAll('[name="score"]:checked')].map(c => c.value);
        rerender();
      })
    );

    // Practice area checkboxes
    document.getElementById("practice-checkboxes").addEventListener("change", () => {
      state.filters.practiceAreas = [...document.querySelectorAll('[name="practice"]:checked')].map(c => c.value);
      rerender();
    });

    // City / county
    document.getElementById("city-filter").addEventListener("change", e => { state.filters.city = e.target.value; rerender(); });
    document.getElementById("county-filter").addEventListener("change", e => { state.filters.county = e.target.value; rerender(); });

    // Status checkboxes
    document.querySelectorAll('[name="status"]').forEach(cb =>
      cb.addEventListener("change", () => {
        state.filters.contactStatuses = [...document.querySelectorAll('[name="status"]:checked')].map(c => c.value);
        rerender();
      })
    );

    // Data availability filters
    document.getElementById("has-website").addEventListener("change", e => { state.filters.hasWebsite = e.target.checked; rerender(); });
    document.getElementById("has-practice-area").addEventListener("change", e => { state.filters.hasPracticeArea = e.target.checked; rerender(); });
    document.getElementById("has-contact").addEventListener("change", e => { state.filters.hasContact = e.target.checked; rerender(); });

    // Starred only
    document.getElementById("starred-only").addEventListener("change", e => { state.filters.starredOnly = e.target.checked; rerender(); });

    // Clear filters
    document.getElementById("clear-filters").addEventListener("click", e => { e.preventDefault(); resetFilters(); });

    // View toggle
    document.getElementById("view-cards-btn").addEventListener("click", () => { setView("cards"); rerender(); });
    document.getElementById("view-table-btn").addEventListener("click", () => { setView("table"); rerender(); });

    // Export
    document.getElementById("export-filtered-btn").addEventListener("click", () =>
      downloadCSV(state.filteredFirms, userData, "kansas-law-firms-filtered.csv")
    );
    document.getElementById("export-all-btn").addEventListener("click", () =>
      downloadCSV(state.allFirms, userData, "kansas-law-firms-all.csv")
    );
    document.getElementById("export-starred-btn").addEventListener("click", () => {
      const starred = state.allFirms.filter(f => userData.starred[f.id]);
      downloadCSV(starred, userData, "kansas-law-firms-starred.csv");
    });

    // Settings
    document.getElementById("my-practice").addEventListener("change", e => {
      settings.myPractice = e.target.value;
      saveSettings(settings);
      state.allFirms = state.allFirms.map(f => ({
        ...f,
        referralScore: calculateReferralScore(f.practiceAreas, settings.myPractice),
      }));
      rerender();
    });

    document.querySelectorAll("[data-view]").forEach(btn =>
      btn.addEventListener("click", () => {
        const v = btn.dataset.view;
        settings.defaultView = v;
        saveSettings(settings);
        document.querySelectorAll("[data-view]").forEach(b => b.classList.toggle("active", b.dataset.view === v));
        setView(v);
      })
    );

    // Table sort
    document.querySelectorAll("#firms-table th[data-col]").forEach(th =>
      th.addEventListener("click", () => {
        const col = th.dataset.col;
        if (state.sort.col === col) {
          state.sort.dir = state.sort.dir === "asc" ? "desc" : "asc";
        } else {
          state.sort.col = col;
          state.sort.dir = col === "referralScore" ? "desc" : "asc";
        }
        renderTable(state.filteredFirms);
        updateSortIndicators();
      })
    );

    // Map popup callbacks — must be window globals because Leaflet popup innerHTML
    // uses inline onclick="window.__mapToggleStar(...)" strings.
    window.__mapToggleStar = function(id) {
      setStar(id, !getStar(id));
      userData = getAllUserData();
      rerenderMap();
    };
    window.__mapSetStatus = function(id, status) {
      setStatus(id, status);
      userData = getAllUserData();
    };
  }

  // ── Tab switching ──────────────────────────────────────────────────────────
  function switchTab(tab) {
    state.tab = tab;
    document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tab));
    document.querySelectorAll(".tab-panel").forEach(p => {
      p.classList.toggle("active", p.id === `tab-${tab}`);
      p.classList.toggle("hidden", p.id !== `tab-${tab}`);
    });
    if (tab === "map") { rerenderMap(); invalidateMapSize(); }
    if (tab === "starred") renderStarred();
  }

  // ── Rerender ───────────────────────────────────────────────────────────────
  function rerender(keepDisplayCount) {
    userData = getAllUserData();
    state.filteredFirms = applyFilters(state.allFirms, state.filters, userData);
    if (!keepDisplayCount) state.displayCount = PAGE_SIZE;
    const count = state.filteredFirms.length;
    const showing = Math.min(state.displayCount, count);
    document.getElementById("result-count").textContent = `Showing ${showing} of ${count} firms (${state.allFirms.length} total)`;
    document.getElementById("toolbar-count").textContent = `${count} firm${count !== 1 ? "s" : ""}`;
    if (state.view === "cards") renderCards(state.filteredFirms);
    else renderTable(state.filteredFirms);
    if (state.tab === "map") rerenderMap();
    if (state.tab === "starred") renderStarred();
  }

  // ── Card rendering ─────────────────────────────────────────────────────────
  function renderCards(firms) {
    const container = document.getElementById("cards-view");
    if (!firms.length) { container.innerHTML = ""; return; }

    const visible = firms.slice(0, state.displayCount);

    // Compute column count from viewport (matches CSS breakpoints in styles.css)
    const colCount = window.innerWidth >= 900 ? 3 : window.innerWidth >= 600 ? 2 : 1;

    // Find the index of the expanded firm so we know which row it's in
    const expandedIdx = state.expandedCardId
      ? visible.findIndex(f => f.id === state.expandedCardId)
      : -1;

    // The panel inserts after the LAST card in the expanded card's row.
    const rowEndIdx = expandedIdx >= 0
      ? Math.min(visible.length - 1, expandedIdx + (colCount - 1 - expandedIdx % colCount))
      : -1;

    const items = [];
    for (let i = 0; i < visible.length; i++) {
      items.push(cardHTML(visible[i]));
      if (i === rowEndIdx) {
        items.push(detailPanelHTML(visible[expandedIdx]));
      }
    }

    if (state.displayCount < firms.length) {
      items.push(`<div class="load-more-container" style="grid-column:1/-1;text-align:center;padding:1.5rem">
        <button class="btn btn-secondary" id="load-more-btn">Show more (${firms.length - state.displayCount} remaining)</button>
      </div>`);
    }

    container.innerHTML = items.join("");
  }

  function cardHTML(firm) {
    const starred = getStar(firm.id);
    const status = getStatus(firm.id);
    const scoreClass = `match-${firm.referralScore}`;
    const isActive = state.expandedCardId === firm.id;

    return `
    <div class="firm-card${isActive ? " active" : ""}" data-id="${firm.id}">
      <div class="firm-card-header">
        <span class="firm-name">${esc(firm.name)}</span>
        <button class="star-btn${starred ? " starred" : ""}" data-star="${firm.id}">
          ${starred ? "★" : "☆"}
        </button>
      </div>
      <div class="practice-badges">
        ${(firm.practiceAreas || []).map(p => `<span class="badge${firm.referralScore === "competitor" ? " competitor" : ""}">${esc(p)}</span>`).join("")}
      </div>
      <div class="firm-meta">📍 ${esc(firm.address.city)}${firm.phone ? ` · ${esc(firm.phone)}` : ""}${firm.email ? ` · <a href="mailto:${esc(firm.email)}" style="color:var(--accent)" onclick="event.stopPropagation()">${esc(firm.email)}</a>` : ""}</div>
      <div class="firm-summary">${esc(firm.summary || "")}</div>
      <div class="firm-footer">
        <span class="match-badge ${scoreClass}">${firm.referralScore}</span>
        <select class="status-select" data-status="${firm.id}">
          ${["uncontacted","reached_out","partner","not_interested"].map(s =>
            `<option value="${s}"${status === s ? " selected" : ""}>${s.replace(/_/g," ")}</option>`
          ).join("")}
        </select>
        ${firm.website ? `<a class="website-link" href="${esc(firm.website)}" target="_blank" onclick="event.stopPropagation()">Website →</a>` : ""}
      </div>
    </div>`;
  }

  function detailPanelHTML(firm) {
    const note = getNote(firm.id);
    return `
    <div class="card-detail-panel" data-panel-for="${firm.id}">
      <button class="detail-close-btn" data-close-panel="${firm.id}">✕ Close</button>
      <div class="card-expanded-details">
        <div>
          <div class="detail-label">Full Address</div>
          <div class="detail-value">${esc(firm.address.street)}<br>${esc(firm.address.city)}, ${esc(firm.address.state)} ${esc(firm.address.zip)}</div>
        </div>
        <div>
          <div class="detail-label">Email</div>
          <div class="detail-value">${firm.email ? `<a href="mailto:${esc(firm.email)}" style="color:var(--accent)">${esc(firm.email)}</a>` : "—"}</div>
        </div>
        <div style="grid-column:1/-1">
          <div class="detail-label">Summary</div>
          <div class="detail-value">${esc(firm.summary || "—")}</div>
        </div>
        <div style="grid-column:1/-1">
          <div class="detail-label">Notes</div>
          <textarea class="notes-textarea" data-note="${firm.id}" placeholder="Add a note...">${esc(note)}</textarea>
        </div>
      </div>
    </div>`;
  }

  // ── Table rendering ────────────────────────────────────────────────────────
  function renderTable(firms) {
    const sorted = sortFirms([...firms]);
    const visible = sorted.slice(0, state.displayCount);
    const tbody = document.getElementById("table-body");
    let html = visible.map(f => tableRowHTML(f)).join("");
    if (state.displayCount < firms.length) {
      html += `<tr><td colspan="9" style="text-align:center;padding:1rem">
        <button class="btn btn-secondary" id="load-more-btn">Show more (${firms.length - state.displayCount} remaining)</button>
      </td></tr>`;
    }
    tbody.innerHTML = html;
  }

  function tableRowHTML(firm) {
    const starred = getStar(firm.id);
    const status = getStatus(firm.id);
    const scoreClass = `match-${firm.referralScore}`;
    return `
    <tr data-id="${firm.id}">
      <td style="color:var(--text)">${esc(firm.name)}</td>
      <td>${(firm.practiceAreas || []).join(", ")}</td>
      <td>${esc(firm.address.city)}</td>
      <td>${esc(firm.phone || "—")}</td>
      <td>${firm.email ? `<a href="mailto:${esc(firm.email)}" class="website-link">${esc(firm.email)}</a>` : "—"}</td>
      <td>${firm.website ? `<a href="${esc(firm.website)}" target="_blank" class="website-link">Link</a>` : "—"}</td>
      <td><span class="match-badge ${scoreClass}">${firm.referralScore}</span></td>
      <td>
        <select class="status-select" data-status="${firm.id}">
          ${["uncontacted","reached_out","partner","not_interested"].map(s =>
            `<option value="${s}"${status === s ? " selected" : ""}>${s.replace(/_/g," ")}</option>`
          ).join("")}
        </select>
      </td>
      <td><button class="star-btn${starred ? " starred" : ""}" data-star="${firm.id}">${starred ? "★" : "☆"}</button></td>
    </tr>`;
  }

  function sortFirms(firms) {
    const { col, dir } = state.sort;
    const mult = dir === "asc" ? 1 : -1;
    return firms.sort((a, b) => {
      let av, bv;
      if (col === "referralScore") { av = SCORE_ORDER[a.referralScore] || 5; bv = SCORE_ORDER[b.referralScore] || 5; }
      else if (col === "city") { av = a.address.city; bv = b.address.city; }
      else if (col === "practiceAreas") { av = (a.practiceAreas || []).join(","); bv = (b.practiceAreas || []).join(","); }
      else { av = a[col] || ""; bv = b[col] || ""; }
      if (typeof av === "string") return mult * av.localeCompare(bv);
      return mult * (av - bv);
    });
  }

  function updateSortIndicators() {
    document.querySelectorAll("#firms-table th[data-col]").forEach(th => {
      th.classList.remove("sort-asc", "sort-desc");
      if (th.dataset.col === state.sort.col) th.classList.add(`sort-${state.sort.dir}`);
    });
  }

  // ── Map ────────────────────────────────────────────────────────────────────
  function rerenderMap() {
    userData = getAllUserData();
    const unmapped = renderMapPins(state.filteredFirms, userData, null, null);
    const list = document.getElementById("unmapped-list");
    list.innerHTML = unmapped.length
      ? unmapped.map(f => `<div class="unmapped-item">${esc(f.name)} — ${esc(f.address.city)}</div>`).join("")
      : `<div class="unmapped-item" style="color:var(--muted)">All firms mapped</div>`;
  }

  // ── Starred tab ────────────────────────────────────────────────────────────
  function renderStarred() {
    userData = getAllUserData();
    const starred = state.allFirms.filter(f => userData.starred[f.id]);
    const container = document.getElementById("starred-cards");
    const empty = document.getElementById("starred-empty");
    container.innerHTML = starred.map(f => cardHTML(f)).join("");
    empty.classList.toggle("hidden", starred.length > 0);
  }

  // ── View toggle ────────────────────────────────────────────────────────────
  function setView(view) {
    state.view = view;
    document.getElementById("cards-view").classList.toggle("hidden", view !== "cards");
    document.getElementById("table-view").classList.toggle("hidden", view !== "table");
    document.getElementById("view-cards-btn").classList.toggle("active", view === "cards");
    document.getElementById("view-table-btn").classList.toggle("active", view !== "cards");
  }

  // ── Global event delegation ────────────────────────────────────────────────
  document.addEventListener("click", e => {
    // Load more button
    if (e.target.id === "load-more-btn") {
      state.displayCount += PAGE_SIZE;
      rerender(true);
      return;
    }

    const starBtn = e.target.closest("[data-star]");
    if (starBtn) {
      const id = starBtn.dataset.star;
      setStar(id, !getStar(id));
      userData = getAllUserData();
      rerender(true);
      return;
    }
    // Close button on detail panel
    const closeBtn = e.target.closest("[data-close-panel]");
    if (closeBtn) { state.expandedCardId = null; rerender(true); return; }

    // Click card to open/close detail panel
    const card = e.target.closest(".firm-card");
    if (card && !e.target.closest("select, button, a, textarea")) {
      state.expandedCardId = state.expandedCardId === card.dataset.id ? null : card.dataset.id;
      rerender(true);
      return;
    }
  });

  document.addEventListener("change", e => {
    const statusSelect = e.target.closest("[data-status]");
    if (statusSelect) {
      setStatus(statusSelect.dataset.status, e.target.value);
      userData = getAllUserData();
    }
  });

  document.addEventListener("blur", e => {
    const noteArea = e.target.closest("[data-note]");
    if (noteArea) {
      setNote(noteArea.dataset.note, e.target.value);
      userData = getAllUserData();
    }
  }, true);

  // ── Utils ──────────────────────────────────────────────────────────────────
  function esc(str) {
    return String(str || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  }

  function resetFilters() {
    state.filters = {
      search: "", referralScores: ["competitor","high","medium","low"],
      practiceAreas: [], city: "", county: "",
      contactStatuses: ["uncontacted","reached_out","partner","not_interested"],
      starredOnly: false,
      hasWebsite: false,
      hasPracticeArea: false,
      hasContact: false,
    };
    document.getElementById("search").value = "";
    document.querySelectorAll('[name="score"]').forEach(cb => cb.checked = true);
    document.querySelectorAll('[name="practice"]').forEach(cb => cb.checked = false);
    document.getElementById("city-filter").value = "";
    document.getElementById("county-filter").value = "";
    document.querySelectorAll('[name="status"]').forEach(cb => cb.checked = true);
    document.getElementById("starred-only").checked = false;
    document.getElementById("has-website").checked = false;
    document.getElementById("has-practice-area").checked = false;
    document.getElementById("has-contact").checked = false;
    rerender();
  }

  function updateDataInfo() {
    const meta = FIRMS_DATA.meta || {};
    const noteCount = Object.keys(JSON.parse(localStorage.getItem("ks_law_notes") || "{}")).length;
    const starCount = Object.keys(JSON.parse(localStorage.getItem("ks_law_starred") || "{}")).filter(k => JSON.parse(localStorage.getItem("ks_law_starred"))[k]).length;
    const scraped = meta.lastScraped ? new Date(meta.lastScraped).toLocaleDateString() : "Unknown";
    document.getElementById("data-info").innerHTML =
      `Last scraped: ${scraped}<br>Total firms: ${meta.totalFirms || state.allFirms.length}<br>Starred: ${starCount}<br>With notes: ${noteCount}`;
  }

  // ── Start ──────────────────────────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", init);
})();
