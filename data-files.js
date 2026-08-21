(function () {
  var STATE_LABELS = { KS: 'Kansas', MO: 'Missouri', OK: 'Oklahoma' };
  var CATEGORIES = [
    { key: 'firms', label: 'Law Firm Data' },
    { key: 'providers', label: 'Providers Data' },
    { key: 'insurance', label: 'Insurance Data' }
  ];

  var stateTabsEl = document.getElementById('state-tabs');
  var statePanelsEl = document.getElementById('state-panels');
  var loadError = document.getElementById('load-error');

  Promise.all([
    fetch('county-data/manifest.json').then(function (r) { return r.json(); }),
    fetch('county-data/providers-manifest.json').then(function (r) { return r.json(); })
  ]).then(function (results) {
    render(results[0].counties || [], results[1].providers || []);
  }).catch(function () {
    loadError.classList.remove('hidden');
  });

  function render(counties, providers) {
    var states = {};
    counties.forEach(function (c) {
      (states[c.state] = states[c.state] || { firms: [], providers: [] }).firms.push(c);
    });
    providers.forEach(function (p) {
      (states[p.state] = states[p.state] || { firms: [], providers: [] }).providers.push(p);
    });

    var stateCodes = Object.keys(states).sort();
    if (stateCodes.length === 0) {
      loadError.classList.remove('hidden');
      return;
    }

    stateCodes.forEach(function (state, i) {
      var isActiveState = i === 0;
      stateTabsEl.appendChild(createStateTabButton(state, isActiveState));
      statePanelsEl.appendChild(createStatePanel(state, states[state], isActiveState));
    });

    bindEvents();
  }

  function createStateTabButton(state, active) {
    var btn = document.createElement('button');
    btn.className = 'tab state-tab' + (active ? ' active' : '');
    btn.dataset.state = state;
    btn.textContent = STATE_LABELS[state] || state;
    return btn;
  }

  function createStatePanel(state, data, active) {
    var panel = document.createElement('div');
    panel.id = 'state-panel-' + state;
    panel.className = 'state-panel' + (active ? '' : ' hidden');

    var catTabs = document.createElement('nav');
    catTabs.className = 'category-tabs';
    CATEGORIES.forEach(function (cat, i) {
      var btn = document.createElement('button');
      btn.className = 'tab category-tab' + (i === 0 ? ' active' : '');
      btn.dataset.state = state;
      btn.dataset.category = cat.key;
      btn.textContent = cat.label;
      catTabs.appendChild(btn);
    });
    panel.appendChild(catTabs);

    CATEGORIES.forEach(function (cat, i) {
      var catPanel = document.createElement('div');
      catPanel.id = 'cat-panel-' + state + '-' + cat.key;
      catPanel.className = 'category-panel' + (i === 0 ? '' : ' hidden');
      catPanel.appendChild(renderCategoryContent(cat.key, data));
      panel.appendChild(catPanel);
    });

    return panel;
  }

  function renderCategoryContent(key, data) {
    if (key === 'firms') return renderFirmGrid(data.firms);
    if (key === 'providers') return renderProviderGrid(data.providers);
    return renderInsurancePlaceholder();
  }

  function renderFirmGrid(entries) {
    if (!entries.length) {
      return renderEmptyState('No law firm data yet', 'Run the county pipeline to generate data files for this state.');
    }
    var sorted = entries.slice().sort(function (a, b) { return a.name.localeCompare(b.name); });
    var grid = document.createElement('div');
    grid.className = 'county-grid';
    sorted.forEach(function (county) { grid.appendChild(createFirmCard(county)); });
    return grid;
  }

  function renderProviderGrid(entries) {
    if (!entries.length) {
      return renderEmptyState('No provider data yet', 'Chiropractor & physical therapist data has not been collected for this state yet.');
    }
    var sorted = entries.slice().sort(function (a, b) { return a.name.localeCompare(b.name); });
    var totalChiro = sorted.reduce(function (sum, p) { return sum + p.chiro; }, 0);
    var totalPt = sorted.reduce(function (sum, p) { return sum + p.pt; }, 0);

    var wrap = document.createElement('div');

    var summary = document.createElement('div');
    summary.className = 'summary-bar';
    summary.innerHTML =
      '<div><strong>' + sorted.length + '</strong> counties</div>' +
      '<div><strong>' + totalChiro.toLocaleString() + '</strong> chiropractors</div>' +
      '<div><strong>' + totalPt.toLocaleString() + '</strong> physical therapists</div>';
    wrap.appendChild(summary);

    var grid = document.createElement('div');
    grid.className = 'county-grid';
    sorted.forEach(function (p) { grid.appendChild(createProviderCard(p)); });
    wrap.appendChild(grid);

    return wrap;
  }

  function renderInsurancePlaceholder() {
    return renderEmptyState('Insurance data coming soon', 'This category has not been populated yet. Check back once the insurance data pipeline is built.');
  }

  function renderEmptyState(title, message) {
    var div = document.createElement('div');
    div.className = 'empty-state';
    div.innerHTML = '<h2>' + escapeHtml(title) + '</h2><p>' + escapeHtml(message) + '</p>';
    return div;
  }

  function createFirmCard(county) {
    var card = document.createElement('div');
    card.className = 'county-card';
    card.innerHTML =
      '<div class="county-card-name">' + escapeHtml(county.name) + ', ' + escapeHtml(county.state) + '</div>' +
      '<div class="county-card-meta">' +
        '<span>' + county.firm_count + ' firms</span>' +
        '<span>Updated ' + formatDate(county.last_updated) + '</span>' +
      '</div>' +
      '<a href="county-data/' + encodeURIComponent(county.csv_file) + '" download class="download-btn">Download CSV</a>';
    return card;
  }

  function createProviderCard(p) {
    var card = document.createElement('div');
    card.className = 'county-card';
    card.innerHTML =
      '<div class="county-card-name">' + escapeHtml(p.name) + '</div>' +
      '<div class="county-card-state">' + escapeHtml(p.state) + '</div>' +
      '<div class="county-card-breakdown">' +
        '<span class="breakdown-item"><strong>' + p.chiro + '</strong> Chiropractors</span>' +
        '<span class="breakdown-item"><strong>' + p.pt + '</strong> Physical Therapists</span>' +
      '</div>' +
      '<a href="county-data/' + encodeURIComponent(p.csv_file) + '" download class="download-btn">Download CSV</a>';
    return card;
  }

  function bindEvents() {
    stateTabsEl.querySelectorAll('.state-tab').forEach(function (btn) {
      btn.addEventListener('click', function () { switchState(btn.dataset.state); });
    });
    statePanelsEl.querySelectorAll('.category-tab').forEach(function (btn) {
      btn.addEventListener('click', function () { switchCategory(btn.dataset.state, btn.dataset.category); });
    });
  }

  function switchState(state) {
    stateTabsEl.querySelectorAll('.state-tab').forEach(function (btn) {
      btn.classList.toggle('active', btn.dataset.state === state);
    });
    statePanelsEl.querySelectorAll('.state-panel').forEach(function (panel) {
      panel.classList.toggle('hidden', panel.id !== 'state-panel-' + state);
    });
    switchCategory(state, 'firms');
  }

  function switchCategory(state, category) {
    var panel = document.getElementById('state-panel-' + state);
    if (!panel) return;
    panel.querySelectorAll('.category-tab').forEach(function (btn) {
      btn.classList.toggle('active', btn.dataset.category === category);
    });
    panel.querySelectorAll('.category-panel').forEach(function (catPanel) {
      catPanel.classList.toggle('hidden', catPanel.id !== 'cat-panel-' + state + '-' + category);
    });
  }

  function formatDate(iso) {
    if (!iso) return 'N/A';
    var d = new Date(iso + 'T00:00:00');
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  }

  function escapeHtml(str) {
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
  }
})();
