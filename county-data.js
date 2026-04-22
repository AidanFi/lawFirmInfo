(function () {
  var grid = document.getElementById('county-grid');
  var emptyState = document.getElementById('empty-state');

  fetch('county-data/manifest.json')
    .then(function (r) { return r.json(); })
    .then(function (data) {
      var counties = data.counties || [];
      if (counties.length === 0) {
        emptyState.classList.remove('hidden');
        return;
      }
      counties.sort(function (a, b) { return a.name.localeCompare(b.name); });
      counties.forEach(function (county) {
        grid.appendChild(createCard(county));
      });
    })
    .catch(function () {
      emptyState.classList.remove('hidden');
    });

  function createCard(county) {
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
