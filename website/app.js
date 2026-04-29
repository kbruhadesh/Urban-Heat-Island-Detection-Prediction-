/* ═══════════════════════════════════════════════════════════════
   UHI Observatory — App Logic
   Loads CSV data, builds Plotly charts, Leaflet map, tables.
   ═══════════════════════════════════════════════════════════════ */

const DATA_BASE = '../data/output/';

const CITY_COORDS = {
  Delhi:     [28.64, 77.10], Mumbai:    [19.08, 72.88],
  Bangalore: [12.98, 77.60], Chennai:   [13.08, 80.24],
  Hyderabad: [17.40, 78.47], Kochi:     [9.97, 76.30],
  Pune:      [18.58, 73.87], Ahmedabad: [23.02, 72.60],
  Kolkata:   [22.60, 88.35], Jaipur:    [26.90, 75.80],
  Surat:     [21.20, 72.83]
};

const COLORS = [
  '#2d8f65','#d4930d','#c0392b','#2980b9','#8e44ad',
  '#16a085','#e07b3a','#27ae60','#e74c3c','#3498db','#f39c12'
];

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

const PLOTLY_LAYOUT = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { family: 'Inter, sans-serif', color: '#3a5a4a', size: 12 },
  margin: { l: 50, r: 30, t: 40, b: 50 },
  legend: { bgcolor: 'rgba(0,0,0,0)', bordercolor: 'rgba(45,143,101,0.15)', borderwidth: 1 },
  xaxis: { gridcolor: 'rgba(45,143,101,0.08)', zerolinecolor: 'rgba(45,143,101,0.15)' },
  yaxis: { gridcolor: 'rgba(45,143,101,0.08)', zerolinecolor: 'rgba(45,143,101,0.15)' },
};

const PLOTLY_CFG = { responsive: true, displayModeBar: false };

/* ── CSV Loader ──────────────────────────────────────────────── */
function loadCSV(filename) {
  return new Promise((resolve, reject) => {
    Papa.parse(DATA_BASE + filename, {
      download: true, header: true, dynamicTyping: true, skipEmptyLines: true,
      complete: r => resolve(r.data),
      error: e => { console.warn(`Failed: ${filename}`, e); resolve([]); }
    });
  });
}

/* ── Init ─────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', async () => {
  // Navbar scroll effect
  window.addEventListener('scroll', () => {
    document.getElementById('navbar').classList.toggle('scrolled', window.scrollY > 30);
    updateActiveNav();
  });

  // Tab switching
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const tabGroup = btn.closest('.section');
      tabGroup.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      tabGroup.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById('panel-' + btn.dataset.tab).classList.add('active');
    });
  });

  // Load all data
  const [rankings, roc, yearly, movingAvg, decade, seasonal, monthly,
         peakMonths, forecast, testPred, hotspots, extremeHeat, allClean
  ] = await Promise.all([
    loadCSV('city_rankings.csv'),
    loadCSV('uhi_rate_of_change.csv'),
    loadCSV('uhi_yearly.csv'),
    loadCSV('uhi_moving_avg.csv'),
    loadCSV('uhi_decade.csv'),
    loadCSV('uhi_seasonal.csv'),
    loadCSV('uhi_monthly.csv'),
    loadCSV('peak_months.csv'),
    loadCSV('uhi_forecast_2025_2030.csv'),
    loadCSV('test_predictions.csv'),
    loadCSV('hotspot_alerts.csv'),
    loadCSV('extreme_heat_events.csv'),
    loadCSV('uhi_all_clean.csv'),
  ]);

  // Build everything
  buildHeroStats(rankings, allClean);
  buildKPIs(rankings);
  buildRankingsChart(rankings);
  buildROCChart(roc);
  buildYearlyChart(yearly);
  buildMovingAvgChart(movingAvg);
  buildDecadeChart(decade);
  buildSeasonalChart(seasonal);
  buildMap(rankings);
  buildHeatmap(monthly);
  buildComparisonCharts(rankings);
  buildPeakMonthsTable(peakMonths);
  buildForecast(yearly, forecast, testPred);
  buildAlerts(hotspots, extremeHeat);
  buildPolicy(rankings);
});

/* ── Hero Stats ──────────────────────────────────────────────── */
function buildHeroStats(rankings, allClean) {
  document.getElementById('stat-records').textContent = allClean.length.toLocaleString();
  const hottest = rankings.reduce((a, b) => a.avg_uhi > b.avg_uhi ? a : b);
  document.getElementById('stat-hottest').innerHTML = hottest.city;
}

/* ── KPI Cards ───────────────────────────────────────────────── */
function buildKPIs(rankings) {
  const grid = document.getElementById('kpi-grid');
  const sorted = [...rankings].sort((a, b) => b.avg_uhi - a.avg_uhi);
  const avgAll = (sorted.reduce((s, r) => s + r.avg_uhi, 0) / sorted.length).toFixed(2);
  const avgUrban = (sorted.reduce((s, r) => s + r.avg_urban_temp, 0) / sorted.length).toFixed(1);

  const kpis = [
    { label: 'Highest UHI City', value: sorted[0].city, delta: `+${sorted[0].avg_uhi.toFixed(2)}°C`, cls: 'up' },
    { label: 'Lowest UHI City', value: sorted[sorted.length-1].city, delta: `${sorted[sorted.length-1].avg_uhi.toFixed(2)}°C`, cls: 'down' },
    { label: 'Avg UHI (All Cities)', value: `${avgAll}°C`, delta: '', cls: '' },
    { label: 'Avg Urban Temperature', value: `${avgUrban}°C`, delta: '', cls: '' },
  ];

  grid.innerHTML = kpis.map(k => `
    <div class="card animate-in">
      <div class="card-title">${k.label}</div>
      <div class="card-value">${k.value}</div>
      ${k.delta ? `<div class="card-delta ${k.cls}">${k.delta}</div>` : ''}
    </div>
  `).join('');
}

/* ── Rankings Bar Chart ──────────────────────────────────────── */
function buildRankingsChart(rankings) {
  const sorted = [...rankings].sort((a, b) => b.avg_uhi - a.avg_uhi);
  const colors = sorted.map(r => r.avg_uhi > 0.3 ? '#c0392b' : (r.avg_uhi > 0 ? '#d4930d' : '#2980b9'));

  Plotly.newPlot('chart-rankings', [{
    x: sorted.map(r => r.city), y: sorted.map(r => r.avg_uhi),
    type: 'bar', marker: { color: colors, cornerradius: 6 },
    hovertemplate: '<b>%{x}</b><br>UHI: %{y:.3f}°C<extra></extra>'
  }], {
    ...PLOTLY_LAYOUT,
    title: { text: 'Average UHI Index by City (2000–2024)', font: { size: 14, color: '#1a2e23' } },
    yaxis: { ...PLOTLY_LAYOUT.yaxis, title: 'Avg UHI Index (°C)' },
  }, PLOTLY_CFG);
}

/* ── Rate of Change Chart ────────────────────────────────────── */
function buildROCChart(roc) {
  const sorted = [...roc].sort((a, b) => b.rate_per_decade_c - a.rate_per_decade_c);
  const colors = sorted.map(r =>
    r.trend === 'Warming' ? '#c0392b' : (r.trend === 'Cooling' ? '#2980b9' : '#6b8f7a')
  );

  Plotly.newPlot('chart-roc', [{
    x: sorted.map(r => r.city), y: sorted.map(r => r.rate_per_decade_c),
    type: 'bar', marker: { color: colors, cornerradius: 6 },
    hovertemplate: '<b>%{x}</b><br>Rate: %{y:.4f}°C/decade<extra></extra>'
  }], {
    ...PLOTLY_LAYOUT,
    yaxis: { ...PLOTLY_LAYOUT.yaxis, title: 'Rate (°C per decade)' },
  }, PLOTLY_CFG);
}

/* ── Yearly Trends ───────────────────────────────────────────── */
function buildYearlyChart(yearly) {
  const cities = [...new Set(yearly.map(r => r.city))];
  const traces = cities.map((city, i) => {
    const d = yearly.filter(r => r.city === city).sort((a, b) => a.year - b.year);
    return {
      x: d.map(r => r.year), y: d.map(r => r.avg_uhi),
      name: city, type: 'scatter', mode: 'lines',
      line: { color: COLORS[i], width: 2 },
      hovertemplate: `<b>${city}</b> %{x}<br>UHI: %{y:.3f}°C<extra></extra>`
    };
  });

  Plotly.newPlot('chart-yearly', traces, {
    ...PLOTLY_LAYOUT,
    title: { text: 'Yearly UHI Index Trends', font: { size: 14, color: '#1a2e23' } },
    yaxis: { ...PLOTLY_LAYOUT.yaxis, title: 'Avg UHI (°C)' },
    xaxis: { ...PLOTLY_LAYOUT.xaxis, title: 'Year' },
  }, PLOTLY_CFG);
}

/* ── Moving Average ──────────────────────────────────────────── */
function buildMovingAvgChart(data) {
  const cities = [...new Set(data.map(r => r.city))];
  const traces = cities.map((city, i) => {
    const d = data.filter(r => r.city === city).sort((a, b) => a.year - b.year);
    return {
      x: d.map(r => r.year), y: d.map(r => r.moving_avg_5yr),
      name: city, type: 'scatter', mode: 'lines',
      line: { color: COLORS[i], width: 2.5 },
    };
  });

  Plotly.newPlot('chart-moving-avg', traces, {
    ...PLOTLY_LAYOUT,
    title: { text: 'Smoothed UHI (5-Year Moving Average)', font: { size: 14, color: '#1a2e23' } },
    yaxis: { ...PLOTLY_LAYOUT.yaxis, title: '5yr Moving Avg UHI (°C)' },
  }, PLOTLY_CFG);
}

/* ── Decade Comparison ───────────────────────────────────────── */
function buildDecadeChart(decade) {
  const decades = [...new Set(decade.map(r => r.decade))].sort();
  const decColors = ['#2980b9', '#d4930d', '#c0392b'];
  const cities = [...new Set(decade.map(r => r.city))].sort();

  const traces = decades.map((dec, i) => {
    const d = decade.filter(r => r.decade === dec);
    const cityMap = {};
    d.forEach(r => cityMap[r.city] = r.avg_uhi);
    return {
      x: cities, y: cities.map(c => cityMap[c] || 0),
      name: dec, type: 'bar', marker: { color: decColors[i], cornerradius: 4 },
    };
  });

  Plotly.newPlot('chart-decade', traces, {
    ...PLOTLY_LAYOUT, barmode: 'group',
    title: { text: 'UHI by Decade', font: { size: 14, color: '#1a2e23' } },
    yaxis: { ...PLOTLY_LAYOUT.yaxis, title: 'Avg UHI (°C)' },
  }, PLOTLY_CFG);
}

/* ── Seasonal Chart ──────────────────────────────────────────── */
function buildSeasonalChart(seasonal) {
  const seasons = ['Pre-Monsoon', 'Monsoon', 'Post-Monsoon', 'Winter'];
  const cities = [...new Set(seasonal.map(r => r.city))].sort();

  const traces = cities.map((city, i) => {
    const d = seasonal.filter(r => r.city === city);
    const sMap = {};
    d.forEach(r => sMap[r.season] = r.avg_uhi);
    return {
      x: seasons, y: seasons.map(s => sMap[s] || 0),
      name: city, type: 'bar', marker: { color: COLORS[i] },
    };
  });

  Plotly.newPlot('chart-seasonal', traces, {
    ...PLOTLY_LAYOUT, barmode: 'group',
    title: { text: 'Seasonal UHI Patterns', font: { size: 14, color: '#1a2e23' } },
    yaxis: { ...PLOTLY_LAYOUT.yaxis, title: 'Avg UHI (°C)' },
  }, PLOTLY_CFG);
}

/* ── Leaflet Map ─────────────────────────────────────────────── */
function buildMap(rankings) {
  const map = L.map('map-container').setView([22.5, 78.5], 5);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    attribution: '© OpenStreetMap © CARTO', maxZoom: 18
  }).addTo(map);

  rankings.forEach(row => {
    const coords = CITY_COORDS[row.city];
    if (!coords) return;
    const uhi = row.avg_uhi;
    const color = uhi > 0.3 ? '#c0392b' : (uhi > 0 ? '#d4930d' : '#2980b9');
    const radius = Math.max(Math.abs(uhi) * 14, 6);

    L.circleMarker(coords, {
      radius, color, fillColor: color, fillOpacity: 0.6, weight: 2
    }).addTo(map).bindPopup(`
      <div style="font-family:Inter,sans-serif;font-size:13px;">
        <b style="font-size:15px;">${row.city}</b><br>
        <span style="color:${color};font-weight:700;">UHI: ${uhi.toFixed(2)}°C</span><br>
        Urban Temp: ${row.avg_urban_temp.toFixed(1)}°C<br>
        Peak UHI: ${row.peak_uhi.toFixed(2)}°C
      </div>
    `).bindTooltip(`${row.city}: ${uhi.toFixed(2)}°C`, { direction: 'top' });
  });
}

/* ── Monthly Heatmap ─────────────────────────────────────────── */
function buildHeatmap(monthly) {
  const cities = [...new Set(monthly.map(r => r.city))].sort();
  const z = cities.map(city => {
    const row = [];
    for (let m = 1; m <= 12; m++) {
      const vals = monthly.filter(r => r.city === city && r.month === m);
      const avg = vals.length ? vals.reduce((s, r) => s + r.avg_uhi, 0) / vals.length : 0;
      row.push(+avg.toFixed(3));
    }
    return row;
  });

  Plotly.newPlot('chart-heatmap', [{
    z, x: MONTHS, y: cities, type: 'heatmap',
    colorscale: [[0,'#2980b9'],[0.35,'#5dade2'],[0.5,'#f7dc6f'],[0.7,'#e07b3a'],[1,'#c0392b']],
    hovertemplate: '<b>%{y}</b> — %{x}<br>UHI: %{z:.3f}°C<extra></extra>',
    colorbar: { title: 'UHI °C', titleside: 'right' }
  }], {
    ...PLOTLY_LAYOUT,
    yaxis: { ...PLOTLY_LAYOUT.yaxis, autorange: 'reversed' },
  }, PLOTLY_CFG);
}

/* ── Comparison Charts ───────────────────────────────────────── */
function buildComparisonCharts(rankings) {
  const sorted = [...rankings].sort((a, b) => b.avg_urban_temp - a.avg_urban_temp);

  Plotly.newPlot('chart-urban-temp', [{
    x: sorted.map(r => r.city), y: sorted.map(r => r.avg_urban_temp),
    type: 'bar', marker: { color: '#e07b3a', cornerradius: 6 },
    hovertemplate: '<b>%{x}</b><br>%{y:.1f}°C<extra></extra>'
  }], { ...PLOTLY_LAYOUT, yaxis: { ...PLOTLY_LAYOUT.yaxis, title: '°C' } }, PLOTLY_CFG);

  Plotly.newPlot('chart-night-temp', [{
    x: sorted.map(r => r.city), y: sorted.map(r => r.avg_night_temp),
    type: 'bar', marker: { color: '#2980b9', cornerradius: 6 },
    hovertemplate: '<b>%{x}</b><br>%{y:.1f}°C<extra></extra>'
  }], { ...PLOTLY_LAYOUT, yaxis: { ...PLOTLY_LAYOUT.yaxis, title: '°C' } }, PLOTLY_CFG);
}

/* ── Peak Months Table ───────────────────────────────────────── */
function buildPeakMonthsTable(peakMonths) {
  const sorted = [...peakMonths].sort((a, b) => b.peak_uhi - a.peak_uhi);
  document.getElementById('peak-months-table').innerHTML = `
    <table>
      <thead><tr><th>City</th><th>Peak Month</th><th>Peak UHI (°C)</th></tr></thead>
      <tbody>
        ${sorted.map(r => `<tr>
          <td><b>${r.city}</b></td>
          <td>${MONTHS[r.peak_month - 1] || r.peak_month}</td>
          <td style="font-weight:600;color:${r.peak_uhi > 0 ? '#c0392b' : '#2980b9'}">${r.peak_uhi.toFixed(4)}</td>
        </tr>`).join('')}
      </tbody>
    </table>
  `;
}

/* ── Forecast ────────────────────────────────────────────────── */
function buildForecast(yearly, forecast, testPred) {
  const cities = [...new Set(yearly.map(r => r.city))].sort();
  let selectedCity = cities[0] || 'Delhi';

  const selector = document.getElementById('forecast-city-selector');
  selector.innerHTML = cities.map(c =>
    `<button class="city-chip ${c === selectedCity ? 'active' : ''}" data-city="${c}">${c}</button>`
  ).join('');

  function renderForecast(city) {
    const hist = yearly.filter(r => r.city === city).sort((a, b) => a.year - b.year);
    const fore = forecast.filter(r => r.city === city);
    const foreYearly = {};
    fore.forEach(r => {
      if (!foreYearly[r.year]) foreYearly[r.year] = { sum: 0, count: 0 };
      foreYearly[r.year].sum += r.predicted_uhi;
      foreYearly[r.year].count++;
    });
    const foreData = Object.entries(foreYearly)
      .map(([yr, v]) => ({ year: +yr, uhi: v.sum / v.count }))
      .sort((a, b) => a.year - b.year);

    Plotly.newPlot('chart-forecast', [
      {
        x: hist.map(r => r.year), y: hist.map(r => r.avg_uhi),
        name: 'Historical', type: 'scatter', mode: 'lines+markers',
        line: { color: '#2d8f65', width: 2.5 },
        marker: { size: 4 },
      },
      {
        x: foreData.map(r => r.year), y: foreData.map(r => r.uhi),
        name: 'Forecast', type: 'scatter', mode: 'lines+markers',
        line: { color: '#8e44ad', width: 3, dash: 'dash' },
        marker: { size: 6 },
      }
    ], {
      ...PLOTLY_LAYOUT,
      title: { text: `${city} — Historical vs Forecast UHI`, font: { size: 14, color: '#1a2e23' } },
      yaxis: { ...PLOTLY_LAYOUT.yaxis, title: 'UHI Index (°C)' },
      xaxis: { ...PLOTLY_LAYOUT.xaxis, title: 'Year' },
    }, PLOTLY_CFG);
  }

  renderForecast(selectedCity);

  selector.addEventListener('click', e => {
    if (e.target.classList.contains('city-chip')) {
      selector.querySelectorAll('.city-chip').forEach(c => c.classList.remove('active'));
      e.target.classList.add('active');
      renderForecast(e.target.dataset.city);
    }
  });

  // Model metrics
  if (testPred.length) {
    const errors = testPred.map(r => Math.abs(r.actual_uhi - r.predicted_uhi));
    const mae = (errors.reduce((s, e) => s + e, 0) / errors.length).toFixed(4);
    const maxErr = Math.max(...errors).toFixed(4);
    document.getElementById('model-metrics').innerHTML = `
      <div class="card"><div class="card-title">Model MAE</div><div class="card-value neutral">${mae}°C</div></div>
      <div class="card"><div class="card-title">Max Error</div><div class="card-value">${maxErr}°C</div></div>
      <div class="card"><div class="card-title">Test Records</div><div class="card-value">${testPred.length}</div></div>
      <div class="card"><div class="card-title">Forecast Range</div><div class="card-value">2025–30</div></div>
    `;
  }
}

/* ── Alerts ───────────────────────────────────────────────────── */
function buildAlerts(hotspots, extremeHeat) {
  const critical = hotspots.filter(r => r.severity === 'CRITICAL');
  const high = hotspots.filter(r => r.severity === 'HIGH');

  document.getElementById('alert-kpis').innerHTML = `
    <div class="card"><div class="card-title">🔴 Critical Events</div><div class="card-value positive">${critical.length}</div></div>
    <div class="card"><div class="card-title">🟠 High Events</div><div class="card-value">${high.length}</div></div>
    <div class="card"><div class="card-title">📊 Total Hotspots</div><div class="card-value">${hotspots.length}</div></div>
    <div class="card"><div class="card-title">🌡️ Extreme Heat Days</div><div class="card-value">${extremeHeat.length}</div></div>
  `;

  // Hotspot table (first 50)
  const rows = hotspots.slice(0, 50);
  document.getElementById('hotspot-table').innerHTML = `
    <table>
      <thead><tr><th>City</th><th>Date</th><th>Urban °C</th><th>UHI Index</th><th>Severity</th></tr></thead>
      <tbody>
        ${rows.map(r => `<tr>
          <td><b>${r.city}</b></td><td>${r.date}</td>
          <td>${(+r.urban_temp_c).toFixed(1)}</td>
          <td style="font-weight:600">${(+r.uhi_index).toFixed(3)}</td>
          <td><span class="badge badge-${r.severity.toLowerCase()}">${r.severity}</span></td>
        </tr>`).join('')}
      </tbody>
    </table>
  `;

  // Extreme heat table (first 30)
  document.getElementById('extreme-table').innerHTML = `
    <table>
      <thead><tr><th>City</th><th>Date</th><th>Urban Temp (°C)</th><th>UHI Index</th></tr></thead>
      <tbody>
        ${extremeHeat.slice(0, 30).map(r => `<tr>
          <td><b>${r.city}</b></td><td>${r.date}</td>
          <td style="color:#c0392b;font-weight:700;">${(+r.urban_temp_c).toFixed(1)}</td>
          <td>${(+r.uhi_index).toFixed(3)}</td>
        </tr>`).join('')}
      </tbody>
    </table>
  `;
}

/* ── Policy Recommendations ──────────────────────────────────── */
function buildPolicy(rankings) {
  const sorted = [...rankings].sort((a, b) => b.avg_uhi - a.avg_uhi);
  const container = document.getElementById('policy-cards');

  container.innerHTML = sorted.map(row => {
    const uhi = row.avg_uhi;
    let cls, label, msg, actions;

    if (uhi > 0.3) {
      cls = 'high-priority'; label = '🔴 HIGH PRIORITY';
      msg = `${row.city} shows significant urban heating (+${uhi.toFixed(2)}°C above rural areas).`;
      actions = [
        '🌳 Increase urban green cover by 15-20% in core areas',
        '🏗️ Mandate cool roof coatings on new constructions',
        '💧 Implement permeable pavements in commercial zones',
        '🌊 Develop urban water bodies and mist cooling systems',
        '🚨 Establish heat action plans for peak months',
      ];
    } else if (uhi > 0) {
      cls = 'moderate'; label = '🟡 MODERATE';
      msg = `${row.city} has mild urban heating (+${uhi.toFixed(2)}°C).`;
      actions = [
        '🌳 Maintain existing green cover and expand urban parks',
        '🏗️ Encourage green building certifications',
        '📊 Continue monitoring with annual assessments',
      ];
    } else {
      cls = 'low-risk'; label = '🔵 LOW RISK';
      msg = `${row.city} is cooler than surroundings (${uhi.toFixed(2)}°C). Likely influenced by coast or green cover.`;
      actions = [
        '✅ Preserve existing green and blue infrastructure',
        '📊 Monitor for future changes as city expands',
        '🌿 Document best practices for other cities',
      ];
    }

    return `
      <div class="policy-card ${cls} animate-in">
        <h4>${row.city} — ${label}</h4>
        <p style="margin-bottom:8px;color:var(--text-body);">${msg}</p>
        <ul>${actions.map(a => `<li>${a}</li>`).join('')}</ul>
        ${row.peak_uhi > 5 ? `<p style="margin-top:10px;color:#c0392b;font-weight:600;">⚠️ Peak UHI reached ${row.peak_uhi.toFixed(2)}°C — extreme event preparedness recommended</p>` : ''}
      </div>
    `;
  }).join('');
}

/* ── Active Nav Link ─────────────────────────────────────────── */
function updateActiveNav() {
  const sections = document.querySelectorAll('section[id], header[id]');
  const links = document.querySelectorAll('.nav-links a');
  let current = '';

  sections.forEach(sec => {
    if (window.scrollY >= sec.offsetTop - 100) current = sec.id;
  });

  links.forEach(link => {
    link.classList.remove('active');
    if (link.getAttribute('href') === '#' + current) link.classList.add('active');
  });
}
