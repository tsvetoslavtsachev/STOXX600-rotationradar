// STOXX 600 Rotation Radar — UI rendering logic.
// Чете data.json и рендерира всички views.

// Държи текущите данни за всеки tab — за Excel export-а.
// За tabs с filters (rank-all, screener), стойността е CURRENT FILTERED set.
const exportState = {
  asOf: null,
  "stable-winners-1m": [],
  "stable-winners-3m": [],
  "quality-dip-1m": [],
  "quality-dip-3m": [],
  "faded-bounces": [],
  "current-strength": [],
  "rank-all": [],
  "screener": [],
};

(async () => {
  const data = await fetchData();
  if (!data) return;

  exportState.asOf = data.metadata?.as_of || "unknown";

  renderMetadata(data.metadata);
  renderWatchlist("stable-winners-1m", data.stable_winners_1m, "1m");
  renderWatchlist("stable-winners-3m", data.stable_winners_3m, "3m");
  renderWatchlist("quality-dip-1m", data.quality_dip_1m, "1m");
  renderWatchlist("quality-dip-3m", data.quality_dip_3m, "3m");
  renderWatchlist("faded-bounces", data.faded_bounces_1m, "1m");
  renderCurrentStrength("current-strength", data.current_strength);
  renderRankAll("rank-all", data.rank_all_stocks);
  renderScreener("screener", data.screener);
  renderHeatmap("sectors", data.sector_rotation);
  // Sub-Industry tab е премахнат за STOXX 600 — iShares CSV няма sub-industry.
  setupTabs();
  setupExportButtons();
})();

async function fetchData() {
  try {
    const res = await fetch("data.json");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    document.querySelector("main").innerHTML =
      `<div class="empty-state">⚠ data.json не може да се зареди.<br><small>${err.message}</small></div>`;
    return null;
  }
}

function renderMetadata(meta) {
  const host = document.getElementById("metadata");
  host.innerHTML = `
    <span>📅 As of: <strong>${meta.as_of}</strong></span>
    <span>📊 Universe: <strong>${meta.total_universe}</strong></span>
    <span>📚 History: ${meta.history_start} → ${meta.history_end}</span>
  `;
}

function renderWatchlist(viewId, rows, deltaWindow) {
  const host = document.querySelector(`#${viewId} .table-host`);
  if (Object.prototype.hasOwnProperty.call(exportState, viewId)) {
    exportState[viewId] = rows || [];
  }
  if (!rows || rows.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма kandidaти в този quadrant сега.</div>`;
    return;
  }

  const showBoth = deltaWindow === "both";
  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "current_rank", label: "Sector Rank" },
    { key: "abs_strength", label: "Abs %ile" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
    { key: "base_rank_6m", label: "Base (6m)" },
  ];
  if (showBoth) {
    headers.push({ key: "delta_1m", label: "Δ 1m" });
    headers.push({ key: "delta_3m", label: "Δ 3m" });
  } else {
    headers.push({ key: `delta_${deltaWindow}`, label: `Δ ${deltaWindow}` });
  }
  headers.push({ key: "trajectory", label: "Rank Path (90d)" });

  const table = document.createElement("table");
  table.appendChild(buildThead(headers));
  table.appendChild(buildTbody(rows, headers));
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function buildThead(headers) {
  const thead = document.createElement("thead");
  const tr = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    tr.appendChild(th);
  });
  thead.appendChild(tr);
  return thead;
}

function buildTbody(rows, headers) {
  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      if (h.key === "ticker") {
        td.innerHTML = `<a class="ticker" href="https://finance.yahoo.com/quote/${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
      } else if (h.key === "trajectory") {
        td.appendChild(makeTrajectorySVG(row.trajectory));
      } else if (h.key.startsWith("delta_")) {
        const v = row[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1);
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "current_rank" || h.key === "base_rank_6m" || h.key === "abs_strength") {
        const v = row[h.key];
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
      } else if (h.key === "mom_12_1_pct") {
        const v = row[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1) + "%";
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else {
        td.textContent = row[h.key] ?? "—";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  return tbody;
}

function makeTrajectorySVG(points) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("class", "trajectory");
  svg.setAttribute("viewBox", "0 0 100 24");
  svg.setAttribute("preserveAspectRatio", "none");

  if (!points || points.length < 2) {
    return svg;
  }

  const ranks = points.map((p) => p.rank).filter((r) => r !== null && r !== undefined);
  if (ranks.length < 2) return svg;

  const w = 100, h = 24;
  const xStep = w / (points.length - 1);
  const path = points
    .map((p, i) => {
      const x = i * xStep;
      const y = h - ((p.rank ?? 50) / 100) * h;
      return (i === 0 ? "M" : "L") + x.toFixed(1) + "," + y.toFixed(1);
    })
    .join(" ");

  const last = ranks[ranks.length - 1];
  const first = ranks[0];
  const stroke = last > first ? "var(--riser)" : "var(--decayer)";

  const pathEl = document.createElementNS("http://www.w3.org/2000/svg", "path");
  pathEl.setAttribute("d", path);
  pathEl.setAttribute("stroke", stroke);
  pathEl.setAttribute("stroke-width", "1.5");
  pathEl.setAttribute("fill", "none");
  svg.appendChild(pathEl);

  // Hover tooltip — изяснява точно какво показва линията
  const titleEl = document.createElementNS("http://www.w3.org/2000/svg", "title");
  const startDate = points[0]?.date ?? "";
  const endDate = points[points.length - 1]?.date ?? "";
  titleEl.textContent =
    `Sector Rank trajectory: ${first.toFixed(1)} → ${last.toFixed(1)} ` +
    `(${startDate} → ${endDate}, ${points.length} търговски дни)`;
  svg.appendChild(titleEl);

  return svg;
}

function renderCurrentStrength(viewId, rows) {
  const host = document.querySelector(`#${viewId} .table-host`);
  exportState["current-strength"] = rows || [];
  if (!rows || rows.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма данни за Current Strength.</div>`;
    return;
  }

  const headers = [
    { key: "rank_index", label: "#" },
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
    { key: "abs_strength", label: "Abs %ile" },
    { key: "current_rank", label: "Sector Rank" },
    { key: "trajectory", label: "Rank Path (90d)" },
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row, idx) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      if (h.key === "rank_index") {
        td.textContent = idx + 1;
        td.dataset.value = idx + 1;
      } else if (h.key === "ticker") {
        td.innerHTML = `<a class="ticker" href="https://finance.yahoo.com/quote/${row.ticker}" target="_blank" rel="noopener">${row.ticker}</a>`;
      } else if (h.key === "trajectory") {
        td.appendChild(makeTrajectorySVG(row.trajectory));
      } else if (h.key === "mom_12_1_pct") {
        const v = row[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(1) + "%";
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else if (h.key === "abs_strength" || h.key === "current_rank") {
        const v = row[h.key];
        td.textContent = v === null || v === undefined ? "—" : v.toFixed(1);
        td.dataset.value = v ?? "";
      } else {
        td.textContent = row[h.key] ?? "—";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function renderHeatmap(viewId, sectors) {
  const host = document.querySelector(`#${viewId} .heatmap-host`);
  if (!sectors || sectors.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма секторни данни.</div>`;
    return;
  }

  const allDeltas = sectors.flatMap((s) => [s.mean_delta_1m, s.mean_delta_3m]).filter((v) => v !== null);
  const maxAbs = Math.max(1, ...allDeltas.map(Math.abs));

  const wrap = document.createElement("div");
  wrap.className = "heatmap";

  const header = document.createElement("div");
  header.className = "heatmap-row header";
  header.innerHTML = `
    <div>Sector</div>
    <div style="text-align:center">Δ 1m</div>
    <div style="text-align:center">Δ 3m</div>
    <div style="text-align:center">Total</div>
    <div style="text-align:center">Risers</div>
    <div style="text-align:center">Decayers</div>
  `;
  wrap.appendChild(header);

  sectors.forEach((s) => {
    const row = document.createElement("div");
    row.className = "heatmap-row";
    row.innerHTML = `
      <div><strong>${s.sector ?? "Unknown"}</strong></div>
      <div class="heat-cell" style="background:${heatColor(s.mean_delta_1m, maxAbs)}">${formatDelta(s.mean_delta_1m)}</div>
      <div class="heat-cell" style="background:${heatColor(s.mean_delta_3m, maxAbs)}">${formatDelta(s.mean_delta_3m)}</div>
      <div style="text-align:center">${s.n_total}</div>
      <div style="text-align:center; color:var(--riser)">${s.n_risers}</div>
      <div style="text-align:center; color:var(--decayer)">${s.n_decayers}</div>
    `;
    wrap.appendChild(row);
  });

  host.replaceChildren(wrap);
}

function renderSubIndustryTable(viewId, subs) {
  const host = document.querySelector(`#${viewId} .table-host`);
  if (!subs || subs.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма sub-industry данни.</div>`;
    return;
  }

  const headers = [
    { key: "sector", label: "Sector" },
    { key: "sub_industry", label: "Sub-Industry" },
    { key: "mean_delta_1m", label: "Δ 1m" },
    { key: "mean_delta_3m", label: "Δ 3m" },
    { key: "n_total", label: "N" },
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  headers.forEach((h, idx) => {
    const th = document.createElement("th");
    th.textContent = h.label;
    th.dataset.col = idx;
    th.dataset.key = h.key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  subs.forEach((s) => {
    const tr = document.createElement("tr");
    headers.forEach((h) => {
      const td = document.createElement("td");
      if (h.key === "mean_delta_1m" || h.key === "mean_delta_3m") {
        const v = s[h.key];
        if (v === null || v === undefined) {
          td.textContent = "—";
        } else {
          td.textContent = (v > 0 ? "+" : "") + v.toFixed(2);
          td.className = v > 0 ? "delta-positive" : v < 0 ? "delta-negative" : "";
        }
        td.dataset.value = v ?? "";
      } else {
        td.textContent = s[h.key] ?? "—";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.replaceChildren(table);
  attachSorting(table, headers);
}

function renderRankAll(viewId, stocks) {
  const host = document.getElementById("rank-table-host");
  const sectorSelect = document.getElementById("rank-sector");
  const quadrantSelect = document.getElementById("rank-quadrant");
  const searchInput = document.getElementById("rank-search");
  const countPill = document.getElementById("rank-count");

  if (!stocks || stocks.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма rank данни.</div>`;
    return;
  }

  const sectors = Array.from(new Set(stocks.map((s) => s.sector).filter(Boolean))).sort();
  sectors.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    sectorSelect.appendChild(opt);
  });

  const headers = [
    { key: "rank_position", label: "#" },
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "score", label: "Score" },
    { key: "abs_strength", label: "Abs %ile" },
    { key: "mom_12_1_pct", label: "12-1 Mom %" },
    { key: "base_rank_6m", label: "Base (6m)" },
    { key: "delta_1m", label: "Δ 1m" },
    { key: "delta_3m", label: "Δ 3m" },
    { key: "quadrant_1m", label: "Quad 1m" },
    { key: "quadrant_3m", label: "Quad 3m" },
  ];

  let currentSort = { key: "rank_position", desc: false };

  function fmtCell(td, key, value) {
    if (value === null || value === undefined) {
      td.textContent = "—";
      td.dataset.value = "";
      return;
    }
    if (key === "ticker") {
      td.innerHTML = `<a class="ticker" href="https://finance.yahoo.com/quote/${value}" target="_blank" rel="noopener">${value}</a>`;
      td.dataset.value = value;
      return;
    }
    if (key === "rank_position") {
      td.textContent = value;
      td.dataset.value = value;
      td.style.fontWeight = "600";
      td.style.color = "var(--text-dim)";
      return;
    }
    if (key === "mom_12_1_pct") {
      td.textContent = (value > 0 ? "+" : "") + value.toFixed(1) + "%";
      td.className = value > 0 ? "delta-positive" : value < 0 ? "delta-negative" : "";
      td.dataset.value = value;
      return;
    }
    if (key === "delta_1m" || key === "delta_3m") {
      td.textContent = (value > 0 ? "+" : "") + value.toFixed(1);
      td.className = value > 0 ? "delta-positive" : value < 0 ? "delta-negative" : "";
      td.dataset.value = value;
      return;
    }
    if (key === "quadrant_1m" || key === "quadrant_3m") {
      const cls = {
        "Stable Winner": "quadrant-stable_winner",
        "Quality Dip": "quadrant-decayer",
        "Faded Bounce": "quadrant-riser",
        "Chronic Loser": "quadrant-chronic_loser",
        "Neutral": "quadrant-neutral",
      }[value] || "quadrant-neutral";
      td.innerHTML = `<span class="quadrant ${cls}">${value}</span>`;
      td.dataset.value = value;
      return;
    }
    if (typeof value === "number") {
      td.textContent = value.toFixed(1);
      td.dataset.value = value;
      return;
    }
    td.textContent = value;
    td.dataset.value = value;
  }

  function applyFilters() {
    const sector = sectorSelect.value;
    const quadrant = quadrantSelect.value;
    const query = searchInput.value.trim().toLowerCase();

    let filtered = stocks.filter((s) => {
      if (sector && s.sector !== sector) return false;
      if (quadrant && s.quadrant_1m !== quadrant) return false;
      if (query) {
        const t = (s.ticker || "").toLowerCase();
        const n = (s.name || "").toLowerCase();
        if (!t.includes(query) && !n.includes(query)) return false;
      }
      return true;
    });

    if (currentSort.key) {
      const k = currentSort.key;
      const dir = currentSort.desc ? -1 : 1;
      filtered = [...filtered].sort((a, b) => {
        const va = a[k];
        const vb = b[k];
        if (va === null || va === undefined) return 1;
        if (vb === null || vb === undefined) return -1;
        if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
        return String(va).localeCompare(String(vb)) * dir;
      });
    }

    countPill.textContent = `${filtered.length} / ${stocks.length} акции`;
    exportState["rank-all"] = filtered;
    renderTable(filtered);
  }

  function renderTable(rows) {
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    headers.forEach((h, idx) => {
      const th = document.createElement("th");
      th.textContent = h.label;
      th.dataset.col = idx;
      th.dataset.key = h.key;
      if (currentSort.key === h.key) {
        th.classList.add(currentSort.desc ? "sort-desc" : "sort-asc");
      }
      th.addEventListener("click", () => {
        currentSort.desc = !(currentSort.key === h.key && currentSort.desc);
        currentSort.key = h.key;
        applyFilters();
      });
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      headers.forEach((h) => {
        const td = document.createElement("td");
        fmtCell(td, h.key, row[h.key]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    host.replaceChildren(table);
  }

  sectorSelect.addEventListener("change", applyFilters);
  quadrantSelect.addEventListener("change", applyFilters);
  searchInput.addEventListener("input", applyFilters);

  applyFilters();
}

function renderScreener(viewId, screenerData) {
  const host = document.querySelector(`#${viewId} .screener-table-host`);
  const sectorSelect = document.getElementById("screener-sector");
  const sizeSelect = document.getElementById("screener-size");
  const searchInput = document.getElementById("screener-search");
  const countPill = document.getElementById("screener-count");

  if (!screenerData || !screenerData.stocks || screenerData.stocks.length === 0) {
    host.innerHTML = `<div class="empty-state">Няма screener данни.</div>`;
    return;
  }

  const stocks = screenerData.stocks;

  // Populate sector dropdown
  const sectors = Array.from(new Set(stocks.map((s) => s.sector).filter(Boolean))).sort();
  sectors.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    sectorSelect.appendChild(opt);
  });

  const headers = [
    { key: "ticker", label: "Ticker" },
    { key: "name", label: "Name", cls: "col-name" },
    { key: "sector", label: "Sector" },
    { key: "country", label: "Country" },
    { key: "etf_weight_pct", label: "Weight %" },
    { key: "size_bucket", label: "Size" },
    { key: "ret_1m", label: "1M %" },
    { key: "ret_3m", label: "3M %" },
    { key: "ret_6m", label: "6M %" },
    { key: "ret_ytd", label: "YTD %" },
    { key: "ret_1y", label: "1Y %" },
    { key: "ret_3y", label: "3Y %" },
    { key: "ret_5y", label: "5Y %" },
    { key: "vol_1y", label: "Vol 1Y %" },
    { key: "sharpe_1y", label: "Sharpe 1Y" },
    { key: "sharpe_3y", label: "Sharpe 3Y" },
    { key: "maxdd_1y", label: "MaxDD 1Y %" },
    { key: "maxdd_3y", label: "MaxDD 3Y %" },
    { key: "maxdd_5y", label: "MaxDD 5Y %" },
    { key: "calmar_3y", label: "Calmar 3Y" },
    { key: "dist_52w_high", label: "from 52w-H %" },
    { key: "days_since_52w_high", label: "Days since H" },
    { key: "beta_1y", label: "Beta 1Y" },
  ];

  let currentSort = { key: null, desc: true };

  function fmtCell(td, key, value) {
    if (value === null || value === undefined) {
      td.textContent = "—";
      td.dataset.value = "";
      return;
    }
    if (key === "ticker") {
      td.innerHTML = `<a class="ticker" href="https://finance.yahoo.com/quote/${value}" target="_blank" rel="noopener">${value}</a>`;
      td.dataset.value = value;
      return;
    }
    if (key === "etf_weight_pct") {
      td.textContent = value.toFixed(2) + "%";
      td.dataset.value = value;
      return;
    }
    if (typeof value === "number") {
      const isReturnLike = key.startsWith("ret_") || key.startsWith("maxdd_") || key === "dist_52w_high";
      if (isReturnLike) {
        td.textContent = (value > 0 ? "+" : "") + value.toFixed(1) + "%";
        td.className = value > 0 ? "delta-positive" : value < 0 ? "delta-negative" : "";
      } else if (key.startsWith("vol_")) {
        td.textContent = value.toFixed(1) + "%";
      } else if (key === "days_since_52w_high") {
        td.textContent = Math.round(value);
      } else {
        td.textContent = value.toFixed(2);
      }
      td.dataset.value = value;
      return;
    }
    td.textContent = value;
    td.dataset.value = value;
  }

  function applyFilters() {
    const sector = sectorSelect.value;
    const size = sizeSelect.value;
    const query = searchInput.value.trim().toLowerCase();

    let filtered = stocks.filter((s) => {
      if (sector && s.sector !== sector) return false;
      if (size && s.size_bucket !== size) return false;
      if (query) {
        const t = (s.ticker || "").toLowerCase();
        const n = (s.name || "").toLowerCase();
        if (!t.includes(query) && !n.includes(query)) return false;
      }
      return true;
    });

    if (currentSort.key) {
      const k = currentSort.key;
      const dir = currentSort.desc ? -1 : 1;
      filtered = [...filtered].sort((a, b) => {
        const va = a[k];
        const vb = b[k];
        if (va === null || va === undefined) return 1;
        if (vb === null || vb === undefined) return -1;
        if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
        return String(va).localeCompare(String(vb)) * dir;
      });
    }

    countPill.textContent = `${filtered.length} / ${stocks.length} акции`;
    exportState["screener"] = filtered;
    renderTable(filtered);
  }

  function renderTable(rows) {
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    headers.forEach((h, idx) => {
      const th = document.createElement("th");
      th.textContent = h.label;
      th.dataset.col = idx;
      th.dataset.key = h.key;
      if (h.cls) th.classList.add(h.cls);
      if (currentSort.key === h.key) {
        th.classList.add(currentSort.desc ? "sort-desc" : "sort-asc");
      }
      th.addEventListener("click", () => {
        currentSort.desc = !(currentSort.key === h.key && currentSort.desc);
        currentSort.key = h.key;
        applyFilters();
      });
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      headers.forEach((h) => {
        const td = document.createElement("td");
        if (h.cls) td.classList.add(h.cls);
        fmtCell(td, h.key, row[h.key]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    host.replaceChildren(table);
  }

  sectorSelect.addEventListener("change", applyFilters);
  sizeSelect.addEventListener("change", applyFilters);
  searchInput.addEventListener("input", applyFilters);

  applyFilters();
}

function heatColor(value, maxAbs) {
  if (value === null || value === undefined) return "var(--bg-elev-2)";
  const t = Math.max(-1, Math.min(1, value / maxAbs));
  if (t > 0) {
    const alpha = 0.15 + t * 0.55;
    return `rgba(46, 160, 67, ${alpha.toFixed(2)})`;
  } else {
    const alpha = 0.15 + Math.abs(t) * 0.55;
    return `rgba(248, 81, 73, ${alpha.toFixed(2)})`;
  }
}

function formatDelta(v) {
  if (v === null || v === undefined) return "—";
  return (v > 0 ? "+" : "") + v.toFixed(2);
}

function attachSorting(table, headers) {
  const ths = table.querySelectorAll("th");
  ths.forEach((th, idx) => {
    th.addEventListener("click", () => {
      const desc = !th.classList.contains("sort-desc");
      ths.forEach((x) => x.classList.remove("sort-asc", "sort-desc"));
      th.classList.add(desc ? "sort-desc" : "sort-asc");
      sortTableByCol(table, idx, desc, headers[idx].key);
    });
  });
}

function sortTableByCol(table, colIdx, desc, key) {
  const tbody = table.querySelector("tbody");
  const rows = Array.from(tbody.querySelectorAll("tr"));
  rows.sort((a, b) => {
    const va = a.children[colIdx]?.dataset.value ?? a.children[colIdx]?.textContent ?? "";
    const vb = b.children[colIdx]?.dataset.value ?? b.children[colIdx]?.textContent ?? "";
    const na = parseFloat(va);
    const nb = parseFloat(vb);
    if (!isNaN(na) && !isNaN(nb)) {
      return desc ? nb - na : na - nb;
    }
    return desc ? vb.localeCompare(va) : va.localeCompare(vb);
  });
  rows.forEach((r) => tbody.appendChild(r));
}

// Map от tab id към human-readable label + sheet column conf за Excel export.
// За всеки tab дефинираме кои полета да се пишат и под какви имена.
const EXPORT_CONFIG = {
  "stable-winners-1m": {
    label: "Stable Winners 1m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "stable-winners-3m": {
    label: "Stable Winners 3m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "quality-dip-1m": {
    label: "Quality Dip 1m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "quality-dip-3m": {
    label: "Quality Dip 3m",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "faded-bounces": {
    label: "Faded Bounces",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["current_rank", "Sector Rank"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "current-strength": {
    label: "Current Strength",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["mom_12_1_pct", "12-1 Mom %"], ["abs_strength", "Abs %ile"], ["current_rank", "Sector Rank"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
    ],
  },
  "rank-all": {
    label: "Rank All",
    columns: [
      ["rank_position", "#"], ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["sub_industry", "Sub-Industry"],
      ["score", "Score"], ["abs_strength", "Abs %ile"], ["mom_12_1_pct", "12-1 Mom %"],
      ["base_rank_6m", "Base 6m"], ["delta_1m", "Δ 1m"], ["delta_3m", "Δ 3m"],
      ["quadrant_1m", "Quadrant 1m"], ["quadrant_3m", "Quadrant 3m"],
    ],
  },
  "screener": {
    label: "Universe Screener",
    columns: [
      ["ticker", "Ticker"], ["name", "Name"], ["sector", "Sector"], ["country", "Country"],
      ["etf_weight_pct", "Weight %"], ["size_bucket", "Size"],
      ["ret_1m", "1M %"], ["ret_3m", "3M %"], ["ret_6m", "6M %"], ["ret_ytd", "YTD %"],
      ["ret_1y", "1Y %"], ["ret_3y", "3Y %"], ["ret_5y", "5Y %"],
      ["vol_1y", "Vol 1Y %"], ["sharpe_1y", "Sharpe 1Y"], ["sharpe_3y", "Sharpe 3Y"],
      ["maxdd_1y", "MaxDD 1Y %"], ["maxdd_3y", "MaxDD 3Y %"], ["maxdd_5y", "MaxDD 5Y %"],
      ["calmar_3y", "Calmar 3Y"], ["dist_52w_high", "from 52w-H %"],
      ["days_since_52w_high", "Days since H"], ["beta_1y", "Beta 1Y"],
    ],
  },
};

function exportTabToExcel(tabId) {
  if (typeof XLSX === "undefined") {
    alert("XLSX library още не е заредена. Изчакай един момент и опитай пак.");
    return;
  }
  const config = EXPORT_CONFIG[tabId];
  if (!config) return;

  const rows = exportState[tabId] || [];
  if (rows.length === 0) {
    alert("Няма данни за export.");
    return;
  }

  const sheetData = rows.map((row) => {
    const out = {};
    config.columns.forEach(([key, label]) => {
      const v = row[key];
      out[label] = v === null || v === undefined ? "" : v;
    });
    return out;
  });

  const ws = XLSX.utils.json_to_sheet(sheetData);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, config.label.substring(0, 31));

  const dateStr = (exportState.asOf || "data").replace(/[^0-9-]/g, "");
  const safeName = tabId.replace(/-/g, "_");
  const filename = `rotation_radar_${safeName}_${dateStr}.xlsx`;
  XLSX.writeFile(wb, filename);
}

function setupExportButtons() {
  document.querySelectorAll(".export-btn").forEach((btn) => {
    const tabId = btn.dataset.export;
    btn.addEventListener("click", () => exportTabToExcel(tabId));
  });
}

function setupTabs() {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.tab;
      document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b === btn));
      document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === target));
      // Full-width body (max-width: none) за tabs с пълни таблици — повече място
      // за хоризонтална таблица. Sticky thead работи защото table-host е
      // overflow:auto с фиксирана височина (75vh).
      const fullTableTabs = ["screener", "rank-all"];
      document.body.classList.toggle("screener-mode", fullTableTabs.includes(target));
    });
  });
}
