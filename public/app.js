// ✅ กันพัง: บางส่วนของโค้ดเดิมเรียก analytics แต่ไม่ได้ประกาศ
window.analytics = window.analytics || {};
"use strict";

/** =====================================================
 * LivingInsider Scraper UI (Robust + Dashboard) — FULL FIXED app.js
 * ✅ แก้ “กราฟไม่ขึ้น / error” แบบครบทุกบรรทัด
 * ✅ กัน 304 / cache ด้วย cache-buster + fetch no-store (ฝั่ง client)
 * ✅ กัน Chart.js v4: horizontalBar ไม่มีแล้ว (ใช้ bar + indexAxis:"y")
 * ✅ กัน element ไม่มี -> ไม่พังทั้งไฟล์ (null guards)
 * ✅ กัน chart วาดก่อน canvas show -> force reflow + maintainAspectRatio false บางกราฟ
 *
 * Requires (ใน index.html):
 * - <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
 * - <script defer src="/app.js"></script>
 *
 * Server endpoints:
 *   POST /api/scrape -> {jobId}
 *   SSE  /api/scrape/progress/:jobId
 *   GET  /api/job/:id -> {jobId,status,rows,meta,error,insights?}
 *   GET  /api/export.csv?jobId=
 *   GET  /api/export.xlsx?jobId=
 * ===================================================== */

/* ---------------------------
 * DOM helpers
 * --------------------------- */
const $ = (sel) => document.querySelector(sel);
const byId = (id) => document.getElementById(id);

/**
 * NOTE:
 * โค้ดเดิมของคุณประกาศ els ด้วย byId(...) ทันที
 * ถ้า app.js ไม่ได้ defer หรือ DOM ยังไม่พร้อม -> els จะเป็น null
 * แต่ของคุณโหลดด้วย <script defer> แล้วก็โอเค
 * อย่างไรก็ตาม เราจะ “re-cache” ใน init() อีกครั้งเพื่อกันพลาด
 */
let els = {};

/* ---------------------------
 * State
 * --------------------------- */
let state = {
  rows: [],
  jobId: "",
  sse: null,
  abortController: null,
  isRunning: false,
  lastMeta: null,
  lastStatus: "",
};

// Global Charts Storage (Chart.js instances)
let dashboardCharts = {};
let chartOverallRankingInstance = null;

/* ---------------------------
 * Utils
 * --------------------------- */
function cleanText(s) {
  return String(s ?? "").trim();
}
function clampInt(n, min, max, fallback = min) {
  n = Number(n);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}
function humanNumber(n) {
  if (n == null || n === "") return "";
  const x = Number(n);
  return Number.isFinite(x) ? x.toLocaleString() : String(n);
}
function isUrl(s) {
  return typeof s === "string" && /^https?:\/\//i.test(s);
}
function origin() {
  return window.location.origin;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Safe number parser:
 * รองรับ "฿ 1,234,000" / "1.2M" / "900K" / number
 */
function toNum(v) {
  if (v == null) return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const s = String(v).trim();
  if (!s) return null;

  // handle shorthand
  const m = s.match(/(-?\d+(?:\.\d+)?)\s*([KMB])\b/i);
  if (m) {
    const base = Number(m[1]);
    if (!Number.isFinite(base)) return null;
    const unit = m[2].toUpperCase();
    const mult = unit === "K" ? 1e3 : unit === "M" ? 1e6 : 1e9;
    return base * mult;
  }

  const cleaned = s.replace(/[^\d.\-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

// ✅ เปิด Dashboard ก่อน แล้วค่อยวาดกราฟ (กัน canvas 0px)
function showDashboardAndRender(rows) {
  try {
    const sec = document.getElementById("dashboardSection");
    if (sec) sec.style.display = "block";

    // บังคับให้ DOM layout ก่อนวาดกราฟ
    requestAnimationFrame(() => {
      if (typeof window.renderDashboard === "function") {
        window.renderDashboard(rows);
      } else {
        console.warn("renderDashboard() not found");
      }
    });
  } catch (e) {
    console.error(e);
    const err = document.getElementById("statusError");
    if (err) {
      err.style.display = "block";
      err.textContent = "Dashboard error: " + (e?.message || e);
    }
  }
}

/**
 * Format money compact (numbers in raw Baht)
 */
function fmtMoneyCompact(n) {
  const v = toNum(n);
  if (v == null) return "-";
  if (v >= 1e9) return (v / 1e9).toFixed(1).replace(/\.0$/, "") + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(0) + "K";
  return String(Math.round(v));
}

/* ---------------------------
 * UI
 * --------------------------- */
function showError(msg) {
  if (!els.statusError) return;
  if (msg) {
    els.statusError.style.display = "block";
    els.statusError.textContent = String(msg);
  } else {
    els.statusError.style.display = "none";
    els.statusError.textContent = "";
  }
}
function setStatus(text, help = "") {
  if (els.statusText) els.statusText.textContent = text || "";
  if (els.statusHelp) els.statusHelp.textContent = help || "";
}
function setMeta(text) {
  if (els.meta) els.meta.textContent = text || "";
}
function setProgress(percent, labelText = "") {
  const p = Math.max(0, Math.min(100, Number(percent) || 0));
  if (els.progressBar) els.progressBar.style.width = `${p}%`;
  if (els.progressText) els.progressText.textContent = `${Math.round(p)}%`;
  if (labelText) setStatus(labelText);
}
function setBusy(busy) {
  state.isRunning = !!busy;
  if (els.btnRun) els.btnRun.disabled = busy;
  if (els.btnSample) els.btnSample.disabled = busy;
  if (els.btnStop) els.btnStop.disabled = !busy;

  if (busy) {
    if (els.btnRun) els.btnRun.textContent = "กำลังทำงาน…";
    if (els.btnSample) els.btnSample.textContent = "กำลังทำงาน…";
    setProgress(0, "กำลังเริ่มต้น…");
  } else {
    if (els.btnRun) els.btnRun.textContent = "Run Scrape";
    if (els.btnSample) els.btnSample.textContent = "Sample 10";
  }
}
function updateExportButtons() {
  const hasJob = !!state.jobId;

  if (els.btnExportCsv) els.btnExportCsv.disabled = !hasJob;
  if (els.btnExportXlsx) els.btnExportXlsx.disabled = !hasJob;
  if (els.btnCopySheets) els.btnCopySheets.disabled = !hasJob;

  if (els.btnExportCsv) els.btnExportCsv.setAttribute("aria-disabled", String(els.btnExportCsv.disabled));
  if (els.btnExportXlsx) els.btnExportXlsx.setAttribute("aria-disabled", String(els.btnExportXlsx.disabled));
  if (els.btnCopySheets) els.btnCopySheets.setAttribute("aria-disabled", String(els.btnCopySheets.disabled));

  if (els.resultHint) {
    const hasRows = Array.isArray(state.rows) && state.rows.length > 0;
    els.resultHint.textContent = hasRows ? `${humanNumber(state.rows.length)} รายการ` : "No results";
  }
}

/* ---------------------------
 * Table
 * --------------------------- */
function renderTable(rows) {
  if (!els.thead || !els.tbody) return;

  els.thead.innerHTML = "";
  els.tbody.innerHTML = "";

  if (!Array.isArray(rows) || rows.length === 0) {
    const tr = document.createElement("tr");
    const th = document.createElement("th");
    th.textContent = "No results";
    tr.appendChild(th);
    els.thead.appendChild(tr);
    return;
  }

  // union keys (first row keys first)
  const seen = new Set();
  const cols = [];

  for (const k of Object.keys(rows[0] || {})) {
    seen.add(k);
    cols.push(k);
  }
  for (const r of rows) {
    for (const k of Object.keys(r || {})) {
      if (!seen.has(k)) {
        seen.add(k);
        cols.push(k);
      }
    }
  }

  // header
  const headerRow = document.createElement("tr");
  for (const col of cols) {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  }
  els.thead.appendChild(headerRow);

  // body
  const frag = document.createDocumentFragment();
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const col of cols) {
      const td = document.createElement("td");
      const val = row?.[col];

      if (isUrl(val)) {
        const a = document.createElement("a");
        a.href = val;
        a.target = "_blank";
        a.rel = "noreferrer";
        a.textContent = val.length > 60 ? val.slice(0, 60) + "…" : val;
        td.appendChild(a);
      } else if (Array.isArray(val)) {
        td.textContent = val.join(" | ");
      } else if (typeof val === "number") {
        td.textContent = humanNumber(val);
      } else if (val && typeof val === "object") {
        const s = JSON.stringify(val);
        td.textContent = s.length > 140 ? s.slice(0, 140) + "…" : s;
      } else {
        td.textContent = val ?? "";
      }

      tr.appendChild(td);
    }
    frag.appendChild(tr);
  }
  els.tbody.appendChild(frag);
}

/* ---------------------------
 * Form -> payload
 * --------------------------- */
function getFormPayload() {
  const startUrl = cleanText(els.startUrl?.value);
  if (!startUrl) throw new Error("กรุณาใส่ Start URL");
  if (!/^https?:\/\//i.test(startUrl)) throw new Error("Start URL ต้องขึ้นต้นด้วย http/https");

  return {
    startUrl,
    dealType: els.dealType?.value || "",
    category: els.category?.value || "",
    keyword: cleanText(els.keyword?.value || ""),
    priceMin: cleanText(els.priceMin?.value || ""),
    priceMax: cleanText(els.priceMax?.value || ""),
    maxPages: clampInt(els.maxPages?.value, 1, 200, 3),
    maxResults: clampInt(els.maxResults?.value, 1, 5000, 50),
    sampleEvery: clampInt(els.sampleEvery?.value, 1, 200, 1),
    preferFastMode: els.preferFastMode?.value || "auto",
  };
}

/* ---------------------------
 * SSE / Poll
 * --------------------------- */
function closeSSE() {
  if (state.sse) {
    try {
      state.sse.close();
    } catch {}
    state.sse = null;
  }
}
function cancelInFlight() {
  if (state.abortController) {
    try {
      state.abortController.abort();
    } catch {}
    state.abortController = null;
  }
  closeSSE();
}

async function fetchJob(jobId) {
  const url = `/api/job/${encodeURIComponent(jobId)}?t=${Date.now()}`; // ✅ cache buster

  const r = await fetch(url, {
    method: "GET",
    cache: "no-store", // ✅ สำคัญ
    headers: {
      Accept: "application/json",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
  });

  // ✅ ถ้า 304 แปลว่าไม่มี body → อย่าพยายาม parse json
  if (r.status === 304) return null;

  if (!r.ok) {
    const text = await r.text().catch(() => "");
    throw new Error(`GET /api/job failed ${r.status}: ${text}`);
  }

  return await r.json();
}

/* ---------------------------
 * Dashboard: show/hide + toggle
 * --------------------------- */
function hideDashboardSection() {
  const dashboardSection = byId("dashboardSection");
  if (dashboardSection) dashboardSection.style.display = "none";
}

function showDashboardSection() {
  const dashboardSection = byId("dashboardSection");
  if (dashboardSection) dashboardSection.style.display = "block";
}

function setDashboardCollapsed(collapsed) {
  const dashboardContent = byId("dashboardContent");
  const toggleText = byId("dashboardToggleText");
  if (!dashboardContent || !toggleText) return;

  dashboardContent.style.display = collapsed ? "none" : "block";
  toggleText.textContent = collapsed ? "แสดง Dashboard" : "ซ่อน Dashboard";
}

function bindDashboardToggle() {
  const btnToggle = byId("btnToggleDashboard");
  const dashboardContent = byId("dashboardContent");
  if (!btnToggle || !dashboardContent) return;

  btnToggle.addEventListener("click", () => {
    const isHidden = dashboardContent.style.display === "none";
    setDashboardCollapsed(!isHidden);

    // ✅ ถ้าเพิ่ง show แล้วกราฟยุบ/ไม่ render: trigger resize
    if (typeof window !== "undefined") {
      setTimeout(() => window.dispatchEvent(new Event("resize")), 120);
    }
  });
}

/* ---------------------------
 * Analytics (fixed & robust)
 * - IMPORTANT: ไม่พึ่ง index ระหว่าง prices[] กับ btsDistances[]
 *   เพราะบาง row อาจไม่มี price/bts ทำให้หลุด alignment
 * --------------------------- */
function analyzeData(rows, meta, insights) {
  console.log("📊 Analyzing data...");

  const safeAvg = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);

  const analytics = {
    total: Array.isArray(rows) ? rows.length : 0,
    meta: meta || {},
    insights: insights || {},

    // price
    prices: [],
    avgPrice: 0,
    minPrice: null,
    maxPrice: null,

    // quality
    qualities: [],
    avgQuality: 0,
    qualityRanges: { excellent: 0, good: 0, average: 0, poor: 0 },

    // BTS
    btsDistances: [],
    nearBTS: 0,
    btsStations: {},

    // categories
    categories: {},

    // facilities
    facilities: {
      pool: 0,
      gym: 0,
      parking: 0,
      security: 0,
      ev_charger: 0,
      sky_pool: 0,
      luxury: 0,
      private_lift: 0,
    },

    // scores
    scores: {
      walkability: [],
      location: [],
      facility: [],
      investment: [],
      value: [],
    },

    // top picks
    topValue: null,
    topQuality: null,
    topInvestment: null,
    topLocation: null,
  };

  const pickTop = (cur, row, field) => {
    const v = toNum(row?.[field]);
    if (v == null) return cur;
    if (!cur) return row;
    const cv = toNum(cur?.[field]) ?? -Infinity;
    return v > cv ? row : cur;
  };

  for (const row of rows) {
    const price = toNum(row.price_value ?? row.price ?? row.price_baht);
    const q = toNum(row.quality_score ?? row.quality ?? row.quality_pct);
    const dist = toNum(row.nearest_bts_distance_km ?? row.bts_distance_km);

    if (price != null && price > 0) analytics.prices.push(price);

    if (q != null) {
      analytics.qualities.push(q);
      if (q >= 90) analytics.qualityRanges.excellent++;
      else if (q >= 75) analytics.qualityRanges.good++;
      else if (q >= 60) analytics.qualityRanges.average++;
      else analytics.qualityRanges.poor++;
    }

    if (dist != null) {
      analytics.btsDistances.push(dist);
      if (dist <= 0.5) analytics.nearBTS++;

      const name = cleanText(row.nearest_bts_name || row.bts_name || "");
      if (name) {
        if (!analytics.btsStations[name]) {
          analytics.btsStations[name] = {
            name,
            count: 0,
            prices: [],
            distances: [],
            qualities: [],
          };
        }
        const st = analytics.btsStations[name];
        st.count++;
        st.distances.push(dist);
        if (price != null) st.prices.push(price);
        if (q != null) st.qualities.push(q);
      }
    }

    const cat = cleanText(row.category || "");
    if (cat) analytics.categories[cat] = (analytics.categories[cat] || 0) + 1;

    if (row.has_pool) analytics.facilities.pool++;
    if (row.has_gym) analytics.facilities.gym++;
    if (row.has_parking) analytics.facilities.parking++;
    if (row.has_security) analytics.facilities.security++;
    if (row.has_ev_charger) analytics.facilities.ev_charger++;
    if (row.has_sky_pool) analytics.facilities.sky_pool++;
    if (row.is_luxury) analytics.facilities.luxury++;
    if (row.has_private_lift) analytics.facilities.private_lift++;

    const w = toNum(row.walkability_score);
    if (w != null) analytics.scores.walkability.push(w);
    const l = toNum(row.location_score);
    if (l != null) analytics.scores.location.push(l);
    const f = toNum(row.facility_score);
    if (f != null) analytics.scores.facility.push(f);
    const inv = toNum(row.investment_score);
    if (inv != null) analytics.scores.investment.push(inv);
    const val = toNum(row.value_score);
    if (val != null) analytics.scores.value.push(val);

    analytics.topValue = pickTop(analytics.topValue, row, "value_score");
    analytics.topQuality = pickTop(analytics.topQuality, row, "quality_score");
    analytics.topInvestment = pickTop(analytics.topInvestment, row, "investment_score");
    analytics.topLocation = pickTop(analytics.topLocation, row, "location_score");
  }

  if (analytics.prices.length) {
    analytics.minPrice = Math.min(...analytics.prices);
    analytics.maxPrice = Math.max(...analytics.prices);
    analytics.avgPrice = safeAvg(analytics.prices);
  } else {
    analytics.minPrice = null;
    analytics.maxPrice = null;
    analytics.avgPrice = 0;
  }

  analytics.avgQuality = analytics.qualities.length ? Math.round(safeAvg(analytics.qualities)) : 0;

  // station rollups
  for (const st of Object.values(analytics.btsStations)) {
    st.avgPrice = st.prices.length ? safeAvg(st.prices) : 0;
    st.avgDistance = st.distances.length ? safeAvg(st.distances) : 0;
    st.avgQuality = st.qualities.length ? Math.round(safeAvg(st.qualities)) : 0;
  }

  console.log("✅ Analysis complete:", analytics);
  return analytics;
}

/* ---------------------------
 * KPI cards
 * --------------------------- */
function updateKPIs(analytics) {
  const kpiTotal = byId("kpiTotalListings");
  if (kpiTotal) kpiTotal.textContent = humanNumber(analytics.total);

  const kpiQuality = byId("kpiAvgQuality");
  if (kpiQuality) kpiQuality.textContent = `${analytics.avgQuality}%`;

  const kpiPrice = byId("kpiPriceRange");
  if (kpiPrice) {
    if (analytics.minPrice != null && analytics.maxPrice != null) {
      kpiPrice.textContent = `฿${fmtMoneyCompact(analytics.minPrice)} - ฿${fmtMoneyCompact(analytics.maxPrice)}`;
    } else {
      kpiPrice.textContent = "-";
    }
  }

  const kpiNearBTS = byId("kpiNearBTS");
  if (kpiNearBTS) kpiNearBTS.textContent = humanNumber(analytics.nearBTS);
}

/* ---------------------------
 * AI insights (client-side heuristics)
 * --------------------------- */
function showAIInsights(analytics) {
  const insightsList = byId("aiInsightsList");
  if (!insightsList) return;

  const insights = [];
  const total = Math.max(1, analytics.total);

  if (analytics.avgQuality >= 85) {
    insights.push({ icon: "⭐", text: `คุณภาพเฉลี่ย ${analytics.avgQuality}% อยู่ในระดับดีมาก! ข้อมูลมีความน่าเชื่อถือสูง` });
  } else if (analytics.avgQuality >= 70) {
    insights.push({ icon: "✅", text: `คุณภาพเฉลี่ย ${analytics.avgQuality}% อยู่ในเกณฑ์ดี ข้อมูลส่วนใหญ่ครบถ้วน` });
  } else if (analytics.avgQuality > 0) {
    insights.push({ icon: "🧩", text: `คุณภาพเฉลี่ย ${analytics.avgQuality}% ยังมีข้อมูลบางส่วนขาดหาย แนะนำเพิ่ม maxResults หรือโหมด Full` });
  }

  const nearBTSPercent = Math.round((analytics.nearBTS / total) * 100);
  if (nearBTSPercent >= 40) {
    insights.push({ icon: "🚇", text: `${nearBTSPercent}% ของประกาศอยู่ใกล้ BTS (≤500m) เดินทางสะดวก` });
  } else if (nearBTSPercent > 0) {
    insights.push({ icon: "🚉", text: `${nearBTSPercent}% อยู่ใกล้ BTS (≤500m) ถ้าเน้นติด BTS แนะนำใส่ keyword “ติดรถไฟฟ้า/ใกล้ BTS”` });
  }

  if (analytics.minPrice != null) {
    insights.push({
      icon: "💰",
      text: `ราคาเฉลี่ย ~฿${fmtMoneyCompact(analytics.avgPrice)} มีช่วงตั้งแต่ ฿${fmtMoneyCompact(analytics.minPrice)} ถึง ฿${fmtMoneyCompact(analytics.maxPrice)}`,
    });
  }

  const luxuryPercent = Math.round(((analytics.facilities.luxury || 0) / total) * 100);
  if (luxuryPercent >= 30) {
    insights.push({ icon: "✨", text: `${luxuryPercent}% เป็นโครงการระดับ Luxury พร้อมสิ่งอำนวยความสะดวกครบครัน` });
  }

  const topStations = Object.values(analytics.btsStations)
    .filter((s) => s.count >= 3)
    .sort((a, b) => b.count - a.count)
    .slice(0, 3);

  if (topStations.length) {
    insights.push({ icon: "📍", text: `สถานี BTS ที่มีประกาศมากสุด: ${topStations.map((s) => s.name).join(", ")}` });
  }

  const topCategory = Object.entries(analytics.categories).sort((a, b) => b[1] - a[1])[0];
  if (topCategory) {
    const percent = Math.round((topCategory[1] / total) * 100);
    insights.push({ icon: "🏠", text: `${topCategory[0]} มีมากที่สุด (${percent}%)` });
  }

  insightsList.innerHTML = insights
    .map(
      (ins) => `
      <div class="insight-item">
        <div class="insight-icon">${ins.icon}</div>
        <div>${ins.text}</div>
      </div>`
    )
    .join("");
}

/* ---------------------------
 * Charts
 * - Destroy old charts safely
 * - Chart.js v4 fix: horizontalBar removed -> indexAxis:"y"
 * --------------------------- */
function destroyDashboardCharts() {
  try {
    Object.values(dashboardCharts).forEach((ch) => {
      try {
        ch.destroy();
      } catch {}
    });
  } finally {
    dashboardCharts = {};
  }

  // overall ranking chart (separate instance)
  try {
    if (chartOverallRankingInstance) {
      chartOverallRankingInstance.destroy();
      chartOverallRankingInstance = null;
    }
  } catch {}
}

function ensureChartJs() {
  if (typeof Chart === "undefined") {
    console.error("Chart.js not loaded");
    showError("❌ Chart.js ยังไม่ถูกโหลด (ตรวจสอบ script CDN ใน index.html)");
    return false;
  }
  return true;
}

function createCharts(analytics) {
  if (!ensureChartJs()) return;

  destroyDashboardCharts();

  const colors = {
    primary: "rgba(59, 130, 246, 0.8)",
    primary2: "rgba(34, 211, 238, 0.8)",
    accent: "rgba(34, 197, 94, 0.8)",
    warn: "rgba(245, 158, 11, 0.8)",
    danger: "rgba(239, 68, 68, 0.8)",
    purple: "rgba(139, 92, 246, 0.8)",
  };

  createPriceDistributionChart(analytics, colors);
  createQualityScoreChart(analytics, colors);
  createBTSDistanceChart(analytics, colors);
  createCategoryChart(analytics, colors);
  createFacilityChart(analytics, colors);

  // ✅ NEW: Overall Ranking
  try {
    renderOverallRankingChart(state.rows, 10);
  } catch (e) {
    console.error("renderOverallRankingChart error:", e);
  }
}

function createPriceDistributionChart(analytics, colors) {
  const canvas = byId("chartPriceDistribution");
  if (!canvas) return;

  const ranges = { "< 3M": 0, "3-5M": 0, "5-10M": 0, "10-20M": 0, "> 20M": 0 };
  for (const price of analytics.prices) {
    const m = price / 1e6;
    if (m < 3) ranges["< 3M"]++;
    else if (m < 5) ranges["3-5M"]++;
    else if (m < 10) ranges["5-10M"]++;
    else if (m < 20) ranges["10-20M"]++;
    else ranges["> 20M"]++;
  }

  dashboardCharts.priceDistribution = new Chart(canvas, {
    type: "bar",
    data: {
      labels: Object.keys(ranges),
      datasets: [
        {
          label: "จำนวนประกาศ",
          data: Object.values(ranges),
          backgroundColor: [colors.accent, colors.primary, colors.primary2, colors.warn, colors.danger],
          borderRadius: 8,
          borderWidth: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context) => `จำนวน: ${context.parsed.y} ประกาศ`,
          },
        },
      },
      scales: {
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });
}

function createQualityScoreChart(analytics, colors) {
  const canvas = byId("chartQualityScore");
  if (!canvas) return;

  dashboardCharts.qualityScore = new Chart(canvas, {
    type: "doughnut",
    data: {
      labels: ["Excellent (≥90)", "Good (75-89)", "Average (60-74)", "Poor (<60)"],
      datasets: [
        {
          data: [
            analytics.qualityRanges.excellent,
            analytics.qualityRanges.good,
            analytics.qualityRanges.average,
            analytics.qualityRanges.poor,
          ],
          backgroundColor: [colors.accent, colors.primary, colors.warn, colors.danger],
          borderWidth: 2,
          borderColor: "rgba(255, 255, 255, 0.5)",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          position: "bottom",
          labels: { padding: 12, font: { size: 11 } },
        },
        tooltip: {
          callbacks: {
            label: (context) => {
              const label = context.label || "";
              const value = context.parsed || 0;
              const percent = analytics.total > 0 ? Math.round((value / analytics.total) * 100) : 0;
              return `${label}: ${value} (${percent}%)`;
            },
          },
        },
      },
    },
  });
}

function createBTSDistanceChart(analytics, colors) {
  const canvas = byId("chartBTSDistance");
  if (!canvas) return;

  // ถ้าไม่มีข้อมูล BTS ให้แสดงข้อความแทน ไม่วาดกราฟ
  if (!analytics.btsDistances || analytics.btsDistances.length === 0) {
    // ถ้าอยากโชว์ “ไม่มีข้อมูล” ในพื้นที่ chart ให้ทำ overlay เอง
    return;
  }

  // buckets (km)
  const buckets = {
    "≤0.5": 0,
    "0.5-1": 0,
    "1-2": 0,
    "2-3": 0,
    ">3": 0,
  };

  for (const d of analytics.btsDistances) {
    if (d <= 0.5) buckets["≤0.5"]++;
    else if (d <= 1) buckets["0.5-1"]++;
    else if (d <= 2) buckets["1-2"]++;
    else if (d <= 3) buckets["2-3"]++;
    else buckets[">3"]++;
  }

  dashboardCharts.btsDistance = new Chart(canvas, {
    type: "bar",
    data: {
      labels: Object.keys(buckets),
      datasets: [
        {
          label: "จำนวนประกาศ",
          data: Object.values(buckets),
          backgroundColor: colors.purple,
          borderRadius: 8,
          borderWidth: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `จำนวน: ${ctx.parsed.y} ประกาศ`,
          },
        },
      },
      scales: {
        y: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });
}

function createCategoryChart(analytics, colors) {
  const canvas = byId("chartCategoryBreakdown");
  if (!canvas) return;

  const categories = Object.entries(analytics.categories).sort((a, b) => b[1] - a[1]).slice(0, 10);
  if (!categories.length) return;

  const chartColors = [
    colors.primary,
    colors.primary2,
    colors.accent,
    colors.warn,
    colors.purple,
    colors.danger,
    "rgba(2, 132, 199, 0.8)",
    "rgba(14, 165, 233, 0.8)",
    "rgba(16, 185, 129, 0.8)",
    "rgba(249, 115, 22, 0.8)",
  ];

  dashboardCharts.category = new Chart(canvas, {
    type: "pie",
    data: {
      labels: categories.map((c) => c[0]),
      datasets: [
        {
          data: categories.map((c) => c[1]),
          backgroundColor: categories.map((_, i) => chartColors[i % chartColors.length]),
          borderWidth: 2,
          borderColor: "rgba(255, 255, 255, 0.5)",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { position: "bottom", labels: { padding: 10, font: { size: 11 } } },
        tooltip: {
          callbacks: {
            label: (context) => {
              const label = context.label || "";
              const value = context.parsed || 0;
              const percent = analytics.total > 0 ? Math.round((value / analytics.total) * 100) : 0;
              return `${label}: ${value} (${percent}%)`;
            },
          },
        },
      },
    },
  });
}

function createFacilityChart(analytics, colors) {
  const canvas = byId("chartFacilities");
  if (!canvas) return;

  const facilities = [
    { label: "สระว่ายน้ำ", value: analytics.facilities.pool },
    { label: "ฟิตเนส/ยิม", value: analytics.facilities.gym },
    { label: "ที่จอดรถ", value: analytics.facilities.parking },
    { label: "Security", value: analytics.facilities.security },
    { label: "EV Charger", value: analytics.facilities.ev_charger },
    { label: "Sky Pool", value: analytics.facilities.sky_pool },
    { label: "Luxury", value: analytics.facilities.luxury },
    { label: "Private Lift", value: analytics.facilities.private_lift },
  ].sort((a, b) => b.value - a.value);

  dashboardCharts.facilities = new Chart(canvas, {
    type: "bar",
    data: {
      labels: facilities.map((f) => f.label),
      datasets: [
        {
          label: "จำนวนโครงการ",
          data: facilities.map((f) => f.value),
          backgroundColor: colors.primary2,
          borderRadius: 6,
          borderWidth: 0,
        },
      ],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context) => {
              const value = context.parsed.x || 0;
              const percent = analytics.total > 0 ? Math.round((value / analytics.total) * 100) : 0;
              return `${value} โครงการ (${percent}%)`;
            },
          },
        },
      },
      scales: {
        x: { beginAtZero: true, ticks: { precision: 0 } },
      },
    },
  });
}

/* ---------------------------
 * Overall Ranking Chart (robust)
 * --------------------------- */
function clamp01(x) {
  return Math.max(0, Math.min(1, x));
}
function round1(x) {
  return Math.round(x * 10) / 10;
}
function num0(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

/**
 * scoreOverall:
 * รองรับ score เป็น 0..1 หรือ 0..100
 * - ถ้าค่า > 1.5 ถือว่าเป็น 0..100 -> แปลงเป็น 0..1
 */
function normScore01(v) {
  const n = num0(v);
  if (n <= 0) return 0;
  if (n > 1.5) return clamp01(n / 100);
  return clamp01(n);
}

function scoreOverall(row) {
  const q = normScore01(row.quality_score ?? row.quality ?? row.quality_pct);
  const v = normScore01(row.value_score ?? row.value ?? row.price_score);
  const l = normScore01(row.location_score ?? row.location ?? row.walkability_score);
  const inv = normScore01(row.investment_score ?? row.investment);

  let total = q * 0.4 + l * 0.3 + v * 0.3;
  if (inv > 0) total = total * 0.9 + inv * 0.1;

  return round1(clamp01(total) * 100); // 0..100
}

function renderOverallRankingChart(rows, topN = 10) {
  if (!ensureChartJs()) return;

  const el = byId("chartOverallRanking");
  if (!el) return;

  const ranked = (rows || [])
    .map((r) => ({
      row: r,
      score: scoreOverall(r),
      title: cleanText(r.title || r.project_name || r.name || r.listing_title || r.project || ""),
    }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, topN);

  if (!ranked.length) return;

  const labels = ranked.map((x, i) => {
    const t = x.title || "(no title)";
    return `${i + 1}. ${t.length > 28 ? t.slice(0, 28) + "…" : t}`;
  });

  const data = ranked.map((x) => x.score);

  if (chartOverallRankingInstance) {
    chartOverallRankingInstance.destroy();
    chartOverallRankingInstance = null;
  }

  // ✅ ทำให้ canvas สูงพอ (กันล้น)
  el.style.height = Math.max(280, ranked.length * 28) + "px";

  chartOverallRankingInstance = new Chart(el, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Overall Score (0-100)",
          data,
          borderWidth: 1,
          borderRadius: 10,
          barThickness: 16,
          maxBarThickness: 18,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      plugins: {
        legend: { display: true },
        tooltip: {
          callbacks: {
            afterLabel: (ctx) => {
              const item = ranked[ctx.dataIndex];
              const r = item?.row || {};
              const q = round1(normScore01(r.quality_score ?? r.quality ?? r.quality_pct) * 100);
              const l = round1(normScore01(r.location_score ?? r.location ?? r.walkability_score) * 100);
              const v = round1(normScore01(r.value_score ?? r.value ?? r.price_score) * 100);
              return `Q:${q}  L:${l}  V:${v}`;
            },
          },
        },
      },
      scales: {
        x: {
          beginAtZero: true,
          max: 100,
          ticks: { callback: (v) => v + "%" },
        },
        y: {
          ticks: {
            autoSkip: false,
            font: { size: 11 },
          },
        },
      },
    },
  });
}

/* ---------------------------
 * Top recommendations + Hotspots
 * --------------------------- */
function setText(id, text) {
  const el = byId(id);
  if (el) el.textContent = text ?? "";
}

function showTopRecommendations(analytics) {
  // Best Value
  if (analytics.topValue) {
    const row = analytics.topValue;
    setText("topValue", row.value_score ?? "-");
    setText("topValueTitle", row.listing_title || row.project_name || "-");
    setText("topValuePrice", row.price_value ? `฿${(toNum(row.price_value) / 1e6).toFixed(2)}M` : "-");
    setText("topValueBTS", row.nearest_bts_name || "-");
  }

  // Best Quality
  if (analytics.topQuality) {
    const row = analytics.topQuality;
    setText("topQuality", row.quality_score ?? "-");
    setText("topQualityTitle", row.listing_title || row.project_name || "-");
    setText("topQualityScore", row.quality_score != null ? `${row.quality_score}%` : "-");
    setText("topQualityFacilities", row.facility_count != null ? String(row.facility_count) : "0");
  }

  // Best Investment
  if (analytics.topInvestment) {
    const row = analytics.topInvestment;
    setText("topInvestment", row.investment_score ?? "-");
    setText("topInvestmentTitle", row.listing_title || row.project_name || "-");
    setText("topInvestmentScore", row.investment_score != null ? `${row.investment_score}%` : "-");
    setText("topInvestmentLocation", row.location_text || "-");
  }

  // Best Location
  if (analytics.topLocation) {
    const row = analytics.topLocation;
    setText("topLocation", row.location_score ?? "-");
    setText("topLocationTitle", row.listing_title || row.project_name || "-");
    setText("topLocationWalk", row.walkability_score != null ? `${row.walkability_score}%` : "-");
    setText(
      "topLocationBTS",
      row.nearest_bts_name
        ? `${row.nearest_bts_name}${row.nearest_bts_distance_km != null ? ` (${row.nearest_bts_distance_km}km)` : ""}`
        : "-"
    );
  }
}

function showBTSHotspots(analytics) {
  const hotspotsList = byId("btsHotspotsList");
  if (!hotspotsList) return;

  const topStations = Object.values(analytics.btsStations)
    .filter((s) => s.count >= 2)
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  if (!topStations.length) {
    hotspotsList.innerHTML = '<div class="help">ไม่พบข้อมูลสถานี BTS</div>';
    return;
  }

  hotspotsList.innerHTML = topStations
    .map(
      (station) => `
      <div class="hotspot-item">
        <div class="hotspot-info">
          <div class="hotspot-name">${station.name}</div>
          <div class="hotspot-stats">
            <span>💰 ฿${(station.avgPrice / 1e6).toFixed(1)}M</span>
            <span>📍 ${station.avgDistance.toFixed(2)}km</span>
            <span>⭐ ${station.avgQuality}%</span>
          </div>
        </div>
        <div class="hotspot-badge">${station.count} ประกาศ</div>
      </div>
    `
    )
    .join("");
}

/* ---------------------------
 * Dashboard entry
 * --------------------------- */
function showDashboard(rows, meta, insights) {
  if (!rows || rows.length === 0) {
    console.log("No data to show dashboard");
    return;
  }

  console.log("🎯 Generating Dashboard...", { rows: rows.length, meta, insights });

  // show section
  showDashboardSection();
  showDashboardAndRender(rows); // ✅ เพิ่มบรรทัดนี้
  // default expanded
  setDashboardCollapsed(false);

  // analyze
  const analytics = analyzeData(rows, meta, insights);

  // update UI blocks
  updateKPIs(analytics);
  showAIInsights(analytics);

  // charts
  createCharts(analytics);

  // top picks & hotspots
  showTopRecommendations(analytics);
  showBTSHotspots(analytics);

  // scroll
  const dashboardSection = byId("dashboardSection");
  if (dashboardSection) {
    setTimeout(() => {
      dashboardSection.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 250);
  }
}

/* ---------------------------
 * ✅ Apply final result (FIXED)
 * - คำนวณ analytics ในที่เดียว
 * - วาดกราฟหลัง showDashboardSection + content visible
 * --------------------------- */
function applyFinal(rows, meta, insights, jobId) {
  state.rows = Array.isArray(rows) ? rows : [];
  state.lastMeta = meta || null;
  state.jobId = jobId || state.jobId || "";

  renderTable(state.rows);
  showDashboardAndRender(rows); // ✅ เพิ่มบรรทัดนี้
  updateExportButtons();

  const errCount = Array.isArray(meta?.errors) ? meta.errors.length : 0;
  const pages = meta?.pagesVisited ?? "-";
  const collected = meta?.collected_links ?? meta?.total_collected ?? "-";
  const elapsedMs = meta?.elapsedMs;

  showError("");
  setProgress(100, "เสร็จสิ้น!");
  setMeta(
    `✅ เสร็จสิ้น! rows=${humanNumber(state.rows.length)} | pages=${humanNumber(pages)} | collected=${humanNumber(
      collected
    )} | errors=${humanNumber(errCount)} | time=${elapsedMs ? Math.round(elapsedMs / 1000) : "-"}s`
  );

  // ✅ Show dashboard safely
  try {
    showDashboard(state.rows, meta || {}, insights || {});
  } catch (e) {
    console.error("showDashboard error:", e);
    showError(e?.message || String(e));
  }

  setBusy(false);
}

/* ---------------------------
 * SSE attach
 * --------------------------- */
function attachSSE(jobId) {
  closeSSE();
  const url = `/api/scrape/progress/${encodeURIComponent(jobId)}`;
  const es = new EventSource(url);
  state.sse = es;

  es.onopen = () => {
    setStatus("เชื่อมต่อแล้ว", "กำลังติดตามความคืบหน้า…");
  };

  es.onmessage = async (ev) => {
    let msg = null;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }

    const status = msg.status || msg.stage || "";
    const meta = msg.meta || {};
    const message = msg.message || "";
    const insights = msg.insights || msg.ai || null;

    state.lastMeta = meta;
    state.lastStatus = status;

    // rough progress
    const pagesVisited = meta.pagesVisited ?? 0;
    const collected = meta.collected_links ?? meta.total_collected ?? 0;
    const parsed = meta.total_parsed ?? meta.totalParsed ?? 0;

    let p = 10;
    if (pagesVisited > 0) p = 10 + Math.min(35, pagesVisited * 10);
    if (collected > 0) {
      const ratio = parsed / Math.max(1, collected);
      p = Math.min(95, 40 + ratio * 55);
    }

    if (status === "error") {
      showError(msg.error || "Unknown error");
      setBusy(false);
      setStatus("Error!", "สแครปล้มเหลว");
      setMeta(`❌ ${msg.error || "Unknown error"}`);
      closeSSE();
      return;
    }

    if (status === "done") {
      setProgress(98, "เสร็จแล้ว กำลังโหลดผลลัพธ์…");
      closeSSE();

      try {
        const job = await fetchJob(jobId);

        // ถ้าเจอ null เพราะ 304 (ควรไม่เกิดแล้ว) -> poll ต่อ
        if (!job) {
          pollJob(jobId, 60);
          return;
        }

        if (job.status === "done") {
          applyFinal(job.rows, job.meta, job.insights || insights, jobId);
        } else if (job.status === "error") {
          showError(job.error || "Unknown error");
          setBusy(false);
          setStatus("Error!", "งานล้มเหลว");
          setMeta(`❌ ${job.error || "Unknown error"}`);
        } else {
          pollJob(jobId, 60);
        }
      } catch (e) {
        showError(e.message || String(e));
        pollJob(jobId, 60);
      }
      return;
    }

    showError("");
    setProgress(p, message || "กำลังทำงาน…");
    setMeta(`กำลังทำงาน: pages=${humanNumber(pagesVisited)} | links=${humanNumber(collected)} | parsed=${humanNumber(parsed)}`);
  };

  es.onerror = () => {
    // SSE unsupported / dropped -> poll
    closeSSE();
    pollJob(jobId, 60);
  };
}

async function pollJob(jobId, maxAttempts = 60) {
  for (let i = 0; i < maxAttempts; i++) {
    if (!state.isRunning) return;

    await sleep(1500);
    try {
      const job = await fetchJob(jobId);

      // ถ้า null เพราะ 304 -> skip loop
      if (!job) continue;

      if (job.status === "error") {
        showError(job.error || "Unknown error");
        setBusy(false);
        setStatus("Error!", "งานล้มเหลว");
        setMeta(`❌ ${job.error || "Unknown error"}`);
        return;
      }

      if (job.status === "done") {
        applyFinal(job.rows, job.meta, job.insights || {}, jobId);
        return;
      }

      const meta = job.meta || state.lastMeta || {};
      const parsed = meta.total_parsed ?? meta.totalParsed ?? 0;
      const pagesVisited = meta.pagesVisited ?? 0;

      const pct = Math.min(95, 15 + (i / maxAttempts) * 80);
      setProgress(pct, "กำลังทำงาน…");
      setMeta(`Polling ${i + 1}/${maxAttempts} | pages=${humanNumber(pagesVisited)} | parsed=${humanNumber(parsed)}`);
    } catch (e) {
      showError(e.message || String(e));
    }
  }

  setBusy(false);
  setStatus("Timeout", "รอนานเกินไป");
  setMeta("⚠️ Timeout: ไม่พบผลลัพธ์จาก /api/job ภายในเวลาที่กำหนด");
}

/* ---------------------------
 * Run scrape
 * --------------------------- */
async function runScrape({ sample = false } = {}) {
  cancelInFlight();
  showError("");
  state.abortController = new AbortController();

  setBusy(true);
  state.rows = [];
  state.jobId = "";
  renderTable([]);
  updateExportButtons();

  // hide dashboard until done
  hideDashboardSection();
  destroyDashboardCharts();

  try {
    const payload = getFormPayload();

    if (sample) {
      payload.maxPages = 1;
      payload.maxResults = Math.min(payload.maxResults || 10, 10);
      payload.sampleEvery = Math.max(payload.sampleEvery || 1, 2);
    }

    setProgress(0, "กำลังส่งคำขอ…");
    setMeta("");

    const resp = await fetch(`/api/scrape?t=${Date.now()}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: state.abortController.signal,
      cache: "no-store",
    });

    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data?.error || `HTTP ${resp.status}`);

    const jobId = data?.jobId;
    if (!jobId) throw new Error("Server did not return jobId");

    state.jobId = jobId;
    updateExportButtons();

    setProgress(5, "ได้รับ Job ID แล้ว");
    setMeta(`Job ID: ${jobId.slice(0, 8)}…`);

    attachSSE(jobId);
  } catch (e) {
    if (e?.name === "AbortError") {
      setBusy(false);
      setStatus("ยกเลิกแล้ว", "การดึงข้อมูลถูกยกเลิก");
      setMeta("ยกเลิกการดึงข้อมูล");
      return;
    }
    showError(e.message || String(e));
    setBusy(false);
    setStatus("Error!", "เปิด Console ดู log ได้");
    setMeta(`❌ ${e.message || String(e)}`);
  } finally {
    state.abortController = null;
  }
}

/* ---------------------------
 * Export
 * --------------------------- */
function exportCsv() {
  if (!state.jobId) return alert("ยังไม่มี jobId");
  window.location.href = `/api/export.csv?jobId=${encodeURIComponent(state.jobId)}`;
}

async function exportXlsx() {
  if (!state.jobId) return alert("ยังไม่มี jobId");

  const url = `/api/export.xlsx?jobId=${encodeURIComponent(state.jobId)}`;
  try {
    const r = await fetch(url, { method: "GET", cache: "no-store" });
    if (!r.ok) {
      const t = await r.text().catch(() => "");
      alert(`❌ Export XLSX ไม่สำเร็จ\n${t || `HTTP ${r.status}`}\n\nรอให้งาน status=done ก่อน แล้วลองใหม่`);
      return;
    }
    const blob = await r.blob();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `livinginsider_${state.jobId}.xlsx`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
  } catch (e) {
    alert(`❌ Export XLSX error: ${e.message || String(e)}`);
  }
}

async function copySheetsFormula() {
  if (!state.jobId) return alert("ยังไม่มี jobId");

  const formula = `=IMPORTDATA("${origin()}/api/export.csv?jobId=${encodeURIComponent(state.jobId)}")`;
  try {
    await navigator.clipboard.writeText(formula);
    alert("✅ คัดลอกสูตร IMPORTDATA สำเร็จ!\n\nวางใน Google Sheets cell A1");
  } catch {
    alert("❌ ไม่สามารถคัดลอกได้\n\n" + formula);
  }
}

/* ---------------------------
 * Clear / Stop / Preset
 * --------------------------- */
function clearData() {
  if (!confirm("ต้องการล้างข้อมูลทั้งหมด?")) return;

  state.rows = [];
  state.jobId = "";
  state.lastMeta = null;

  showError("");
  renderTable([]);
  updateExportButtons();

  hideDashboardSection();
  destroyDashboardCharts();

  setProgress(0, "พร้อมใช้งาน");
  setMeta("ล้างข้อมูลแล้ว");
}

function stopScrape() {
  cancelInFlight();
  showError("");
  setBusy(false);
  setStatus("หยุดแล้ว", "ยกเลิกการดึงข้อมูล");
  setMeta("ยกเลิกการดึงข้อมูล");
}

function usePresetDefault() {
  if (!els.startUrl) return;
  els.startUrl.value =
    "https://www.livinginsider.com/searchword/all/Buysell/1/%E0%B8%A3%E0%B8%A7%E0%B8%A1%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B8%81%E0%B8%A8%E0%B8%82%E0%B8%B2%E0%B8%A2-%E0%B9%80%E0%B8%8A%E0%B9%88%E0%B8%B2-%E0%B8%84%E0%B8%AD%E0%B8%99%E0%B9%82%E0%B8%94-%E0%B8%9A%E0%B9%89%E0%B8%B2%E0%B8%99-%E0%B8%97%E0%B8%B5%E0%B9%88%E0%B8%94%E0%B8%B4%E0%B8%99.html";
}

/* ---------------------------
 * Init
 * --------------------------- */
function cacheEls() {
  els = {
    startUrl: byId("startUrl"),
    dealType: byId("dealType"),
    category: byId("category"),
    keyword: byId("keyword"),
    priceMin: byId("priceMin"),
    priceMax: byId("priceMax"),
    maxPages: byId("maxPages"),
    maxResults: byId("maxResults"),
    sampleEvery: byId("sampleEvery"),
    preferFastMode: byId("preferFastMode"),

    btnSample: byId("btnSample"),
    btnRun: byId("btnRun"),
    btnStop: byId("btnStop"),
    btnExportCsv: byId("btnExportCsv"),
    btnExportXlsx: byId("btnExportXlsx"),
    btnCopySheets: byId("btnCopySheets"),
    btnClear: byId("btnClear"),
    btnPresetDefault: byId("btnPresetDefault"),

    meta: byId("meta"),
    statusText: byId("statusText"),
    statusHelp: byId("statusHelp"),
    statusError: byId("statusError"),
    progressText: byId("progressText"),
    progressBar: byId("progressBar"),

    resultHint: byId("resultHint"),
    thead: byId("thead"),
    tbody: byId("tbody"),
  };
}

function initEvents() {
  if (els.btnRun) els.btnRun.addEventListener("click", () => runScrape({ sample: false }));
  if (els.btnSample) els.btnSample.addEventListener("click", () => runScrape({ sample: true }));
  if (els.btnStop) els.btnStop.addEventListener("click", stopScrape);

  if (els.btnExportCsv) els.btnExportCsv.addEventListener("click", exportCsv);
  if (els.btnExportXlsx) els.btnExportXlsx.addEventListener("click", exportXlsx);
  if (els.btnCopySheets) els.btnCopySheets.addEventListener("click", copySheetsFormula);

  if (els.btnClear) els.btnClear.addEventListener("click", clearData);
  if (els.btnPresetDefault) els.btnPresetDefault.addEventListener("click", usePresetDefault);

  bindDashboardToggle();

  // keyboard shortcuts
  document.addEventListener("keydown", (e) => {
    if (state.isRunning) return;

    const tag = (document.activeElement?.tagName || "").toLowerCase();
    if (tag === "textarea" || tag === "input" || tag === "select") return;

    if (e.key === "Enter") {
      e.preventDefault();
      if (e.ctrlKey || e.metaKey) runScrape({ sample: false });
      else runScrape({ sample: true });
    }
  });
}

function init() {
  cacheEls();

  if (els.startUrl && !cleanText(els.startUrl.value)) usePresetDefault();

  initEvents();
  updateExportButtons();
  showError("");

  setProgress(0, "พร้อมใช้งาน");
  setMeta("กด Sample 10 หรือ Run Scrape เพื่อเริ่ม (เปิด Console ดู log ได้)");
  console.log("🎉 LivingInsider Scraper UI Ready");

  hideDashboardSection();
  destroyDashboardCharts();
}

/**
 * Global error guard — กัน “กดไม่ได้” แบบเงียบ ๆ เพราะ JS พัง
 */
window.addEventListener("error", (ev) => {
  const msg = ev?.error?.stack || ev?.message || String(ev);
  console.error("Window error:", msg);
  showError(msg);
});
window.addEventListener("unhandledrejection", (ev) => {
  const msg = ev?.reason?.stack || ev?.reason?.message || String(ev?.reason || ev);
  console.error("Unhandled rejection:", msg);
  showError(msg);
});

// ensure init runs (script defer ก็จริง แต่กันพลาดอีกชั้น)
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}

// export if needed
if (typeof window !== "undefined") {
  window.showDashboard = showDashboard;
}
