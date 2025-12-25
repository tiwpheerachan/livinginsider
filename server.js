// server.js (LATEST FIXED) — no 304 for /api/job + improved TTL + persistence guards + detailed logging
// ✅ Fixes “กราฟไม่ขึ้น” ที่เกิดจาก /api/job ได้ 304 (ETag/cache) ทำให้ client ไม่ได้ JSON ใหม่
// ✅ เพิ่ม Cache-Control: no-store เฉพาะ API ที่ต้องสด + ปิด ETag ทั้งระบบ
// ✅ คงโครงเดิมของคุณ (TTL, eviction, SSE ping, export CSV/XLSX) แต่เพิ่มส่วนสำคัญให้ครบ

import "dotenv/config";
import express from "express";
import cors from "cors";
import path from "path";
import { fileURLToPath } from "url";
import { v4 as uuidv4 } from "uuid";
import * as XLSX from "xlsx";

import scrapeListings from "./src/scraper.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// IMPORTANT: allow override via env PORT
const PORT = Number(process.env.PORT || 3000);

/** =========================
 *  Crash guards
 *  ========================= */
process.on("unhandledRejection", (err) => {
  console.error("UNHANDLED REJECTION:", err);
});
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

/** =========================
 *  Config
 *  ========================= */
const TTL_MS = Number(process.env.CACHE_TTL_MS || 60 * 60 * 1000); // 60 min
const MAX_CACHE_ITEMS = Number(process.env.CACHE_MAX_ITEMS || 100);
const JSON_LIMIT = String(process.env.JSON_LIMIT || "2mb");
const SSE_PING_MS = Number(process.env.SSE_PING_MS || 15_000);

/** =========================
 *  In-memory job store
 *  ========================= */
const jobs = new Map();

/** cleanup timer (TTL) */
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;

  for (const [jobId, j] of jobs.entries()) {
    if (now - j.createdAt > TTL_MS) {
      try {
        if (j.clients) {
          for (const res of j.clients) {
            try { res.end(); } catch {}
          }
        }
      } catch {}
      jobs.delete(jobId);
      cleaned++;
    }
  }

  if (cleaned > 0) {
    console.log(`[cleanup] Removed ${cleaned} expired jobs. Active: ${jobs.size}`);
  }
}, Math.min(TTL_MS / 2, 60_000)).unref();

/** Keep cache bounded */
function evictIfNeeded() {
  if (jobs.size <= MAX_CACHE_ITEMS) return;

  const entries = [...jobs.entries()].sort((a, b) => a[1].createdAt - b[1].createdAt);
  const removeN = Math.max(1, jobs.size - MAX_CACHE_ITEMS);

  console.log(`[evict] Removing ${removeN} oldest jobs to stay under ${MAX_CACHE_ITEMS}`);

  for (let i = 0; i < removeN; i++) {
    const [jobId, j] = entries[i];
    try {
      if (j.clients) {
        for (const res of j.clients) {
          try { res.end(); } catch {}
        }
      }
    } catch {}
    jobs.delete(jobId);
  }
}

function createJob(opts) {
  evictIfNeeded();

  const jobId = uuidv4();
  const now = Date.now();

  jobs.set(jobId, {
    createdAt: now,
    updatedAt: now,
    status: "running",
    opts,
    meta: {
      pagesVisited: 0,
      collected_links: 0,
      sampled_links: 0,
      total_parsed: 0,
      errors: [],
    },
    rows: [],
    error: null,
    clients: new Set(),
  });

  console.log(`[job] Created job ${jobId.slice(0, 8)} | Active: ${jobs.size}`);
  return jobId;
}

function getJob(jobId) {
  const j = jobs.get(jobId);
  if (!j) return null;

  if (Date.now() - j.createdAt > TTL_MS) {
    jobs.delete(jobId);
    return null;
  }
  return j;
}

/** =========================
 *  SSE helpers
 *  ========================= */
function sseSend(res, obj) {
  try {
    res.write(`data: ${JSON.stringify(obj)}\n\n`);
  } catch {}
}

function broadcast(jobId, obj) {
  const j = jobs.get(jobId);
  if (!j?.clients) return;
  for (const res of j.clients) sseSend(res, obj);
}

function closeAllClients(jobId) {
  const j = jobs.get(jobId);
  if (!j?.clients) return;
  for (const res of j.clients) {
    try { res.end(); } catch {}
  }
  j.clients.clear();
}

/** =========================
 *  Export helpers
 *  ========================= */
function toCsv(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return "\uFEFF";
  const headers = Object.keys(rows[0]);

  const esc = (val) => {
    if (val === null || val === undefined) return "";
    if (typeof val === "object") val = JSON.stringify(val);
    const s = String(val).replace(/\r?\n/g, " ").trim();
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };

  const lines = [headers.join(",")];
  for (const r of rows) lines.push(headers.map((h) => esc(r?.[h])).join(","));
  return "\uFEFF" + lines.join("\n");
}

function toXlsxBuffer(rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const ws = XLSX.utils.json_to_sheet(safeRows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "data");
  return XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
}

/** =========================
 *  Validate/normalize options
 *  ========================= */
function cleanStr(v) {
  return String(v ?? "").trim();
}
function clampInt(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}
function normalizeOpts(body) {
  const b = body && typeof body === "object" ? body : {};
  const startUrl = cleanStr(b.startUrl);
  if (!startUrl) throw new Error("startUrl is required");
  if (!/^https?:\/\//i.test(startUrl)) throw new Error("startUrl must start with http/https");

  return {
    startUrl,
    dealType: cleanStr(b.dealType),
    category: cleanStr(b.category),
    keyword: cleanStr(b.keyword),
    priceMin: cleanStr(b.priceMin),
    priceMax: cleanStr(b.priceMax),
    maxPages: clampInt(b.maxPages, 1, 200, 3),
    maxResults: clampInt(b.maxResults, 1, 5000, 50),
    sampleEvery: clampInt(b.sampleEvery, 1, 100, 1),
    preferFastMode: cleanStr(b.preferFastMode || b.fastMode || "auto"),
  };
}

/** =========================
 *  App
 *  ========================= */
const app = express();

// ✅ สำคัญมาก: ปิด ETag ทั้งระบบ (กัน 304 ที่ทำให้ client ไม่ได้ JSON)
app.set("etag", false);

app.use(cors());
app.use(express.json({ limit: JSON_LIMIT }));

/** =========================
 *  API no-cache middleware
 *  - ป้องกัน browser/cache กลืน response ที่ต้อง “สด”
 *  ========================= */
app.use((req, res, next) => {
  if (req.path.startsWith("/api/")) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    res.setHeader("Surrogate-Control", "no-store");
  }
  next();
});

// request logging (api only)
app.use((req, res, next) => {
  const t0 = Date.now();
  res.on("finish", () => {
    if (req.path.startsWith("/api/")) {
      const ms = Date.now() - t0;
      console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} ${res.statusCode} ${ms}ms`);
    }
  });
  next();
});

// static UI (cache ok)
app.use(express.static(path.join(__dirname, "public"), { maxAge: "1h" }));

app.get("/api/health", (_req, res) => {
  res.json({
    ok: true,
    time: new Date().toISOString(),
    port: PORT,
    ttl_ms: TTL_MS,
    jobs: jobs.size,
  });
});

/** =========================
 *  SSE Progress
 *  ========================= */
app.get("/api/scrape/progress/:jobId", (req, res) => {
  const jobId = String(req.params.jobId || "");
  const j = getJob(jobId);
  if (!j) {
    console.log(`[sse] Job ${jobId.slice(0, 8)} not found`);
    return res.status(404).end();
  }

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  j.clients.add(res);
  console.log(`[sse] Client connected to job ${jobId.slice(0, 8)} | clients=${j.clients.size}`);

  sseSend(res, {
    jobId,
    status: j.status,
    meta: j.meta,
    message: j.status === "running" ? "started" : "ready",
    ts: Date.now(),
  });

  const ping = setInterval(() => {
    try {
      res.write(`: ping ${Date.now()}\n\n`);
    } catch {}
  }, SSE_PING_MS);

  req.on("close", () => {
    clearInterval(ping);
    try {
      j.clients.delete(res);
      console.log(`[sse] Client disconnected from job ${jobId.slice(0, 8)} | clients=${j.clients.size}`);
    } catch {}
  });
});

/** =========================
 *  Run scrape
 *  - async default: returns {jobId}
 *  - sync=1: returns {jobId, rows, meta} (wait until done)
 *  ========================= */
app.post("/api/scrape", async (req, res) => {
  let jobId = "";
  try {
    const opts = normalizeOpts(req.body);
    jobId = createJob(opts);
    const j = jobs.get(jobId);

    const isSync = String(req.query.sync || "") === "1";

    // if async: respond immediately
    if (!isSync) {
      res.json({ jobId });
    }

    broadcast(jobId, { jobId, status: "running", message: "เริ่มสแครป…", meta: j?.meta, ts: Date.now() });

    const startedAt = Date.now();
    console.log(`[scrape] Starting job ${jobId.slice(0, 8)} | pages=${opts.maxPages} results=${opts.maxResults}`);

    const result = await scrapeListings({
      ...opts,
      onProgress: (p) => {
        const jj = jobs.get(jobId);
        if (!jj) return;

        const nextMeta = { ...(jj.meta || {}), ...(p?.meta || {}) };
        jj.updatedAt = Date.now();
        jj.meta = nextMeta;

        // Log progress periodically
        if (p?.stage === "detail" || (nextMeta.total_parsed % 5 === 0)) {
          console.log(
            `[progress] ${jobId.slice(0, 8)} | ${p?.stage || "unknown"} | parsed=${nextMeta.total_parsed} links=${nextMeta.collected_links}`
          );
        }

        broadcast(jobId, {
          jobId,
          status: "running",
          message: p?.message || p?.stage || "กำลังทำงาน…",
          meta: nextMeta,
          ts: Date.now(),
        });
      },
    });

    const elapsedMs = Date.now() - startedAt;

    if (j) {
      j.status = "done";
      j.rows = Array.isArray(result?.rows) ? result.rows : [];
      j.meta = { ...(j.meta || {}), ...(result?.meta || {}), elapsedMs };
      j.updatedAt = Date.now();

      broadcast(jobId, { jobId, status: "done", meta: j.meta, message: "เสร็จแล้ว", ts: Date.now() });
      closeAllClients(jobId);

      console.log(
        `✅ job ${jobId.slice(0, 8)} done | rows=${j.rows.length} | elapsed=${Math.round(elapsedMs / 1000)}s | errors=${
          j.meta?.errors?.length || 0
        }`
      );
    }

    // if sync: now return full payload
    if (isSync) {
      return res.json({ jobId, rows: j?.rows || [], meta: j?.meta || {} });
    }
  } catch (err) {
    console.error("❌ SCRAPE FAILED:", err);

    if (jobId && jobs.has(jobId)) {
      const j = jobs.get(jobId);
      j.status = "error";
      j.error = String(err?.stack || err?.message || err);
      j.updatedAt = Date.now();
      broadcast(jobId, { jobId, status: "error", error: j.error, meta: j.meta, ts: Date.now() });
      closeAllClients(jobId);
    }

    if (!res.headersSent) res.status(400).json({ error: String(err?.message || err) });
  }
});

/** =========================
 *  Fetch job result
 *  ✅ สำคัญ: route นี้ต้องไม่โดน 304 อีกต่อไป
 *  เพราะเราปิด ETag + บังคับ no-store แล้ว
 *  ========================= */
app.get("/api/job/:id", (req, res) => {
  const jobId = String(req.params.id || "");
  const j = getJob(jobId);

  if (!j) {
    console.log(`[job] Job ${jobId.slice(0, 8)} not found or expired`);
    return res.status(404).json({ error: "job not found or expired" });
  }

  // ✅ ใส่ header ซ้ำแบบ explicit เผื่อ proxy บางตัว
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.setHeader("Surrogate-Control", "no-store");

  res.json({
    jobId,
    status: j.status,
    meta: j.meta,
    error: j.error,
    rows: j.status === "done" ? j.rows : [],
  });
});

/** =========================
 *  Export CSV/XLSX
 *  ========================= */
app.get("/api/export.csv", (req, res) => {
  const jobId = cleanStr(req.query.jobId);
  const j = getJob(jobId);
  if (!j) return res.status(404).send("job not found or expired");
  if (j.status !== "done") return res.status(409).send("job not ready yet");

  const csv = toCsv(j.rows);
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="livinginsider_${jobId}.csv"`);
  res.send(csv);
});

app.get("/api/export.xlsx", (req, res) => {
  const jobId = cleanStr(req.query.jobId);
  const j = getJob(jobId);
  if (!j) return res.status(404).send("job not found or expired");
  if (j.status !== "done") return res.status(409).send("job not ready yet");

  const buf = toXlsxBuffer(j.rows);
  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.setHeader("Content-Disposition", `attachment; filename="livinginsider_${jobId}.xlsx"`);
  res.end(buf);
});

/** Root fallback */
app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

/** Listen */
app.listen(PORT, () => {
  console.log(`LivingInsider Scraper UI running on http://localhost:${PORT}`);
  console.log(`TTL: ${Math.round(TTL_MS / 60000)} minutes | Max cache: ${MAX_CACHE_ITEMS} jobs`);
  console.log(`API:
  - POST /api/scrape              (async, returns {jobId})
  - POST /api/scrape?sync=1       (sync, returns {jobId, rows, meta})
  - SSE  /api/scrape/progress/:jobId
  - GET  /api/job/:id
  - GET  /api/export.csv?jobId=...
  - GET  /api/export.xlsx?jobId=...`);
});
