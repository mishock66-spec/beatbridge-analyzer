"use strict";

const express = require("express");
const crypto = require("crypto");
const { runAnalysis } = require("./analyzer");
const { runClassifyAndGenerate, runGenerateTemplates } = require("./classifier");

const app = express();

// CORS — allow requests from any origin (status polling from beatbridge.live)
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || 3000;
const ANALYZER_SECRET = process.env.ANALYZER_SECRET;

// In-memory job store (single-process; fine for this use case)
const jobs = new Map();

function generateJobId() {
  return crypto.randomBytes(8).toString("hex");
}

function validateSecret(req, res) {
  const { apiSecret } = req.body;
  if (!ANALYZER_SECRET) {
    res.status(500).json({ error: "ANALYZER_SECRET not configured on server" });
    return false;
  }
  if (!apiSecret || apiSecret !== ANALYZER_SECRET) {
    res.status(403).json({ error: "Invalid apiSecret" });
    return false;
  }
  return true;
}

// ── GET /health ───────────────────────────────────────────────────────────────

app.get("/health", (_req, res) => {
  res.json({ status: "ok", version: "1.0.0" });
});

// ── POST /analyze ─────────────────────────────────────────────────────────────

app.post("/analyze", (req, res) => {
  if (!validateSecret(req, res)) return;

  const { contacts } = req.body;

  if (!Array.isArray(contacts) || contacts.length === 0) {
    return res.status(400).json({ error: "contacts must be a non-empty array" });
  }

  if (contacts.length > 50) {
    return res.status(400).json({ error: "Maximum 50 contacts per job" });
  }

  // Validate each contact has required fields
  for (const c of contacts) {
    if (!c.username || !c.record_id) {
      return res
        .status(400)
        .json({ error: "Each contact must have username and record_id" });
    }
  }

  const jobId = generateJobId();
  const job = {
    jobId,
    status: "queued",
    total: contacts.length,
    progress: 0,
    current: null,   // username currently being analyzed
    completed: [],
    skipped: [],
    errors: [],
    startedAt: new Date().toISOString(),
    finishedAt: null,
  };

  jobs.set(jobId, job);

  // Start analysis in background (non-blocking)
  setImmediate(() => {
    runAnalysis(contacts, job).catch((err) => {
      job.status = "failed";
      job.error = err.message;
      console.error("[server] Unhandled analyzer error:", err);
    });
  });

  res.json({
    jobId,
    status: "started",
    total: contacts.length,
  });
});

// ── GET /status/:jobId ────────────────────────────────────────────────────────

app.get("/status/:jobId", (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  res.json({
    jobId: job.jobId,
    status: job.status,
    progress: job.progress,
    total: job.total,
    current: job.current,
    completed: job.completed.length,
    skipped: job.skipped.length,
    errors: job.errors.length,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    // Include details when job is finished
    ...(["completed", "failed", "rate_limited"].includes(job.status)
      ? {
          results: job.completed,
          errorDetails: job.errors,
          skippedDetails: job.skipped,
          changes: job.changes,
        }
      : {}),
  });
});

// ── POST /classify-and-generate ──────────────────────────────────────────────
// Starts a background job: fetches contacts where Type de profil = "Autre"
// (or all if forceAll), classifies + generates DM templates, updates Airtable.
// Returns { jobId, status: "started", total } immediately — poll /status/:jobId.

app.post("/classify-and-generate", (req, res) => {
  if (!validateSecret(req, res)) return;

  const { artist, batchSize, forceAll } = req.body;
  if (!artist || typeof artist !== "string") {
    return res.status(400).json({ error: "artist is required" });
  }

  const size = Number(batchSize) || 50;
  if (size < 1) {
    return res.status(400).json({ error: "batchSize must be at least 1 (use 9999 for Process All)" });
  }

  if (!process.env.ANTHROPIC_API_KEY) {
    return res.status(500).json({ error: "ANTHROPIC_API_KEY not configured on server" });
  }
  if (!process.env.AIRTABLE_API_KEY) {
    return res.status(500).json({ error: "AIRTABLE_API_KEY not configured on server" });
  }

  const jobId = generateJobId();
  const job = {
    jobId,
    status: "queued",
    total: 0,
    progress: 0,
    current: null,
    completed: [],
    skipped: [],
    errors: [],
    changes: [],
    startedAt: new Date().toISOString(),
    finishedAt: null,
  };

  jobs.set(jobId, job);

  setImmediate(() => {
    runClassifyAndGenerate(artist, size, forceAll === true, job).catch((err) => {
      job.status = "failed";
      job.error = err.message;
      job.finishedAt = new Date().toISOString();
      console.error("[classify-and-generate] Unhandled error:", err);
    });
  });

  res.json({ jobId, status: "started", total: 0 });
});

// ── POST /generate-templates ──────────────────────────────────────────────────
// Starts a background job: fetches contacts that have a profile type but no/bad
// template, generates DM templates, updates Airtable.
// Returns { jobId, status: "started", total } immediately — poll /status/:jobId.

app.post("/generate-templates", (req, res) => {
  if (!validateSecret(req, res)) return;

  const { artist, batchSize } = req.body;
  if (!artist || typeof artist !== "string") {
    return res.status(400).json({ error: "artist is required" });
  }

  const size = Number(batchSize) || 50;
  if (size < 1) {
    return res.status(400).json({ error: "batchSize must be at least 1 (use 9999 for Process All)" });
  }

  if (!process.env.ANTHROPIC_API_KEY) {
    return res.status(500).json({ error: "ANTHROPIC_API_KEY not configured on server" });
  }
  if (!process.env.AIRTABLE_API_KEY) {
    return res.status(500).json({ error: "AIRTABLE_API_KEY not configured on server" });
  }

  const jobId = generateJobId();
  const job = {
    jobId,
    status: "queued",
    total: 0,
    progress: 0,
    current: null,
    completed: [],
    skipped: [],
    errors: [],
    changes: [],
    startedAt: new Date().toISOString(),
    finishedAt: null,
  };

  jobs.set(jobId, job);

  setImmediate(() => {
    runGenerateTemplates(artist, size, job).catch((err) => {
      job.status = "failed";
      job.error = err.message;
      job.finishedAt = new Date().toISOString();
      console.error("[generate-templates] Unhandled error:", err);
    });
  });

  res.json({ jobId, status: "started", total: 0 });
});

// ── 404 fallback ──────────────────────────────────────────────────────────────

app.use((_req, res) => {
  res.status(404).json({ error: "Not found" });
});

// ── Start ─────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`BeatBridge Analyzer running on port ${PORT}`);
  if (!ANALYZER_SECRET) {
    console.warn("⚠️  ANALYZER_SECRET is not set — all requests will be rejected");
  }
  if (!process.env.ANTHROPIC_API_KEY) {
    console.warn("⚠️  ANTHROPIC_API_KEY is not set");
  }
  if (!process.env.AIRTABLE_API_KEY) {
    console.warn("⚠️  AIRTABLE_API_KEY is not set");
  }

  // Start Bull queue worker if Redis is configured
  if (process.env.REDIS_URL) {
    require("./worker");
    console.log("✅ Bull queue worker started");
  } else {
    console.warn("⚠️  REDIS_URL not set — Bull queue worker disabled");
  }
});
