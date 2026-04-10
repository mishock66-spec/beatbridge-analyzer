"use strict";

const { chromium } = require("playwright");
const Anthropic = require("@anthropic-ai/sdk");
const Airtable = require("airtable");

const MIN_DELAY_MS = 15000;  // 15s minimum between profiles
const MAX_DELAY_MS = 25000;  // 25s maximum between profiles
const PAUSE_EVERY = 15;
const PAUSE_MS = 5 * 60 * 1000;
const RATE_LIMIT_COOLDOWN_MS = 60 * 1000; // 60s cooldown after rate limit
const MAX_CONSECUTIVE_RATE_LIMITS = 2;    // stop after 2 in a row
const INITIAL_WARMUP_MS = 5000; // wait before first profile

const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "appW42oNhB9Hl14bq";
const AIRTABLE_TABLE_ID = process.env.AIRTABLE_TABLE_ID || "tbl0nVXbK5BQnU5FM";

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const SYSTEM_PROMPT = `You are analyzing Instagram profiles for BeatBridge, a hip-hop networking platform for beatmakers.

Profile type options:
Beatmaker/Producteur, Artiste/Rappeur, Manager, Ingé son, Label, DJ, Studio, Autre

DM template rules:
- 1–2 sentences maximum
- Opens with: "Hey [name],"
- References something SPECIFIC from their profile (bio detail, content type, genre, highlights)
- Ends with exactly one of: "think we could build something?", "would love to connect.", or "open to hear your thoughts."
- NEVER includes a link or URL
- NEVER mentions the beatmaker's name directly — use the literal placeholder: [BEATMAKER_NAME]

Return ONLY valid JSON, nothing else:
{
  "profile_type": "...",
  "template": "Hey [name], I'm [BEATMAKER_NAME], a beatmaker — ...",
  "analysis_note": "One-line reason for the classification",
  "confidence": "high|medium|low"
}`;

function randomDelay() {
  const ms = Math.floor(Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS)) + MIN_DELAY_MS;
  return new Promise((r) => setTimeout(r, ms));
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Simulate brief human-like mouse movement
async function humanMouseMove(page) {
  try {
    await page.mouse.move(
      100 + Math.random() * 500,
      100 + Math.random() * 400
    );
  } catch {
    // non-fatal
  }
}

// Visit Instagram homepage to warm up / reset session state
async function visitHomepage(page) {
  try {
    await page.goto("https://www.instagram.com/", {
      waitUntil: "domcontentloaded",
      timeout: 15000,
    });
    await sleep(2000);
    await humanMouseMove(page);
  } catch {
    // non-fatal — continue even if homepage load fails
  }
}

// ── Scrape ────────────────────────────────────────────────────────────────────

async function scrapeProfile(page, username) {
  const url = `https://www.instagram.com/${username}/`;

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    // Wait for network to settle
    try {
      await page.waitForLoadState("networkidle", { timeout: 15000 });
    } catch {
      // non-fatal
    }

    await humanMouseMove(page);

    const current = page.url();

    if (
      current.includes("/accounts/login") ||
      current.includes("/challenge") ||
      current.includes("/suspended") ||
      current.includes("/privacy/checks")
    ) {
      throw new Error("RATE_LIMIT");
    }

    const bodyText = await page.evaluate(() => document.body?.innerText || "");
    if (
      bodyText.includes("We restrict certain activity") ||
      bodyText.includes("Let us know if you think") ||
      bodyText.includes("suspicious activity")
    ) {
      throw new Error("RATE_LIMIT");
    }

    if (
      bodyText.includes("Sorry, this page isn") ||
      bodyText.includes("This account is private")
    ) {
      return {
        skipped: true,
        reason: bodyText.includes("private") ? "Private account" : "Page not found",
      };
    }

    // Strategy 1: JSON-LD
    const jsonLdData = await page.evaluate(() => {
      const scripts = [...document.querySelectorAll('script[type="application/ld+json"]')];
      for (const s of scripts) {
        try {
          const d = JSON.parse(s.textContent || "");
          if (d["@type"] === "ProfilePage" || d.mainEntity) return d;
          if (Array.isArray(d)) {
            const profile = d.find((e) => e["@type"] === "ProfilePage" || e.mainEntity);
            if (profile) return profile;
          }
        } catch { /* skip */ }
      }
      return null;
    });

    // Strategy 2: meta description
    const metaDesc = await page.evaluate(
      () => document.querySelector('meta[name="description"]')?.content || ""
    );

    let bioFromMeta = "";
    const metaBioMatch = metaDesc.match(/\d+[^\-]+-\s*[^:]+:\s*(.+)/);
    if (metaBioMatch) bioFromMeta = metaBioMatch[1].trim();

    let followersFromMeta = "", followingFromMeta = "", postsFromMeta = "";
    const fm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Follower/i);
    const fgm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Following/i);
    const pm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Post/i);
    if (fm) followersFromMeta = fm[1];
    if (fgm) followingFromMeta = fgm[1];
    if (pm) postsFromMeta = pm[1];

    // Strategy 3: DOM
    const domData = await page.evaluate(() => {
      let bio = "";
      const bioSelectors = [
        "header section h1 ~ div span",
        "header section > div:last-child span",
        "header section > div > span",
        "header section span[class]",
        "header span",
      ];
      for (const sel of bioSelectors) {
        for (const el of document.querySelectorAll(sel)) {
          const t = el.innerText?.trim() || "";
          if (
            t.length > 8 &&
            !t.match(/^\d/) &&
            !t.includes(" followers") &&
            !t.includes(" following") &&
            !t.includes(" posts") &&
            !t.includes("Edit profile") &&
            !t.includes("Follow")
          ) {
            bio = t;
            break;
          }
        }
        if (bio) break;
      }

      let followers = "", following = "", posts = "";
      const liEls = [...document.querySelectorAll("header ul li, header section ul li, header section li")];
      for (const li of liEls) {
        const raw = li.innerText.replace(/\s+/g, " ").trim();
        if (/follower/i.test(raw) && !/following/i.test(raw)) {
          const m = raw.match(/([\d,. ]+[KkMm]?)\s*follower/i);
          if (m) followers = m[1].trim();
        } else if (/following/i.test(raw)) {
          const m = raw.match(/([\d,. ]+[KkMm]?)\s*following/i);
          if (m) following = m[1].trim();
        } else if (/post/i.test(raw)) {
          const m = raw.match(/([\d,. ]+[KkMm]?)\s*post/i);
          if (m) posts = m[1].trim();
        }
      }

      const nameEl =
        document.querySelector("header h2") ||
        document.querySelector("header h1") ||
        document.querySelector("header section > div:first-child span");
      const fullName = nameEl?.innerText?.trim() || "";

      const postImgs = [...document.querySelectorAll("article img[alt], main img[alt]")].slice(0, 12);
      const postCaptions = postImgs
        .map((img) => img.alt?.trim())
        .filter((t) => t && t.length > 5);

      return { bio, followers, following, posts, fullName, postCaptions };
    });

    return {
      bio: domData.bio || bioFromMeta,
      metaDesc,
      followers: domData.followers || followersFromMeta,
      following: domData.following || followingFromMeta,
      posts: domData.posts || postsFromMeta,
      fullName: domData.fullName || (jsonLdData?.mainEntity?.name) || "",
      postCaptions: domData.postCaptions,
    };

  } catch (err) {
    if (err.message === "RATE_LIMIT") throw err;
    return { error: err.message };
  }
}

// ── Claude analysis ───────────────────────────────────────────────────────────

async function analyzeWithClaude(client, contact, profileData) {
  const profileCtx = [
    `Username: @${contact.username}`,
    `Current type in database: ${contact.current_type}`,
    `Artist network: ${contact.artist}`,
    `Full name: ${profileData.fullName || "unknown"}`,
    `Bio: ${profileData.bio || "(empty)"}`,
    `Followers: ${profileData.followers || "unknown"}`,
    `Following: ${profileData.following || "unknown"}`,
    `Posts: ${profileData.posts || "unknown"}`,
    `Recent post captions (up to 6):`,
    ...(profileData.postCaptions
      ?.slice(0, 6)
      .map((c, i) => `  ${i + 1}. ${c.slice(0, 120)}`) || ["  (none visible)"]),
    `---`,
    `Current DM template: ${contact.current_template || "(none)"}`,
  ].join("\n");

  const message = await client.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 512,
    system: SYSTEM_PROMPT,
    messages: [{ role: "user", content: profileCtx }],
  });

  const text = message.content?.[0]?.text || "";
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) throw new Error(`No JSON in Claude response: ${text.slice(0, 200)}`);
  return JSON.parse(jsonMatch[0]);
}

// ── Airtable update ───────────────────────────────────────────────────────────
// Use field IDs to avoid name-mismatch errors.
// Field IDs (table tbl0nVXbK5BQnU5FM):
//   template       → fldy8ho1lxBh8iB3n
//   Type de profil → fld8dCqjrnqCsRSog
//   Notes          → fldpLozVCrvYj62i0

async function updateAirtable(base, recordId, analysis, existingBio) {
  const newBio = existingBio
    ? `${existingBio}\n— Analysis: ${analysis.analysis_note}`
    : `— Analysis: ${analysis.analysis_note}`;

  const fields = {};
  if (analysis.template)     fields["fldy8ho1lxBh8iB3n"] = analysis.template;
  if (analysis.profile_type) fields["fld8dCqjrnqCsRSog"]  = analysis.profile_type;
  fields["fldpLozVCrvYj62i0"] = newBio;
  fields["fldLRttkukXJiVs0u"] = true; // analyzed checkbox

  await base(AIRTABLE_TABLE_ID).update([{ id: recordId, fields }]);
  console.log(`[analyzer]   💾  Airtable updated OK (${recordId})`);
}

// ── Main session runner ───────────────────────────────────────────────────────

async function runAnalysis(contacts, job) {
  const anthropicClient = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  const airtableBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--disable-web-security",
        "--lang=en-US,en",
        `--user-agent=${USER_AGENT}`,
      ],
    });
  } catch (err) {
    job.status = "failed";
    job.error = `Failed to launch browser: ${err.message}`;
    return;
  }

  const context = await browser.newContext({
    userAgent: USER_AGENT,
    locale: "en-US",
    timezoneId: "America/New_York",
    viewport: { width: 1280, height: 800 },
    extraHTTPHeaders: {
      "Accept-Language": "en-US,en;q=0.9",
    },
  });

  const page = await context.newPage();

  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3] });
    window.chrome = { runtime: {} };
  });

  // Warm up: visit homepage with networkidle to fully load before starting
  console.log(`[analyzer] Warming up — visiting Instagram homepage (networkidle)...`);
  try {
    await page.goto("https://www.instagram.com/", { waitUntil: "networkidle", timeout: 30000 });
    await humanMouseMove(page);
  } catch {
    // non-fatal
  }
  const warmupMs = 3000 + Math.random() * 2000;
  console.log(`[analyzer] Waiting ${(warmupMs / 1000).toFixed(1)}s before first profile...`);
  await sleep(warmupMs);

  job.status = "running";
  let consecutiveRateLimits = 0;

  for (let i = 0; i < contacts.length; i++) {
    const contact = contacts[i];

    // Long pause every PAUSE_EVERY profiles
    if (i > 0 && i % PAUSE_EVERY === 0) {
      console.log(`[analyzer] Pausing for ${PAUSE_MS / 60000}min after ${i} profiles...`);
      await sleep(PAUSE_MS);
      // Re-warm homepage after long pause
      await visitHomepage(page);
    }

    job.current = `@${contact.username}`;
    console.log(`[analyzer] [${i + 1}/${contacts.length}] @${contact.username}`);

    // Visit homepage before each profile to mimic human navigation
    await visitHomepage(page);

    try {
      const profileData = await scrapeProfile(page, contact.username);

      // Reset rate-limit streak on a successful page load
      consecutiveRateLimits = 0;

      if (profileData.skipped) {
        console.log(`[analyzer]   ⏭  Skipped: ${profileData.reason}`);
        job.skipped.push({ username: contact.username, reason: profileData.reason });
        job.progress = i + 1;
        await randomDelay();
        continue;
      }

      if (profileData.error) {
        console.log(`[analyzer]   ✗  Scrape error: ${profileData.error}`);
        job.errors.push({ username: contact.username, error: profileData.error });
        job.progress = i + 1;
        await randomDelay();
        continue;
      }

      console.log(`[analyzer]      Bio: ${(profileData.bio || "(none)").slice(0, 80)}`);
      console.log(`[analyzer]      Followers: ${profileData.followers || "?"}, Posts: ${profileData.posts || "?"}`);

      // Claude analysis
      console.log(`[analyzer]   🤖  Sending to Claude Haiku...`);
      const analysis = await analyzeWithClaude(anthropicClient, contact, profileData);
      console.log(`[analyzer]   🤖  Claude: ${contact.current_type} → ${analysis.profile_type} [${analysis.confidence}]`);

      // Airtable update
      try {
        await updateAirtable(airtableBase, contact.record_id, analysis, profileData.bio);
        job.completed.push({
          username: contact.username,
          old_type: contact.current_type,
          new_type: analysis.profile_type,
          confidence: analysis.confidence,
        });
        console.log(`[analyzer]   ✓  Done. completed=${job.completed.length + 1}`);
      } catch (airtableErr) {
        console.error(`[analyzer]   ❌  Airtable update failed for @${contact.username}: ${airtableErr.message}`);
        job.errors.push({ username: contact.username, error: `Airtable: ${airtableErr.message}` });
      }
      job.progress = i + 1;
      console.log(`[analyzer]      progress=${job.progress}/${contacts.length}`);

    } catch (err) {
      if (err.message === "RATE_LIMIT") {
        consecutiveRateLimits++;
        console.log(
          `[analyzer]   ⚠️  Rate limit on @${contact.username} (${consecutiveRateLimits}/${MAX_CONSECUTIVE_RATE_LIMITS} consecutive)`
        );

        job.skipped.push({ username: contact.username, reason: "Rate limited" });
        job.progress = i + 1;

        if (consecutiveRateLimits >= MAX_CONSECUTIVE_RATE_LIMITS) {
          console.log("[analyzer] Too many consecutive rate limits — stopping session.");
          job.status = "rate_limited";
          break;
        }

        // Cool down, then visit homepage before retrying
        console.log(`[analyzer]   ⏳  Cooling down ${RATE_LIMIT_COOLDOWN_MS / 1000}s...`);
        await sleep(RATE_LIMIT_COOLDOWN_MS);
        console.log(`[analyzer]   🏠  Visiting homepage before next profile...`);
        await visitHomepage(page);
        continue;
      }

      console.error(`[analyzer]   ✗  Unhandled error: ${err.message}`);
      job.errors.push({ username: contact.username, error: err.message });
      job.progress = i + 1;  // always advance progress, even on error
    }

    if (i < contacts.length - 1) {
      const delayMs = Math.floor(Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS)) + MIN_DELAY_MS;
      console.log(`[analyzer]   ⏳  Waiting ${(delayMs / 1000).toFixed(1)}s...`);
      await sleep(delayMs);
    }
  }

  await browser.close();

  if (job.status === "running") {
    job.status = "completed";
  }
  job.progress = contacts.length;
  job.current = null;
  job.finishedAt = new Date().toISOString();
  console.log(
    `[analyzer] ════ Session done — completed=${job.completed.length} skipped=${job.skipped.length} errors=${job.errors.length} ════`
  );
}

module.exports = { runAnalysis };
