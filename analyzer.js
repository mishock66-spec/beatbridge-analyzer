"use strict";

const { chromium } = require("playwright");
const Anthropic = require("@anthropic-ai/sdk");
const Airtable = require("airtable");

const MIN_DELAY_MS = 3000;
const MAX_DELAY_MS = 8000;
const PAUSE_EVERY = 15;
const PAUSE_MS = 5 * 60 * 1000;
const RATE_LIMIT_COOLDOWN_MS = 30 * 1000; // wait 30s then continue
const MAX_CONSECUTIVE_RATE_LIMITS = 3;    // stop session after 3 in a row

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

// ── Scrape ────────────────────────────────────────────────────────────────────

async function scrapeProfile(page, username) {
  const url = `https://www.instagram.com/${username}/`;

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    // Wait for network to settle — catches lazy-loaded content
    try {
      await page.waitForLoadState("networkidle", { timeout: 15000 });
    } catch {
      // networkidle timeout is non-fatal — just proceed
    }

    const current = page.url();

    // Rate-limit / challenge signals
    if (
      current.includes("/accounts/login") ||
      current.includes("/challenge") ||
      current.includes("/suspended") ||
      current.includes("/privacy/checks")
    ) {
      throw new Error("RATE_LIMIT");
    }

    // Check body text for block indicators
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

    // ── Strategy 1: JSON-LD structured data ──────────────────────────────────
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
        } catch { /* skip malformed */ }
      }
      return null;
    });

    // ── Strategy 2: meta[name="description"] — usually has followers + bio ───
    const metaDesc = await page.evaluate(
      () => document.querySelector('meta[name="description"]')?.content || ""
    );

    // Parse bio from meta description
    // Format: "X Followers, Y Following, Z Posts - See Instagram photos..."
    // or:     "X Followers, Y Following, Z Posts - username: bio text here"
    let bioFromMeta = "";
    const metaBioMatch = metaDesc.match(/\d+[^\-]+-\s*[^:]+:\s*(.+)/);
    if (metaBioMatch) bioFromMeta = metaBioMatch[1].trim();

    // Parse follower counts from meta description
    let followersFromMeta = "", followingFromMeta = "", postsFromMeta = "";
    const fm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Follower/i);
    const fgm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Following/i);
    const pm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Post/i);
    if (fm) followersFromMeta = fm[1];
    if (fgm) followingFromMeta = fgm[1];
    if (pm) postsFromMeta = pm[1];

    // ── Strategy 3: DOM extraction ────────────────────────────────────────────
    const domData = await page.evaluate(() => {
      let bio = "";
      const bioSelectors = [
        // Modern Instagram bio span
        "header section h1 ~ div span",
        "header section > div:last-child span",
        "header section > div > span",
        "header section span[class]",
        // Fallback: any non-stat span in header
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

      // Stats from <ul> in header
      let followers = "", following = "", posts = "";
      const liEls = [
        ...document.querySelectorAll("header ul li, header section ul li, header section li"),
      ];
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

      // Full name from header
      const nameEl =
        document.querySelector("header h2") ||
        document.querySelector("header h1") ||
        document.querySelector("header section > div:first-child span");
      const fullName = nameEl?.innerText?.trim() || "";

      // Post captions from img alt attributes
      const postImgs = [
        ...document.querySelectorAll("article img[alt], main img[alt]"),
      ].slice(0, 12);
      const postCaptions = postImgs
        .map((img) => img.alt?.trim())
        .filter((t) => t && t.length > 5);

      return { bio, followers, following, posts, fullName, postCaptions };
    });

    // Merge strategies: DOM > meta fallbacks
    const bio = domData.bio || bioFromMeta;
    const followers = domData.followers || followersFromMeta;
    const following = domData.following || followingFromMeta;
    const posts = domData.posts || postsFromMeta;
    const fullName = domData.fullName ||
      (jsonLdData?.mainEntity?.name) || "";

    return {
      bio,
      metaDesc,
      followers,
      following,
      posts,
      fullName,
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

async function updateAirtable(base, recordId, analysis, existingBio) {
  const newBio = existingBio
    ? `${existingBio}\n— Analysis: ${analysis.analysis_note}`
    : `— Analysis: ${analysis.analysis_note}`;

  const fields = {
    template: analysis.template,
    "Type de profil": analysis.profile_type,
    Notes: newBio,
    analyzed: true,
  };

  try {
    await base(AIRTABLE_TABLE_ID).update([{ id: recordId, fields }]);
  } catch (err) {
    if (err.message && err.message.includes("analyzed")) {
      const { analyzed: _analyzed, ...fieldsWithoutAnalyzed } = fields;
      await base(AIRTABLE_TABLE_ID).update([{ id: recordId, fields: fieldsWithoutAnalyzed }]);
    } else {
      throw err;
    }
  }
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

  // Mask all automation signals
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3] });
    window.chrome = { runtime: {} };
  });

  job.status = "running";
  let consecutiveRateLimits = 0;

  for (let i = 0; i < contacts.length; i++) {
    const contact = contacts[i];
    job.progress = i;

    // Long pause every PAUSE_EVERY profiles
    if (i > 0 && i % PAUSE_EVERY === 0) {
      console.log(`[analyzer] Pausing for ${PAUSE_MS / 60000}min after ${i} profiles...`);
      await sleep(PAUSE_MS);
    }

    job.current = `@${contact.username}`;
    console.log(`[analyzer] [${i + 1}/${contacts.length}] @${contact.username}`);

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

      const analysis = await analyzeWithClaude(anthropicClient, contact, profileData);
      await updateAirtable(airtableBase, contact.record_id, analysis, profileData.bio);

      job.completed.push({
        username: contact.username,
        old_type: contact.current_type,
        new_type: analysis.profile_type,
        confidence: analysis.confidence,
      });
      job.progress = i + 1;

      console.log(`[analyzer]   ✓  ${contact.current_type} → ${analysis.profile_type} [${analysis.confidence}]`);

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

        // Cool down then continue with next profile
        console.log(`[analyzer]   ⏳  Cooling down ${RATE_LIMIT_COOLDOWN_MS / 1000}s before next profile...`);
        await sleep(RATE_LIMIT_COOLDOWN_MS);
        continue;
      }

      console.error(`[analyzer]   ✗  ${err.message}`);
      job.errors.push({ username: contact.username, error: err.message });
    }

    if (i < contacts.length - 1) {
      await randomDelay();
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
    `[analyzer] Session done — ${job.completed.length} processed, ${job.skipped.length} skipped, ${job.errors.length} errors`
  );
}

module.exports = { runAnalysis };
