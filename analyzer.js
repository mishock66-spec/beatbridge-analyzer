"use strict";

const { chromium } = require("playwright");
const Anthropic = require("@anthropic-ai/sdk");
const Airtable = require("airtable");

const MIN_DELAY_MS = 3000;
const MAX_DELAY_MS = 8000;
const PAUSE_EVERY = 15;
const PAUSE_MS = 5 * 60 * 1000;

const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "appW42oNhB9Hl14bq";
const AIRTABLE_TABLE_ID = process.env.AIRTABLE_TABLE_ID || "tbl0nVXbK5BQnU5FM";

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

async function scrapeProfile(page, username) {
  const url = `https://www.instagram.com/${username}/`;

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForTimeout(3000);

    const current = page.url();
    if (
      current.includes("/accounts/login") ||
      current.includes("/challenge") ||
      current.includes("/suspended")
    ) {
      throw new Error("RATE_LIMIT");
    }

    const bodyText = await page.evaluate(() => document.body?.innerText || "");
    if (
      bodyText.includes("Sorry, this page isn") ||
      bodyText.includes("This account is private")
    ) {
      return {
        skipped: true,
        reason: bodyText.includes("private") ? "Private account" : "Page not found",
      };
    }

    const data = await page.evaluate(() => {
      let bio = "";
      const metaDesc =
        document.querySelector('meta[name="description"]')?.content || "";

      const bioSelectors = [
        "header section > div > span",
        "header section > div:last-child span",
        "header section span[class]",
      ];
      for (const sel of bioSelectors) {
        for (const el of document.querySelectorAll(sel)) {
          const t = el.innerText?.trim() || "";
          if (
            t.length > 8 &&
            !t.match(/^\d/) &&
            !t.includes(" followers") &&
            !t.includes(" following") &&
            !t.includes(" posts")
          ) {
            bio = t;
            break;
          }
        }
        if (bio) break;
      }

      let followers = "",
        following = "",
        posts = "";
      const statItems = [
        ...document.querySelectorAll(
          "header ul li, header section ul li, header section li"
        ),
      ];
      for (const li of statItems) {
        const raw = li.innerText.replace(/\s+/g, " ").trim();
        if (/follow/.test(raw) && !/ following/.test(raw)) {
          const m = raw.match(/([\d,. ]+[KkMm]?)\s*follower/i);
          if (m) followers = m[1].trim();
        } else if (/following/.test(raw)) {
          const m = raw.match(/([\d,. ]+[KkMm]?)\s*following/i);
          if (m) following = m[1].trim();
        } else if (/post/.test(raw)) {
          const m = raw.match(/([\d,. ]+[KkMm]?)\s*post/i);
          if (m) posts = m[1].trim();
        }
      }

      if (!followers && metaDesc) {
        const fm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Follower/i);
        const fgm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Following/i);
        const pm = metaDesc.match(/([\d,.]+[KkMm]?)\s*Post/i);
        if (fm) followers = fm[1];
        if (fgm) following = fgm[1];
        if (pm) posts = pm[1];
      }

      const nameEl =
        document.querySelector("header h1") ||
        document.querySelector("header h2") ||
        document.querySelector("header section > div:first-child span");
      const fullName = nameEl?.innerText?.trim() || "";

      const postImgs = [
        ...document.querySelectorAll("article img[alt], main img[alt]"),
      ].slice(0, 12);
      const postCaptions = postImgs
        .map((img) => img.alt?.trim())
        .filter((t) => t && t.length > 5);

      return { bio, metaDesc, followers, following, posts, fullName, postCaptions };
    });

    return data;
  } catch (err) {
    if (err.message === "RATE_LIMIT") throw err;
    return { error: err.message };
  }
}

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
    // Retry without "analyzed" if the field doesn't exist yet
    if (err.message && err.message.includes("analyzed")) {
      const { analyzed: _analyzed, ...fieldsWithoutAnalyzed } = fields;
      await base(AIRTABLE_TABLE_ID).update([
        { id: recordId, fields: fieldsWithoutAnalyzed },
      ]);
    } else {
      throw err;
    }
  }
}

/**
 * Run a full analysis session.
 * @param {object[]} contacts  — array from the queue
 * @param {object}   job       — shared job state object (mutated in place)
 */
async function runAnalysis(contacts, job) {
  const anthropicClient = new Anthropic({
    apiKey: process.env.ANTHROPIC_API_KEY,
  });

  const airtableBase = new Airtable({
    apiKey: process.env.AIRTABLE_API_KEY,
  }).base(AIRTABLE_BASE_ID);

  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
      ],
    });
  } catch (err) {
    job.status = "failed";
    job.error = `Failed to launch browser: ${err.message}`;
    return;
  }

  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 900 },
  });

  const page = await context.newPage();
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    window.chrome = { runtime: {} };
  });

  job.status = "running";

  for (let i = 0; i < contacts.length; i++) {
    const contact = contacts[i];
    job.progress = i;

    // Long pause every PAUSE_EVERY profiles
    if (i > 0 && i % PAUSE_EVERY === 0) {
      console.log(`[analyzer] Pausing for ${PAUSE_MS / 60000}min after ${i} profiles...`);
      await new Promise((r) => setTimeout(r, PAUSE_MS));
    }

    console.log(`[analyzer] [${i + 1}/${contacts.length}] @${contact.username}`);

    try {
      const profileData = await scrapeProfile(page, contact.username);

      if (profileData.skipped) {
        job.skipped.push({ username: contact.username, reason: profileData.reason });
        await randomDelay();
        continue;
      }

      if (profileData.error) {
        job.errors.push({ username: contact.username, error: profileData.error });
        await randomDelay();
        continue;
      }

      const analysis = await analyzeWithClaude(anthropicClient, contact, profileData);
      await updateAirtable(airtableBase, contact.record_id, analysis, profileData.bio);

      job.completed.push({
        username: contact.username,
        old_type: contact.current_type,
        new_type: analysis.profile_type,
        confidence: analysis.confidence,
      });

      console.log(
        `[analyzer]   ✓  ${contact.current_type} → ${analysis.profile_type} [${analysis.confidence}]`
      );
    } catch (err) {
      if (err.message === "RATE_LIMIT") {
        console.log("[analyzer] Rate limit detected — stopping session.");
        job.status = "rate_limited";
        break;
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
  job.finishedAt = new Date().toISOString();
  console.log(
    `[analyzer] Session done — ${job.completed.length} processed, ${job.errors.length} errors`
  );
}

module.exports = { runAnalysis };
