"use strict";

const Anthropic = require("@anthropic-ai/sdk");
const Airtable = require("airtable");

const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "appW42oNhB9Hl14bq";
const AIRTABLE_TABLE_ID = process.env.AIRTABLE_TABLE_ID || "tbl0nVXbK5BQnU5FM";

// Field IDs for writes (consistent with analyzer.js)
const FIELD_PROFILE_TYPE = "fld8dCqjrnqCsRSog";
const FIELD_TEMPLATE     = "fldy8ho1lxBh8iB3n";
const FIELD_FOLLOW_UP    = "fldvT8Qq6LDFzcRgJ";

const FOLLOW_UP_TEXT = "Appreciate the reply — here it is: [LINK]";

// How many contacts to process per Claude call (Airtable also accepts up to 10 per update)
const CLAUDE_BATCH_SIZE = 10;

// batchSize >= this value → process everything (no limit)
const PROCESS_ALL_THRESHOLD = 9999;

// ── Airtable helpers ──────────────────────────────────────────────────────────

function fetchRecords(base, filterFormula, extraFields) {
  return new Promise((resolve, reject) => {
    const records = [];
    base(AIRTABLE_TABLE_ID)
      .select({
        filterByFormula: filterFormula,
        fields: ["Pseudo Instagram", "Suivi par", "Type de profil", "Notes", ...extraFields],
      })
      .eachPage(
        (page, fetchNext) => { records.push(...page); fetchNext(); },
        (err) => { if (err) reject(err); else resolve(records); }
      );
  });
}

// Update up to 10 records in a single Airtable call (classify + template + follow_up)
async function updateRecordsBatch(base, updates) {
  await base(AIRTABLE_TABLE_ID).update(
    updates.map((u) => ({
      id: u.id,
      fields: {
        [FIELD_PROFILE_TYPE]: u.profileType,
        [FIELD_TEMPLATE]:     u.template,
        [FIELD_FOLLOW_UP]:    FOLLOW_UP_TEXT,
      },
    }))
  );
}

// Template-only update — does NOT touch the profile type field (avoids select-value errors)
async function updateTemplatesOnlyBatch(base, updates) {
  await base(AIRTABLE_TABLE_ID).update(
    updates.map((u) => ({
      id: u.id,
      fields: {
        [FIELD_TEMPLATE]:  u.template,
        [FIELD_FOLLOW_UP]: FOLLOW_UP_TEXT,
      },
    }))
  );
}

// ── Claude Haiku: classify + generate templates for a batch ───────────────────

function buildClassifyPrompt(contacts, artist) {
  return `Classify these ${contacts.length} Instagram profiles and generate a DM template for each.
Return ONLY a valid JSON array with ${contacts.length} objects, no markdown fences:
[
  {
    "index": 0,
    "profile_type": "...",
    "template": "Hey [name], I'm [BEATMAKER_NAME]..."
  }
]

Artist network: ${artist}

Profiles:
${contacts.map((c, i) =>
  `${i}. username="${c.username}" bio="${(c.bio || "").slice(0, 150).replace(/"/g, "'")}" currentType="${c.profileType}"`
).join("\n")}

Profile types (pick ONE per profile):
Beatmaker/Producteur, Artiste/Rappeur, Ingé son, Manager, Label, DJ, Photographe/Vidéaste, Autre

Template format by type:
Beatmaker/Producteur: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Always looking to connect with producers who stay in the studio, think we could build something?"
Ingé son: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. The right mix makes all the difference, would love your ears on something I've been working on, think it could be worth your time?"
Manager: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got something that could make sense for your artists, think it's worth a listen?"
Label: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got some records that could make sense for your roster, think it's worth a listen?"
Artiste/Rappeur: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got a record I think fits your lane, think it could work?"
DJ: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got something that could hit different in a set, think it could fit your rotation?"
Photographe/Vidéaste: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. You capture the vibe, I make the sound, could be interesting to connect, think it could work?"
Autre: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. I noticed ${artist} follows you — I've been working on some records that I think could fit their sound. Would you be open to passing along a quick listen? I'd really appreciate it."

bioRef rules:
- Extract something SPECIFIC from bio if available (lowercase, half sentence)
- Strip "Bio: " prefix if present
- If no bio: use "caught your page through ${artist}'s network"
- [BEATMAKER_NAME] is a literal placeholder — NEVER replace it
- Never include a link in the template`;
}

async function classifyBatch(client, contacts, artist) {
  const message = await client.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 2048,
    messages: [{ role: "user", content: buildClassifyPrompt(contacts, artist) }],
  });

  const text = message.content?.[0]?.text || "";
  const clean = text.replace(/```(?:json)?/g, "").trim();
  const arrMatch = clean.match(/\[[\s\S]*\]/);
  if (!arrMatch) throw new Error(`No JSON array in response: ${text.slice(0, 300)}`);
  return JSON.parse(arrMatch[0]);
}

function buildTemplateOnlyPrompt(contacts, artist) {
  return `Generate a personalized DM template for each of these ${contacts.length} Instagram profiles.
The profile type is already set — use it to pick the right template format.
Return ONLY a valid JSON array with ${contacts.length} objects, no markdown fences:
[
  {
    "index": 0,
    "template": "Hey [name], I'm [BEATMAKER_NAME]..."
  }
]

Artist network: ${artist}

Profiles:
${contacts.map((c, i) =>
  `${i}. username="${c.username}" bio="${(c.bio || "").slice(0, 150).replace(/"/g, "'")}" profileType="${c.profileType}"`
).join("\n")}

Template format by type:
Beatmaker/Producteur: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Always looking to connect with producers who stay in the studio, think we could build something?"
Ingé son: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. The right mix makes all the difference, would love your ears on something I've been working on, think it could be worth your time?"
Manager: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got something that could make sense for your artists, think it's worth a listen?"
Label: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got some records that could make sense for your roster, think it's worth a listen?"
Artiste/Rappeur: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got a record I think fits your lane, think it could work?"
DJ: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. Got something that could hit different in a set, think it could fit your rotation?"
Photographe/Vidéaste: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. [bioRef]. You capture the vibe, I make the sound, could be interesting to connect, think it could work?"
Autre: "Hey [name], I'm [BEATMAKER_NAME], a beatmaker. I noticed ${artist} follows you — I've been working on some records that I think could fit their sound. Would you be open to passing along a quick listen? I'd really appreciate it."

bioRef rules:
- Extract something SPECIFIC from bio if available (lowercase, half sentence)
- Strip "Bio: " prefix if present
- If no bio: use "caught your page through ${artist}'s network"
- [BEATMAKER_NAME] is a literal placeholder — NEVER replace it
- Never include a link`;
}

async function generateTemplatesBatch(client, contacts, artist) {
  const message = await client.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 2048,
    messages: [{ role: "user", content: buildTemplateOnlyPrompt(contacts, artist) }],
  });

  const text = message.content?.[0]?.text || "";
  const clean = text.replace(/```(?:json)?/g, "").trim();
  const arrMatch = clean.match(/\[[\s\S]*\]/);
  if (!arrMatch) throw new Error(`No JSON array in response: ${text.slice(0, 300)}`);
  return JSON.parse(arrMatch[0]);
}

// ── Shared: run a batch loop over records ─────────────────────────────────────

async function processBatchLoop({ records, batchFn, buildUpdate, updateFn, job, label }) {
  const doUpdate = updateFn || updateRecordsBatch;
  let processed = 0;
  const errors = [];

  for (let i = 0; i < records.length; i += CLAUDE_BATCH_SIZE) {
    const batch = records.slice(i, i + CLAUDE_BATCH_SIZE);
    const batchNum = Math.floor(i / CLAUDE_BATCH_SIZE) + 1;

    if (job) {
      job.current = `@${batch[0].username}${batch.length > 1 ? ` +${batch.length - 1}` : ""}`;
    }

    let results;
    // ── Step 1: Claude API call ───────────────────────────────────────────────
    try {
      results = await batchFn(batch);
    } catch (claudeErr) {
      console.error(`[classifier] ✗ ${label} batch ${batchNum} — Claude error: ${claudeErr.message}`);
      for (const c of batch) {
        const errEntry = { username: c.username, error: `Claude: ${claudeErr.message}` };
        errors.push(errEntry);
        if (job) job.errors.push(errEntry);
      }
      if (job) job.progress += batch.length;
      if (i + CLAUDE_BATCH_SIZE < records.length) {
        await new Promise((r) => setTimeout(r, 500));
      }
      continue;
    }

    const updates = results
      .filter((r) => r && typeof r.index === "number" && batch[r.index])
      .map((r) => buildUpdate(r, batch[r.index]));

    // ── Step 2: Airtable update ───────────────────────────────────────────────
    if (updates.length > 0) {
      try {
        await doUpdate(batch[0].base, updates);
        processed += updates.length;
        if (job) {
          job.progress += updates.length;
          job.completed.push(
            ...updates.map((u) => ({ username: u._username, profileType: u.profileType }))
          );
          for (const u of updates) {
            job.changes.push({
              username: u._username,
              oldType:  u._oldType,
              newType:  u.profileType,
              template: u.template,
            });
          }
        }
        console.log(`[classifier] ✓ ${label} batch ${batchNum}: ${updates.length} done`);
      } catch (airtableErr) {
        console.error(
          `[classifier] ✗ ${label} batch ${batchNum} — Airtable update error: ${airtableErr.message}`,
          `records: ${updates.map((u) => u._username || u.id).join(", ")}`
        );
        for (const u of updates) {
          const errEntry = { username: u._username, error: `Airtable: ${airtableErr.message}` };
          errors.push(errEntry);
          if (job) job.errors.push(errEntry);
        }
        if (job) job.progress += updates.length;
      }
    }

    // Any contacts missing from Claude's response
    if (updates.length < batch.length) {
      for (let m = updates.length; m < batch.length; m++) {
        const c = batch[m];
        const errEntry = { username: c.username, error: "Missing from batch response" };
        errors.push(errEntry);
        if (job) { job.errors.push(errEntry); job.progress++; }
      }
    }

    // Pause between batches (not after last)
    if (i + CLAUDE_BATCH_SIZE < records.length) {
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  return { processed, errors };
}

// ── Main: classify unclassified contacts ──────────────────────────────────────

async function runClassifyAndGenerate(artist, batchSize, forceAll = false, job = null) {
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  const base   = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  const processAll = batchSize === 0 || batchSize >= PROCESS_ALL_THRESHOLD;
  console.log(`[classifier] classify-and-generate — artist=${artist} batchSize=${batchSize} forceAll=${forceAll} processAll=${processAll}`);

  const filter = forceAll
    ? `{Suivi par} = "${artist}"`
    : `AND({Suivi par} = "${artist}", {Type de profil} = "Autre")`;

  const allRecords = await fetchRecords(base, filter, []);
  const records    = processAll ? allRecords : allRecords.slice(0, batchSize);

  console.log(`[classifier] processing ${records.length} records (${allRecords.length} total matching)`);

  if (job) {
    job.total  = records.length;
    job.status = "running";
  }

  // Enrich each record with fields needed by the batch loop
  const contacts = records.map((r) => ({
    base,
    recordId:    r.id,
    username:    r.fields["Pseudo Instagram"] || r.id,
    bio:         (r.fields["Notes"] || "").replace(/^Bio:\s*/i, "").trim(),
    profileType: r.fields["Type de profil"] || "Autre",
  }));

  const { processed, errors } = await processBatchLoop({
    records: contacts,
    batchFn: (batch) => classifyBatch(client, batch, artist),
    buildUpdate: (r, c) => ({
      id:          c.recordId,
      profileType: r.profile_type || "Autre",
      template:    r.template || "",
      _username:   c.username,
      _oldType:    c.profileType,
    }),
    job,
    label: "classify",
  });

  if (job) {
    job.status     = "completed";
    job.current    = null;
    job.finishedAt = new Date().toISOString();
  }

  const remaining = processAll ? 0 : Math.max(0, allRecords.length - batchSize);
  return {
    processed,
    total: records.length,
    remaining,
    status: errors.length === 0 ? "done" : "partial",
    ...(errors.length > 0 ? { errors } : {}),
  };
}

// ── Main: generate templates for already-classified contacts ──────────────────

async function runGenerateTemplates(artist, batchSize, job = null) {
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  const base   = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  const processAll = batchSize === 0 || batchSize >= PROCESS_ALL_THRESHOLD;
  console.log(`[classifier] generate-templates — artist=${artist} batchSize=${batchSize} processAll=${processAll}`);

  const filter = `AND({Suivi par} = "${artist}", {Type de profil} != "Autre", {Type de profil} != "")`;
  const allRecords = await fetchRecords(base, filter, ["template"]);

  const needsTemplate = allRecords.filter((r) => {
    const tmpl = r.fields["template"] || r.fields[FIELD_TEMPLATE] || "";
    return !tmpl || tmpl.startsWith("Bio:");
  });

  const toProcess = processAll ? needsTemplate : needsTemplate.slice(0, batchSize);

  console.log(`[classifier] ${toProcess.length} records need template (${allRecords.length} total typed)`);

  if (job) {
    job.total  = toProcess.length;
    job.status = "running";
  }

  const contacts = toProcess.map((r) => ({
    base,
    recordId:    r.id,
    username:    r.fields["Pseudo Instagram"] || r.id,
    bio:         (r.fields["Notes"] || "").replace(/^Bio:\s*/i, "").trim(),
    profileType: r.fields["Type de profil"] || "Autre",
  }));

  const { processed, errors } = await processBatchLoop({
    records: contacts,
    batchFn: (batch) => generateTemplatesBatch(client, batch, artist),
    buildUpdate: (r, c) => ({
      id:          c.recordId,
      profileType: c.profileType,       // kept for job.changes tracking only
      template:    r.template || "",
      _username:   c.username,
      _oldType:    c.profileType,
    }),
    updateFn: updateTemplatesOnlyBatch,  // only writes template + follow_up (not profile type)
    job,
    label: "templates",
  });

  if (job) {
    job.status     = "completed";
    job.current    = null;
    job.finishedAt = new Date().toISOString();
  }

  const remaining = processAll
    ? 0
    : Math.max(0, needsTemplate.length - batchSize);

  return {
    processed,
    total: toProcess.length,
    remaining,
    status: errors.length === 0 ? "done" : "partial",
    ...(errors.length > 0 ? { errors } : {}),
  };
}

module.exports = { runClassifyAndGenerate, runGenerateTemplates };
