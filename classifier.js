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

// ── DM template builders by profile type ──────────────────────────────────────

const TEMPLATE_BUILDERS = {
  "Beatmaker/Producteur": (ref) =>
    `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. ${ref}. Always looking to connect with producers who stay in the studio, think we could build something?`,
  "Ingé son": (ref) =>
    `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. ${ref}. The right mix makes all the difference, would love your ears on something I've been working on, think it could be worth your time?`,
  "Manager": (ref) =>
    `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. ${ref}. Got something that could make sense for your artists, think it's worth a listen?`,
  "Label": (ref) =>
    `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. ${ref}. Got some records that could make sense for your roster, think it's worth a listen?`,
  "Artiste/Rappeur": (ref) =>
    `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. ${ref}. Got a record I think fits your lane, think it could work?`,
  "DJ": (ref) =>
    `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. ${ref}. Got something that could hit different in a set, think it could fit your rotation?`,
  "Photographe/Vidéaste": (ref) =>
    `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. ${ref}. You capture the vibe, I make the sound, could be interesting to connect, think it could work?`,
};

function buildTemplate(profileType, bioRef, artist) {
  const ref = bioRef || `caught your page through ${artist}'s network`;
  const builder = TEMPLATE_BUILDERS[profileType];
  if (!builder) {
    // "Autre" or unknown type — generic template
    return `Hey [name], I'm [BEATMAKER_NAME], a beatmaker. I noticed ${artist} follows you — I've been working on some records that I think could fit their sound. Would you be open to passing along a quick listen? I'd really appreciate it.`;
  }
  return builder(ref);
}

// ── Claude Haiku: classify profile + extract bio reference ───────────────────

const CLASSIFY_SYSTEM = `You classify Instagram contacts for BeatBridge, a hip-hop networking platform, and extract a personalized bio reference for outreach DMs.

PROFILE TYPE OPTIONS (pick exactly one):
Beatmaker/Producteur, Artiste/Rappeur, Ingé son, Manager, Label, DJ, Photographe/Vidéaste, Autre

BIO REFERENCE RULES:
- Extract ONE specific, natural detail from the bio that fits mid-sentence (e.g. "saw you mix trap and afrobeats", "noticed you manage artists in Atlanta")
- Strip any leading "Bio: " from the input before analyzing
- Keep it concise — half a sentence, lowercase
- If the bio is empty, vague, or not music-related: return null

Return ONLY valid JSON, nothing else:
{
  "profile_type": "...",
  "bioRef": "..." or null
}`;

async function classifyContact(client, username, notes, artist) {
  const rawBio = notes ? notes.replace(/^Bio:\s*/i, "").trim() : "";

  const userContent = [
    `Username: @${username}`,
    `Artist network: ${artist}`,
    `Bio: ${rawBio || "(none)"}`,
  ].join("\n");

  const message = await client.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 256,
    system: CLASSIFY_SYSTEM,
    messages: [{ role: "user", content: userContent }],
  });

  const text = message.content?.[0]?.text || "";
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) throw new Error(`No JSON in response: ${text.slice(0, 200)}`);
  return JSON.parse(jsonMatch[0]);
}

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

async function updateRecord(base, recordId, profileType, template) {
  await base(AIRTABLE_TABLE_ID).update([{
    id: recordId,
    fields: {
      [FIELD_PROFILE_TYPE]: profileType,
      [FIELD_TEMPLATE]:     template,
      [FIELD_FOLLOW_UP]:    FOLLOW_UP_TEXT,
    },
  }]);
}

// ── Main: classify unclassified contacts ──────────────────────────────────────

async function runClassifyAndGenerate(artist, batchSize, forceAll = false, job = null) {
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  console.log(`[classifier] classify-and-generate — artist=${artist} batchSize=${batchSize} forceAll=${forceAll}`);

  const filter = forceAll
    ? `{Suivi par} = "${artist}"`
    : `AND({Suivi par} = "${artist}", {Type de profil} = "Autre")`;
  const allRecords = await fetchRecords(base, filter, []);
  const records = allRecords.slice(0, batchSize);

  console.log(`[classifier] fetched ${records.length} records (${allRecords.length} total matching)`);

  // Update job with real total now that we know it
  if (job) {
    job.total = records.length;
    job.status = "running";
  }

  let processed = 0;
  const errors = [];

  for (const record of records) {
    const username = record.fields["Pseudo Instagram"] || record.id;
    const notes    = record.fields["Notes"] || "";

    if (job) job.current = `@${username}`;

    try {
      const result = await classifyContact(client, username, notes, artist);
      const profileType = result.profile_type || "Autre";
      const template    = buildTemplate(profileType, result.bioRef || null, artist);

      await updateRecord(base, record.id, profileType, template);
      processed++;
      if (job) {
        job.progress++;
        job.completed.push({ username, profileType });
      }
      console.log(`[classifier] ✓ @${username} → ${profileType}`);
    } catch (err) {
      console.error(`[classifier] ✗ @${username}: ${err.message}`);
      errors.push({ username, error: err.message });
      if (job) job.errors.push({ username, error: err.message });
    }

    // Brief pause between Claude calls
    await new Promise((r) => setTimeout(r, 300));
  }

  if (job) {
    job.status = "completed";
    job.current = null;
    job.finishedAt = new Date().toISOString();
  }

  const remaining = Math.max(0, allRecords.length - batchSize);
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
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  console.log(`[classifier] generate-templates — artist=${artist} batchSize=${batchSize}`);

  // Fetch contacts that have a profile type set (not "Autre", not empty)
  const filter = `AND({Suivi par} = "${artist}", {Type de profil} != "Autre", {Type de profil} != "")`;
  const allRecords = await fetchRecords(base, filter, ["template"]);

  // Filter in JS: keep only records with no template or template starting with "Bio:"
  const toProcess = allRecords
    .filter((r) => {
      const tmpl = r.fields["template"] || r.fields[FIELD_TEMPLATE] || "";
      return !tmpl || tmpl.startsWith("Bio:");
    })
    .slice(0, batchSize);

  console.log(`[classifier] ${toProcess.length} records need template (${allRecords.length} total typed)`);

  // Update job with real total
  if (job) {
    job.total = toProcess.length;
    job.status = "running";
  }

  let processed = 0;
  const errors = [];

  for (const record of toProcess) {
    const username    = record.fields["Pseudo Instagram"] || record.id;
    const notes       = record.fields["Notes"] || "";
    const profileType = record.fields["Type de profil"] || "Autre";

    if (job) job.current = `@${username}`;

    try {
      // Get bioRef via Haiku (no reclassification — keep existing type)
      const result  = await classifyContact(client, username, notes, artist);
      const template = buildTemplate(profileType, result.bioRef || null, artist);

      await updateRecord(base, record.id, profileType, template);
      processed++;
      if (job) {
        job.progress++;
        job.completed.push({ username, profileType });
      }
      console.log(`[classifier] ✓ @${username} (${profileType}) → template generated`);
    } catch (err) {
      console.error(`[classifier] ✗ @${username}: ${err.message}`);
      errors.push({ username, error: err.message });
      if (job) job.errors.push({ username, error: err.message });
    }

    await new Promise((r) => setTimeout(r, 300));
  }

  if (job) {
    job.status = "completed";
    job.current = null;
    job.finishedAt = new Date().toISOString();
  }

  const remaining = Math.max(0, allRecords.filter((r) => {
    const tmpl = r.fields["template"] || r.fields[FIELD_TEMPLATE] || "";
    return !tmpl || tmpl.startsWith("Bio:");
  }).length - batchSize);

  return {
    processed,
    total: toProcess.length,
    remaining: Math.max(0, remaining),
    status: errors.length === 0 ? "done" : "partial",
    ...(errors.length > 0 ? { errors } : {}),
  };
}

module.exports = { runClassifyAndGenerate, runGenerateTemplates };
