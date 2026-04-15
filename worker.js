"use strict";

/**
 * Bull queue worker — runs alongside the Express server on Railway.
 * Picks up "process-contacts" jobs added by the Next.js admin panel.
 * Calls classifier functions directly (no HTTP round-trip to self).
 */

const Bull = require("bull");
const { runClassifyAndGenerate, runGenerateTemplates } = require("./classifier");

const REDIS_URL = process.env.REDIS_URL;

if (!REDIS_URL) {
  console.warn("[worker] REDIS_URL is not set — worker will not start");
  module.exports = {};
  return;
}

const processContactsQueue = new Bull("process-contacts", REDIS_URL, {
  defaultJobOptions: { attempts: 1 },
});

processContactsQueue.process(async (job) => {
  const { artist, action, batchSize, autoLoop } = job.data;
  const forceAll = action === "reclassify-all";

  let cumulativeProcessed = 0;
  let grandTotal = 0;
  let batchNumber = 0;

  console.log(
    `[worker] starting job ${job.id} — artist=${artist} action=${action} batchSize=${batchSize} autoLoop=${autoLoop} forceAll=${forceAll}`
  );

  do {
    batchNumber++;

    let result;
    try {
      if (action === "templates") {
        result = await runGenerateTemplates(artist, batchSize);
      } else {
        result = await runClassifyAndGenerate(artist, batchSize, forceAll);
      }
    } catch (err) {
      console.error(`[worker] batch ${batchNumber} error:`, err.message);
      throw err; // fail the job
    }

    cumulativeProcessed += result.processed ?? 0;
    if (grandTotal === 0) grandTotal = (result.total ?? 0) + (result.remaining ?? 0);

    const progress = {
      processed: cumulativeProcessed,
      total: grandTotal,
      remaining: result.remaining ?? 0,
      batchNumber,
      ...(result.errors?.length > 0 ? { errors: result.errors } : {}),
    };

    await job.progress(progress);
    console.log(
      `[worker] job ${job.id} batch ${batchNumber} done — processed=${cumulativeProcessed} remaining=${result.remaining}`
    );

    if ((result.remaining ?? 0) === 0 || !autoLoop) break;

    await new Promise((r) => setTimeout(r, 2000));
  } while (true);

  const final = { processed: cumulativeProcessed, total: grandTotal };
  console.log(`[worker] job ${job.id} complete:`, final);
  return final;
});

processContactsQueue.on("failed", (job, err) => {
  console.error(`[worker] job ${job.id} failed:`, err.message);
});

processContactsQueue.on("completed", (job, result) => {
  console.log(`[worker] job ${job.id} completed:`, result);
});

console.log("[worker] processContactsWorker listening on Redis queue…");

module.exports = { processContactsQueue };
