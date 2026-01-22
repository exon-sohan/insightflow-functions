/**
 * Batch API Status Checker Function
 * Timer trigger that polls Azure OpenAI Batch API for job completion
 * and processes results when ready
 */

const { app } = require("@azure/functions");
const { AzureOpenAI } = require("openai");
const { BlobServiceClient } = require("@azure/storage-blob");
const batchJobStorage = require("./batchJobStorage");

const AZURE_OPENAI_ENDPOINT = process.env.AZURE_AI_PROJECT_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT_NAME;
const AZURE_OPENAI_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const STORAGE_CONNECTION_STRING =
  process.env.AZURE_STORAGE_CONNECTION_STRING ||
  process.env.AzureWebJobsStorage;
const OUTPUT_CONTAINER = process.env.OUTPUT_CONTAINER || "output";

// Max retries before marking job as failed
const MAX_RETRIES = 3;

/* ───────────────────────────────────────────────────────────── */
/* LOGGER */
/* ───────────────────────────────────────────────────────────── */
function logger(context) {
  return {
    info: (msg, data) => context.log(`[INFO] ${msg}`, data ?? ""),
    success: (msg, data) => context.log(`[SUCCESS] ${msg}`, data ?? ""),
    warn: (msg, data) => context.warn(`[WARN] ${msg}`, data ?? ""),
    error: (msg, err) =>
      context.error(`[ERROR] ${msg}`, err?.stack || err || ""),
    debug: (msg, data) => context.log(`[DEBUG] ${msg}`, data ?? ""),
  };
}

/* ───────────────────────────────────────────────────────────── */
/* TIMER TRIGGER - Check Batch Status (every 5 minutes) */
/* ───────────────────────────────────────────────────────────── */
app.timer("batchApiStatusChecker", {
  schedule: "0 */5 * * * *", // Every 5 minutes
  handler: async (myTimer, context) => {
    const log = logger(context);
    const startTime = Date.now();

    log.info("════════════════════════════════════════════");
    log.info("Batch API Status Checker - Timer triggered");
    log.info(`Timestamp: ${new Date().toISOString()}`);
    log.info("════════════════════════════════════════════");

    try {
      // Validate config
      if (!AZURE_OPENAI_ENDPOINT || !AZURE_OPENAI_API_KEY) {
        log.warn("Azure OpenAI not configured, skipping status check");
        return;
      }

      // Initialize Azure OpenAI client
      const openAIClient = new AzureOpenAI({
        endpoint: AZURE_OPENAI_ENDPOINT,
        apiKey: AZURE_OPENAI_API_KEY,
        deployment: AZURE_OPENAI_DEPLOYMENT,
        apiVersion: "2024-10-21",
      });

      // Get all pending batch jobs
      const pendingJobs = await batchJobStorage.getPendingBatchJobs();
      log.info(`Found ${pendingJobs.length} pending batch job(s)`);

      if (pendingJobs.length === 0) {
        log.info("No pending jobs to check");
        return;
      }

      // Check status of each pending job
      for (const job of pendingJobs) {
        log.info(`Checking batch job: ${job.batchId}`);

        try {
          const batchStatus = await openAIClient.batches.retrieve(job.batchId);
          log.info(`Batch ${job.batchId} status: ${batchStatus.status}`);

          switch (batchStatus.status) {
            case "completed":
              log.success(`Batch ${job.batchId} completed!`);
              await processBatchResults(
                openAIClient,
                job,
                batchStatus,
                log
              );
              break;

            case "failed":
              log.error(`Batch ${job.batchId} failed`);
              await batchJobStorage.markBatchFailed(
                job.batchId,
                batchStatus.errors?.data?.[0]?.message || "Batch processing failed"
              );
              break;

            case "expired":
              log.error(`Batch ${job.batchId} expired (exceeded 24h window)`);
              await batchJobStorage.markBatchFailed(
                job.batchId,
                "Batch expired - exceeded 24 hour completion window"
              );
              break;

            case "cancelled":
              log.warn(`Batch ${job.batchId} was cancelled`);
              await batchJobStorage.markBatchFailed(
                job.batchId,
                "Batch was cancelled"
              );
              break;

            case "validating":
            case "in_progress":
            case "finalizing":
              log.info(`Batch ${job.batchId} still processing (${batchStatus.status})`);
              // Check if job is stuck (submitted more than 24 hours ago)
              const submittedAt = new Date(job.submittedAt);
              const hoursElapsed = (Date.now() - submittedAt.getTime()) / (1000 * 60 * 60);
              if (hoursElapsed > 25) {
                log.warn(`Batch ${job.batchId} has been processing for ${hoursElapsed.toFixed(1)} hours`);
              }
              break;

            default:
              log.warn(`Unknown batch status: ${batchStatus.status}`);
          }
        } catch (jobErr) {
          log.error(`Error checking batch ${job.batchId}`, jobErr);

          // Increment retry count
          const updatedJob = await batchJobStorage.incrementRetryCount(job.batchId);

          if (updatedJob.retryCount >= MAX_RETRIES) {
            log.error(`Batch ${job.batchId} exceeded max retries, marking as failed`);
            await batchJobStorage.markBatchFailed(
              job.batchId,
              `Failed after ${MAX_RETRIES} retries: ${jobErr.message}`
            );
          }
        }
      }

      log.success("Status check completed", {
        jobsChecked: pendingJobs.length,
        durationMs: Date.now() - startTime,
      });
    } catch (err) {
      log.error("Status checker failed", err);
      throw err;
    }
  },
});

/* ───────────────────────────────────────────────────────────── */
/* PROCESS BATCH RESULTS */
/* ───────────────────────────────────────────────────────────── */
async function processBatchResults(openAIClient, job, batchStatus, log) {
  log.info(`Processing results for batch ${job.batchId}`);

  try {
    // Download output file from Azure OpenAI
    const outputFileId = batchStatus.output_file_id;
    if (!outputFileId) {
      throw new Error("No output file ID in completed batch");
    }

    log.info(`Downloading output file: ${outputFileId}`);
    const fileResponse = await openAIClient.files.content(outputFileId);
    const outputContent = await fileResponse.text();

    // Parse JSONL results
    const results = parseJsonlResults(outputContent, log);
    log.info(`Parsed ${results.length} result(s) from batch output`);

    // Transform results to match existing output format
    const processedResults = results.map((result) => {
      if (result.error) {
        return {
          success: false,
          originalRecordId: result.custom_id,
          aiResponse: { error: result.error.message || "Processing failed" },
          processedAt: new Date().toISOString(),
        };
      }

      // Extract the AI response content
      const content = result.response?.body?.choices?.[0]?.message?.content;
      let insights;

      try {
        insights = content ? JSON.parse(content) : { parseError: true };
      } catch {
        insights = { rawResponse: content, parseError: true };
      }

      return {
        success: true,
        originalRecordId: result.custom_id,
        aiResponse: {
          insights,
          usage: result.response?.body?.usage,
        },
        processedAt: new Date().toISOString(),
      };
    });

    // Save results to output blob
    const outputBlobPath = await saveResultsToBlob(job, processedResults, log);

    // Mark batch as completed in tracking storage
    await batchJobStorage.markBatchCompleted(
      job.batchId,
      outputFileId,
      batchStatus.error_file_id
    );

    // Then mark as processed with output path
    await batchJobStorage.markBatchProcessed(job.batchId, outputBlobPath);

    log.success(`Batch ${job.batchId} results saved to ${outputBlobPath}`);

    // Log summary
    const successCount = processedResults.filter((r) => r.success).length;
    const failCount = processedResults.filter((r) => !r.success).length;
    log.info("Batch processing summary", {
      total: processedResults.length,
      success: successCount,
      failed: failCount,
    });
  } catch (err) {
    log.error(`Failed to process batch ${job.batchId} results`, err);
    await batchJobStorage.markBatchFailed(job.batchId, err.message);
    throw err;
  }
}

/* ───────────────────────────────────────────────────────────── */
/* PARSE JSONL RESULTS */
/* ───────────────────────────────────────────────────────────── */
function parseJsonlResults(jsonlContent, log) {
  const lines = jsonlContent.trim().split("\n");
  const results = [];

  for (const line of lines) {
    if (!line.trim()) continue;

    try {
      const result = JSON.parse(line);
      results.push(result);
    } catch (parseErr) {
      log.warn(`Failed to parse JSONL line: ${line.substring(0, 100)}...`);
    }
  }

  return results;
}

/* ───────────────────────────────────────────────────────────── */
/* SAVE RESULTS TO BLOB STORAGE */
/* ───────────────────────────────────────────────────────────── */
async function saveResultsToBlob(job, results, log) {
  if (!STORAGE_CONNECTION_STRING) {
    throw new Error("Storage connection string not configured");
  }

  const blobServiceClient = BlobServiceClient.fromConnectionString(
    STORAGE_CONNECTION_STRING
  );
  const containerClient = blobServiceClient.getContainerClient(OUTPUT_CONTAINER);
  await containerClient.createIfNotExists();

  // Extract original filename from input blob path
  const inputFileName = job.inputBlobPath.split("/").pop() || "unknown";
  const baseName = inputFileName.replace(/\.[^/.]+$/, "");
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outputBlobName = `${baseName}_batch_processed_${timestamp}.json`;

  const blockBlobClient = containerClient.getBlockBlobClient(outputBlobName);
  const content = JSON.stringify(results, null, 2);

  await blockBlobClient.upload(content, Buffer.byteLength(content), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  log.success(`Results saved to blob: ${outputBlobName}`);
  return `${OUTPUT_CONTAINER}/${outputBlobName}`;
}
