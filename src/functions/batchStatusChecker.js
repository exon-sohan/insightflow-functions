/**
 * InsightFlow - Batch Status Checker
 * Timer trigger to poll batch jobs, process results, and trigger ADF WriteBack
 */

const { app } = require("@azure/functions");
const { AzureOpenAI } = require("openai");
const { BlobServiceClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");
const batchJobStorage = require("./batchJobStorage");

const AZURE_OPENAI_ENDPOINT = process.env.AZURE_AI_PROJECT_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT_NAME;
const AZURE_OPENAI_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING || process.env.AzureWebJobsStorage;
const OUTPUT_CONTAINER = process.env.OUTPUT_CONTAINER || "output";
const MAX_RETRIES = 3;

// ADF configuration for triggering WriteBack pipeline
const SUBSCRIPTION_ID = process.env.AZURE_SUBSCRIPTION_ID;
const RESOURCE_GROUP = process.env.AZURE_RESOURCE_GROUP;
const DATA_FACTORY_NAME = process.env.DYNAMIC_DATA_FACTORY_NAME || process.env.DATA_FACTORY_NAME;
const WRITEBACK_PIPELINE_NAME = "SalesforceWriteBackPipeline";

/* ─────────────────────────────────────────────────────────────── */
/* TIMER TRIGGER - Check Batch Status (every 5 min)                */
/* ─────────────────────────────────────────────────────────────── */
app.timer("batchStatusChecker", {
  schedule: "0 */5 * * * *",
  handler: async (timer, context) => {
    context.log("Batch status checker running");

    try {
      if (!AZURE_OPENAI_ENDPOINT || !AZURE_OPENAI_API_KEY) {
        context.log("Azure OpenAI not configured, skipping");
        return;
      }

      const openAIClient = new AzureOpenAI({
        endpoint: AZURE_OPENAI_ENDPOINT,
        apiKey: AZURE_OPENAI_API_KEY,
        deployment: AZURE_OPENAI_DEPLOYMENT,
        apiVersion: "2024-10-21",
      });

      const pendingJobs = await batchJobStorage.getPendingJobs();
      context.log(`Found ${pendingJobs.length} pending jobs`);

      if (pendingJobs.length === 0) return;

      for (const job of pendingJobs) {
        try {
          const batchStatus = await openAIClient.batches.retrieve(job.batchId);
          context.log(`Batch ${job.batchId}: ${batchStatus.status}`);

          switch (batchStatus.status) {
            case "completed":
              await processResults(openAIClient, job, batchStatus, context);
              break;

            case "failed":
              await batchJobStorage.markFailed(job.batchId, batchStatus.errors?.data?.[0]?.message || "Failed");
              break;

            case "expired":
            case "cancelled":
              await batchJobStorage.markFailed(job.batchId, `Batch ${batchStatus.status}`);
              break;

            case "validating":
            case "in_progress":
            case "finalizing":
              // Still processing
              const hours = (Date.now() - new Date(job.submittedAt).getTime()) / 3600000;
              if (hours > 25) {
                context.warn(`Batch ${job.batchId} running for ${hours.toFixed(1)}h`);
              }
              break;
          }
        } catch (err) {
          context.error(`Error checking ${job.batchId}:`, err.message);
          const updated = await batchJobStorage.incrementRetry(job.batchId);
          if (updated.retryCount >= MAX_RETRIES) {
            await batchJobStorage.markFailed(job.batchId, `Max retries: ${err.message}`);
          }
        }
      }
    } catch (err) {
      context.error("Status checker failed:", err);
      throw err;
    }
  },
});

/* ─────────────────────────────────────────────────────────────── */
/* PROCESS BATCH RESULTS                                           */
/* ─────────────────────────────────────────────────────────────── */
async function processResults(openAIClient, job, batchStatus, context) {
  context.log(`Processing results for ${job.batchId}`);

  const outputFileId = batchStatus.output_file_id;
  if (!outputFileId) throw new Error("No output file ID");

  // Download results from OpenAI
  const fileResponse = await openAIClient.files.content(outputFileId);
  const outputContent = await fileResponse.text();

  // Parse JSONL
  const results = outputContent
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);

  context.log(`Parsed ${results.length} results`);

  // Transform to Salesforce-compatible format for ADF WriteBack
  const processedResults = results
    .map((result) => transformToSalesforceFormat(result, job, context))
    .filter(Boolean);

  // Save to output blob (for ADF to pick up)
  const { outputPath, fileName } = await saveResults(job, processedResults, context);

  // Update job status
  await batchJobStorage.markCompleted(job.batchId, outputFileId, batchStatus.error_file_id);
  await batchJobStorage.markProcessed(job.batchId, outputPath);

  const successCount = processedResults.length;
  context.log(`Saved ${successCount}/${results.length} results to ${outputPath}`);

  // Trigger ADF WriteBack Pipeline
  await triggerWriteBackPipeline(fileName, context);
}

/* ─────────────────────────────────────────────────────────────── */
/* TRANSFORM TO SALESFORCE FORMAT                                  */
/* Maps AI insights to Salesforce azinsights__c field structure    */
/* Only uses fields that exist in the actual SF object             */
/* ─────────────────────────────────────────────────────────────── */
function transformToSalesforceFormat(result, job, context) {
  if (result.error) {
    context.warn(`Skipping failed record ${result.custom_id}: ${result.error.message}`);
    return null;
  }

  const content = result.response?.body?.choices?.[0]?.message?.content;
  let insights;
  try {
    insights = content ? JSON.parse(content) : null;
  } catch {
    context.warn(`Failed to parse AI response for ${result.custom_id}`);
    return null;
  }

  if (!insights) return null;

  // Map to actual Salesforce azinsights__c object fields
  const sfRecord = {
    ParentObjectId__c: result.custom_id,
    ParentObjectApiName__c: job.objectName || null,
    RawInsightsJSON__c: truncate(JSON.stringify(insights), 131072),
  };

  return sfRecord;
}

/* ─────────────────────────────────────────────────────────────── */
/* SAVE RESULTS TO BLOB                                            */
/* Output as JSONL (JSON Lines) - one record per line for ADF      */
/* ─────────────────────────────────────────────────────────────── */
async function saveResults(job, results, context) {
  const blobServiceClient = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
  const containerClient = blobServiceClient.getContainerClient(OUTPUT_CONTAINER);
  await containerClient.createIfNotExists();

  const baseName = job.inputBlobPath.split("/").pop()?.replace(/\.[^/.]+$/, "") || "unknown";
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const fileName = `${baseName}_processed_${timestamp}.jsonl`;

  // JSONL format: one JSON object per line (ADF handles this as tabular data)
  const content = results.map(r => JSON.stringify(r)).join("\n");
  const blobClient = containerClient.getBlockBlobClient(fileName);
  await blobClient.upload(content, Buffer.byteLength(content), {
    blobHTTPHeaders: { blobContentType: "application/x-ndjson" },
  });

  context.log(`Saved ${results.length} records to ${OUTPUT_CONTAINER}/${fileName}`);
  return { outputPath: `${OUTPUT_CONTAINER}/${fileName}`, fileName };
}

/* ─────────────────────────────────────────────────────────────── */
/* TRIGGER ADF WRITEBACK PIPELINE                                  */
/* ─────────────────────────────────────────────────────────────── */
async function triggerWriteBackPipeline(fileName, context) {
  if (!SUBSCRIPTION_ID || !RESOURCE_GROUP || !DATA_FACTORY_NAME) {
    context.warn("ADF not configured, skipping WriteBack pipeline trigger");
    return;
  }

  try {
    const credential = new DefaultAzureCredential();
    const token = await credential.getToken("https://management.azure.com/.default");

    const runUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATA_FACTORY_NAME}/pipelines/${WRITEBACK_PIPELINE_NAME}/createRun?api-version=2018-06-01`;

    const pipelineParams = {
      fileName: fileName,
    };

    const response = await fetch(runUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(pipelineParams),
    });

    if (!response.ok) {
      const errorText = await response.text();
      context.error(`Failed to trigger WriteBack pipeline: ${response.status} - ${errorText}`);
      return;
    }

    const result = await response.json();
    context.log(`WriteBack pipeline triggered: ${result.runId} for file ${fileName}`);
  } catch (err) {
    context.error(`Error triggering WriteBack pipeline: ${err.message}`);
  }
}

/* ─────────────────────────────────────────────────────────────── */
/* HELPERS                                                         */
/* ─────────────────────────────────────────────────────────────── */
function truncate(str, maxLength) {
  if (!str) return null;
  if (typeof str !== "string") str = String(str);
  return str.length > maxLength ? str.substring(0, maxLength - 3) + "..." : str;
}

function arrayToString(arr) {
  if (!arr || !Array.isArray(arr)) return null;
  return arr.join(", ");
}
