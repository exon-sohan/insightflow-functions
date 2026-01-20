const { app } = require("@azure/functions");
const { AzureOpenAI } = require("@azure/openai");
const { DefaultAzureCredential } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");

const AZURE_OPENAI_ENDPOINT = process.env.AZURE_AI_PROJECT_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT_NAME;
const STORAGE_ACCOUNT_NAME = process.env.AZURE_STORAGE_ACCOUNT_NAME;
const INPUT_CONTAINER = "datasets";
const OUTPUT_CONTAINER = "output";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "10", 10);

/* ───────────────────────────────────────────────────────────── */
/* LOGGER */
/* ───────────────────────────────────────────────────────────── */
function logger(context) {
  return {
    info: (msg, data) => context.log(`ℹ️ [INFO] ${msg}`, data ?? ""),
    success: (msg, data) => context.log(`✅ [SUCCESS] ${msg}`, data ?? ""),
    warn: (msg, data) => context.warn(`⚠️ [WARN] ${msg}`, data ?? ""),
    error: (msg, err) =>
      context.error(`❌ [ERROR] ${msg}`, err?.stack || err || ""),
    debug: (msg, data) => context.log(`🐞 [DEBUG] ${msg}`, data ?? ""),
  };
}

/* ───────────────────────────────────────────────────────────── */
/* BLOB TRIGGER */
/* ───────────────────────────────────────────────────────────── */
app.storageBlob("storageBlobTrigger", {
  path: "datasets/{name}",
  connection: "AzureWebJobsStorage",
  handler: async (blob, context) => {
    const log = logger(context);
    const fileName = context.triggerMetadata.name;
    const startTime = Date.now();

    log.info("════════════════════════════════════════════");
    log.info("Blob trigger fired");
    log.info(`File name: ${fileName}`);
    log.info(`File size: ${blob.length} bytes`);
    log.info("════════════════════════════════════════════");

    try {
      /* PARSE */
      log.info("Parsing blob content");
      const content = blob.toString("utf8");
      const records = parseCallRecords(content);
      log.info(`Parsed ${records.length} record(s)`);

      /* AZURE OPENAI INIT */
      log.info("Initializing Azure OpenAI client");
      log.info(`Endpoint: ${AZURE_OPENAI_ENDPOINT || "NOT SET"}`);
      log.info(`Deployment: ${AZURE_OPENAI_DEPLOYMENT || "NOT SET"}`);

      if (!AZURE_OPENAI_ENDPOINT) {
        throw new Error(
          "AZURE_AI_PROJECT_ENDPOINT environment variable is not set"
        );
      }

      if (!AZURE_OPENAI_DEPLOYMENT) {
        throw new Error(
          "AZURE_OPENAI_DEPLOYMENT_NAME environment variable is not set"
        );
      }

      log.info("Creating DefaultAzureCredential...");
      const credential = new DefaultAzureCredential();
      log.info("DefaultAzureCredential created");

      let openAIClient;
      try {
        log.info("Creating AzureOpenAI client...");
        openAIClient = new AzureOpenAI({
          endpoint: AZURE_OPENAI_ENDPOINT,
          credential: credential,
          apiVersion: "2024-10-21",
        });
        log.info("AzureOpenAI client created successfully");
      } catch (initErr) {
        log.error("Failed to create Azure OpenAI client", initErr);
        throw initErr;
      }

      log.success(`Connected to Azure OpenAI deployment: ${AZURE_OPENAI_DEPLOYMENT}`);

      /* PROCESS RECORDS IN BATCHES */
      const processedResults = await processBatches(
        records,
        BATCH_SIZE,
        openAIClient,
        AZURE_OPENAI_DEPLOYMENT,
        log
      );

      /* SAVE OUTPUT */
      await saveProcessedResults(fileName, processedResults, log);

      log.success("All records processed successfully", {
        total: records.length,
        durationMs: Date.now() - startTime,
      });
    } catch (err) {
      log.error("Dataset processing failed", err);
      throw err;
    }
  },
});

/* ───────────────────────────────────────────────────────────── */
/* BATCH PROCESSING */
/* ───────────────────────────────────────────────────────────── */
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

async function processBatches(records, batchSize, openAIClient, deploymentName, log) {
  const batches = chunkArray(records, batchSize);
  const allResults = [];

  log.info(
    `Processing ${records.length} records in ${batches.length} batch(es) of up to ${batchSize}`
  );

  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const batch = batches[batchIndex];
    const batchStart = batchIndex * batchSize;

    log.info(
      `═══ Starting batch ${batchIndex + 1}/${batches.length} (${
        batch.length
      } records) ═══`
    );

    // Process all records in this batch concurrently
    const batchPromises = batch.map((record, indexInBatch) => {
      const globalIndex = batchStart + indexInBatch;
      const recordId = record.Name || record.Id || `Record-${globalIndex + 1}`;

      log.info(`Queuing record ${globalIndex + 1}/${records.length}`, {
        recordId,
      });

      return processWithOpenAI(openAIClient, deploymentName, record, log)
        .then((result) => ({
          success: true,
          originalRecord: record,
          aiResponse: result,
          processedAt: new Date().toISOString(),
        }))
        .catch((error) => ({
          success: false,
          originalRecord: record,
          aiResponse: { error: error.message || String(error) },
          processedAt: new Date().toISOString(),
        }));
    });

    // Wait for all records in this batch to complete
    const batchResults = await Promise.all(batchPromises);

    const successCount = batchResults.filter((r) => r.success).length;
    const failCount = batchResults.filter((r) => !r.success).length;

    log.success(`Batch ${batchIndex + 1} completed`, {
      success: successCount,
      failed: failCount,
    });

    allResults.push(...batchResults);
  }

  const totalSuccess = allResults.filter((r) => r.success).length;
  const totalFailed = allResults.filter((r) => !r.success).length;

  log.info(`All batches completed`, {
    totalRecords: records.length,
    success: totalSuccess,
    failed: totalFailed,
  });

  return allResults;
}

/* ───────────────────────────────────────────────────────────── */
/* AZURE OPENAI PROCESSING */
/* ───────────────────────────────────────────────────────────── */
async function processWithOpenAI(openAIClient, deploymentName, record, log) {
  const recordId = record.Name || record.Id || "UNKNOWN";

  /* ─────────────────────────────────────────────
   * BUILD PROMPT
   * ───────────────────────────────────────────── */
  const systemPrompt = `You are an AI assistant specialized in analyzing customer call records for leadership reporting.

RULES:
- Output ONLY valid JSON (no markdown, no explanations)
- Do NOT hallucinate or infer information not present in the data
- Use null, false, or [] if data is missing or unclear
- Be precise and factual`;

  const userPrompt = `Analyze the following call record and generate structured insights for leadership reporting.

=== CALL RECORD ===
Record ID: ${record.Id || "N/A"}
Name: ${record.Name || "N/A"}
Call Details:
${record.calldetails__c || "N/A"}

Call Highlights:
${stripHtml(record.callhighlights__c || "")}

Description: ${record.Description__c || "N/A"}
Status: ${record.Status__c || "N/A"}
Created Date: ${record.CreatedDate || "N/A"}

=== REQUIRED OUTPUT ===
{
  "callSummary": "2–4 sentence executive-ready summary",

  "flags": {
    "hasProductMention": false,
    "hasQualityComplaint": false,
    "hasDeliveryComplaint": false,
    "hasEscalationRisk": false,
    "hasCompetitorMention": false,
    "hasUpsellOpportunity": false,
    "hasComplianceIssue": false,
    "hasActionItem": false,
    "hasFeatureRequest": false,
    "hasPricingConcern": false,
    "hasTechnicalIssue": false
  },

  "sentiment": {
    "score": "Positive" | "Neutral" | "Negative",
    "confidence": 0.0,
    "trend": "Improving" | "Stable" | "Worsening",
    "emotion": "Calm" | "Frustrated" | "Angry" | "Satisfied" | "Anxious" | null,
    "reason": "Brief explanation"
  },

  "customerIntent": {
    "primary": "Support" | "Complaint" | "Inquiry" | "Purchase" | "Renewal" | "Cancellation" | "Feedback",
    "secondary": [],
    "confidence": 0.0
  },

  "businessImpact": {
    "revenueImpact": "None" | "Low" | "Medium" | "High",
    "churnRisk": "Low" | "Medium" | "High",
    "accountHealth": "Healthy" | "At Risk" | "Critical",
    "reason": "Explanation"
  },

  "products": {
    "mentioned": [],
    "primary": null,
    "context": "Positive" | "Negative" | "Inquiry" | null
  },

  "qualityComplaint": {
    "detected": false,
    "category": "Defect" | "Performance" | "Durability" | "Functionality" | null,
    "severity": "Critical" | "Major" | "Minor" | null,
    "details": null
  },

  "deliveryComplaint": {
    "detected": false,
    "category": "Late Delivery" | "Damaged" | "Wrong Item" | "Missing Item" | "Tracking Issues" | null,
    "severity": "Critical" | "Major" | "Minor" | null,
    "details": null
  },

  "technicalIssue": {
    "detected": false,
    "category": "Bug" | "Integration" | "Performance" | "Compatibility" | "Configuration" | "Security" | null,
    "severity": "Critical" | "Major" | "Minor" | null,
    "details": null
  },

  "pricingConcern": {
    "detected": false,
    "type": "Too Expensive" | "Competitor Pricing" | "Discount Request" | "Value Perception" | "Contract Terms" | null,
    "severity": "Critical" | "Major" | "Minor" | null,
    "details": null
  },

  "featureRequest": {
    "detected": false,
    "feature": null,
    "priority": "High" | "Medium" | "Low" | null,
    "productArea": null,
    "details": null
  },

  "competitorMention": {
    "detected": false,
    "competitors": [],
    "context": "Pricing" | "Features" | "Service" | "Switching" | null
  },

  "upsellOpportunity": {
    "detected": false,
    "products": [],
    "estimatedValue": "Low" | "Medium" | "High" | null,
    "reason": null
  },

  "escalationRisk": {
    "level": "High" | "Medium" | "Low",
    "reason": "Explanation",
    "accountAtRisk": false
  },

  "complianceRisk": {
    "detected": false,
    "type": "Data Privacy" | "Financial" | "Regulatory" | "Contractual" | null,
    "severity": "Critical" | "Major" | "Minor" | null,
    "details": null
  },

  "callQuality": {
    "clarity": 0.0,
    "agentEmpathy": 0.0,
    "issueUnderstanding": 0.0,
    "resolutionEffectiveness": 0.0,
    "notes": null
  },

  "actionItems": [
    {
      "action": "What needs to be done",
      "priority": "High" | "Medium" | "Low",
      "owner": "Sales" | "Support" | "Technical" | "Management",
      "dueInDays": 0,
      "customerVisible": false,
      "blockingIssue": false
    }
  ],

  "followUp": {
    "required": false,
    "recommendedDate": null,
    "reason": null,
    "preferredChannel": "Call" | "Email" | "WhatsApp" | "In-App" | null
  },

  "resolutionStatus": "Resolved" | "Pending" | "Escalated" | "Requires Follow-up",

  "keyTopics": [
    { "topic": "Topic", "category": "General", "importance": "High" }
  ],

  "analysisMeta": {
    "confidenceScore": 0.0,
    "missingInformation": [],
    "assumptionsMade": []
  }
}`;

  /* ─────────────────────────────────────────────
   * CALL AZURE OPENAI
   * ───────────────────────────────────────────── */
  try {
    log.info(`Processing record with Azure OpenAI`, { recordId });

    const response = await openAIClient.chat.completions.create({
      model: deploymentName,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      temperature: 0.3,
      max_tokens: 4000,
      response_format: { type: "json_object" },
    });

    const content = response.choices[0]?.message?.content;
    if (!content) {
      throw new Error("No response content from Azure OpenAI");
    }

    /* ─────────────────────────────────────────────
     * PARSE JSON RESPONSE
     * ───────────────────────────────────────────── */
    let insights;
    try {
      insights = JSON.parse(content);
    } catch (parseErr) {
      log.warn("JSON parse failed", { recordId });
      insights = { rawResponse: content, parseError: true };
    }

    log.success(`Processed record ${recordId}`);
    return { insights, usage: response.usage };
  } catch (err) {
    log.error(`Failed to process record ${recordId}`, err);
    throw err;
  }
}

/* ───────────────────────────────────────────────────────────── */
/* SAVE RESULTS TO BLOB STORAGE */
/* ───────────────────────────────────────────────────────────── */
async function saveProcessedResults(originalFileName, results, log) {
  log.info("Saving processed results to Blob Storage");

  if (!STORAGE_ACCOUNT_NAME) {
    throw new Error("AZURE_STORAGE_ACCOUNT_NAME environment variable is not set");
  }

  const credential = new DefaultAzureCredential();
  const blobServiceClient = new BlobServiceClient(
    `https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
    credential
  );
  const containerClient =
    blobServiceClient.getContainerClient(OUTPUT_CONTAINER);
  await containerClient.createIfNotExists();

  const base = originalFileName.replace(/\.[^/.]+$/, "");
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const outputName = `${base}_processed_${ts}.json`;

  const blockBlob = containerClient.getBlockBlobClient(outputName);
  const body = JSON.stringify(results, null, 2);

  await blockBlob.upload(body, Buffer.byteLength(body), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  log.success("Results saved", { outputName });
}

/* ───────────────────────────────────────────────────────────── */
/* HELPERS */
/* ───────────────────────────────────────────────────────────── */
function parseCallRecords(content) {
  return content
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function stripHtml(html) {
  return html
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
