/**
 * Storage Blob Trigger - Batch API Version
 * Submits call records to Azure OpenAI Batch API for processing (50% cost savings)
 */

const { app } = require("@azure/functions");
const { AzureOpenAI } = require("openai");
const { BlobServiceClient } = require("@azure/storage-blob");
const batchJobStorage = require("./batchJobStorage");

const AZURE_OPENAI_ENDPOINT = process.env.AZURE_AI_PROJECT_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT_NAME;
// Batch deployment must be GlobalBatch SKU - different from standard deployment
const AZURE_OPENAI_BATCH_DEPLOYMENT = process.env.AZURE_OPENAI_BATCH_DEPLOYMENT_NAME || "gpt-4o-mini-batch";
const AZURE_OPENAI_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const STORAGE_CONNECTION_STRING =
  process.env.AZURE_STORAGE_CONNECTION_STRING ||
  process.env.AzureWebJobsStorage;
const OUTPUT_CONTAINER = process.env.OUTPUT_CONTAINER || "output";
const PROMPT_CONFIG_CONTAINER = "prompt-configs";

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
/* BLOB TRIGGER - Submit Batch Job */
/* ───────────────────────────────────────────────────────────── */
app.storageBlob("storageBlobTrigger", {
  path: "datasets/{name}",
  connection: "AzureWebJobsStorage",
  handler: async (blob, context) => {
    const log = logger(context);
    const fileName = context.triggerMetadata.name;
    const startTime = Date.now();

    log.info("════════════════════════════════════════════");
    log.info("Batch API Submit - Blob trigger fired");
    log.info(`File name: ${fileName}`);
    log.info(`File size: ${blob.length} bytes`);
    log.info("════════════════════════════════════════════");

    try {
      // Parse records
      log.info("Parsing blob content");
      const content = blob.toString("utf8");
      const records = parseCallRecords(content);
      log.info(`Parsed ${records.length} record(s)`);

      if (records.length === 0) {
        log.warn("No records found in file, skipping");
        return;
      }

      // Try to load custom prompt config based on the file path
      // File path is like "ObjectName/ObjectName_timestamp.json"
      let promptConfig = null;
      try {
        const objectNameFromPath = fileName.split("/")[0] || fileName.split("_")[0];
        if (objectNameFromPath) {
          promptConfig = await loadPromptConfig(objectNameFromPath.toLowerCase(), log);
          if (promptConfig) {
            log.info(`Loaded custom prompt config for object: ${objectNameFromPath}`);
          }
        }
      } catch (configError) {
        log.debug("No custom prompt config found, using defaults");
      }

      // Validate Azure OpenAI config
      if (!AZURE_OPENAI_ENDPOINT) {
        throw new Error("AZURE_AI_PROJECT_ENDPOINT environment variable is not set");
      }
      if (!AZURE_OPENAI_BATCH_DEPLOYMENT) {
        throw new Error("AZURE_OPENAI_BATCH_DEPLOYMENT_NAME environment variable is not set");
      }
      if (!AZURE_OPENAI_API_KEY) {
        throw new Error("AZURE_OPENAI_API_KEY environment variable is not set");
      }

      // Initialize Azure OpenAI client
      log.info("Initializing Azure OpenAI client");
      log.info(`Endpoint: ${AZURE_OPENAI_ENDPOINT}`);
      log.info(`Batch Deployment: ${AZURE_OPENAI_BATCH_DEPLOYMENT}`);

      const openAIClient = new AzureOpenAI({
        endpoint: AZURE_OPENAI_ENDPOINT,
        apiKey: AZURE_OPENAI_API_KEY,
        deployment: AZURE_OPENAI_DEPLOYMENT,
        apiVersion: "2024-10-21",
      });

      // Create JSONL content for batch API
      log.info("Creating JSONL batch input file");
      const jsonlContent = createBatchJsonl(records, AZURE_OPENAI_BATCH_DEPLOYMENT, promptConfig);

      // Upload JSONL file to Azure OpenAI
      log.info("Uploading batch input file to Azure OpenAI");
      const inputFile = await uploadBatchFile(openAIClient, jsonlContent, fileName);
      log.success(`Batch input file uploaded: ${inputFile.id}`);

      // Create batch job
      log.info("Creating batch job");
      const batch = await openAIClient.batches.create({
        input_file_id: inputFile.id,
        endpoint: "/chat/completions",
        completion_window: "24h",
      });
      log.success(`Batch job created: ${batch.id}`);

      // Store batch job info for status checker
      await batchJobStorage.createBatchJob({
        batchId: batch.id,
        inputFileId: inputFile.id,
        inputBlobPath: `datasets/${fileName}`,
        recordCount: records.length,
      });
      log.success("Batch job saved to tracking storage");

      log.success("Batch submission completed", {
        batchId: batch.id,
        recordCount: records.length,
        durationMs: Date.now() - startTime,
      });
    } catch (err) {
      log.error("Batch submission failed", err);
      throw err;
    }
  },
});

/* ───────────────────────────────────────────────────────────── */
/* CREATE JSONL FOR BATCH API */
/* ───────────────────────────────────────────────────────────── */
function createBatchJsonl(records, deploymentName, promptConfig = null) {
  const lines = records.map((record, index) => {
    const recordId = record.Id || record.Name || `record-${index}`;

    // Use custom prompts if provided, otherwise use defaults
    const systemPromptContent = promptConfig?.systemPrompt || getSystemPrompt();
    const userPromptContent = promptConfig?.userPromptTemplate
      ? buildCustomUserPrompt(record, promptConfig.userPromptTemplate, promptConfig.fieldNames)
      : getUserPrompt(record);

    const request = {
      custom_id: recordId,
      method: "POST",
      url: "/v1/chat/completions",  // Must use /v1/ prefix for Azure OpenAI Batch API
      body: {
        model: deploymentName,
        messages: [
          { role: "system", content: systemPromptContent },
          { role: "user", content: userPromptContent },
        ],
        temperature: 0.3,
        max_tokens: 4000,
        response_format: { type: "json_object" },
      },
    };

    return JSON.stringify(request);
  });

  return lines.join("\n");
}

/* ───────────────────────────────────────────────────────────── */
/* BUILD CUSTOM USER PROMPT WITH DYNAMIC FIELDS */
/* ───────────────────────────────────────────────────────────── */
function buildCustomUserPrompt(record, userPromptTemplate, fieldNames) {
  // Replace {{fieldName}} placeholders with actual values from the record
  let prompt = userPromptTemplate;

  // Replace all {{fieldName}} placeholders
  const fieldList = fieldNames ? fieldNames.split(",").map(f => f.trim()) : [];

  // Build a record data section with all available fields
  let recordDataSection = "=== RECORD DATA ===\n";
  for (const field of fieldList) {
    const value = record[field] !== undefined ? record[field] : "N/A";
    recordDataSection += `${field}: ${value}\n`;
    // Also replace specific placeholders like {{Id}}, {{Name}}, etc.
    prompt = prompt.replace(new RegExp(`\\{\\{${field}\\}\\}`, 'g'), value || "N/A");
  }

  // Also add all record fields for flexibility
  for (const [key, value] of Object.entries(record)) {
    prompt = prompt.replace(new RegExp(`\\{\\{${key}\\}\\}`, 'g'), value || "N/A");
  }

  // Replace {{RECORD_DATA}} placeholder with the full record data section
  prompt = prompt.replace(/\{\{RECORD_DATA\}\}/g, recordDataSection);

  // Replace {{RECORD_JSON}} with the full JSON record
  prompt = prompt.replace(/\{\{RECORD_JSON\}\}/g, JSON.stringify(record, null, 2));

  return prompt;
}

/* ───────────────────────────────────────────────────────────── */
/* LOAD PROMPT CONFIG FROM BLOB STORAGE */
/* ───────────────────────────────────────────────────────────── */
async function loadPromptConfig(objectName, log) {
  try {
    const blobServiceClient = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
    const containerClient = blobServiceClient.getContainerClient(PROMPT_CONFIG_CONTAINER);

    const configBlobName = `${objectName}_config.json`;
    const blockBlobClient = containerClient.getBlockBlobClient(configBlobName);

    // Check if blob exists
    const exists = await blockBlobClient.exists();
    if (!exists) {
      return null;
    }

    // Download and parse config
    const downloadResponse = await blockBlobClient.download(0);
    const configContent = await streamToString(downloadResponse.readableStreamBody);
    const config = JSON.parse(configContent);

    log.info(`Loaded prompt config: systemPrompt=${!!config.systemPrompt}, userPromptTemplate=${!!config.userPromptTemplate}`);
    return config;
  } catch (error) {
    log.debug(`Failed to load prompt config for ${objectName}: ${error.message}`);
    return null;
  }
}

async function streamToString(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", (data) => chunks.push(data.toString()));
    readableStream.on("end", () => resolve(chunks.join("")));
    readableStream.on("error", reject);
  });
}

/* ───────────────────────────────────────────────────────────── */
/* UPLOAD BATCH FILE TO AZURE OPENAI */
/* ───────────────────────────────────────────────────────────── */
async function uploadBatchFile(openAIClient, jsonlContent, originalFileName) {
  const blob = new Blob([jsonlContent], { type: "application/jsonl" });
  const file = new File([blob], `batch_input_${originalFileName}.jsonl`, {
    type: "application/jsonl",
  });

  const uploadedFile = await openAIClient.files.create({
    file: file,
    purpose: "batch",
  });

  return uploadedFile;
}

/* ───────────────────────────────────────────────────────────── */
/* PROMPTS */
/* ───────────────────────────────────────────────────────────── */
function getSystemPrompt() {
  return `You are an AI assistant specialized in analyzing customer call records for leadership reporting.

RULES:
- Output ONLY valid JSON (no markdown, no explanations)
- Do NOT hallucinate or infer information not present in the data
- Use null, false, or [] if data is missing or unclear
- Be precise and factual`;
}

function getUserPrompt(record) {
  return `Analyze the following call record and generate structured insights for leadership reporting.

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
  "callSummary": "2-4 sentence executive-ready summary",

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
}

/* ───────────────────────────────────────────────────────────── */
/* HELPERS */
/* ───────────────────────────────────────────────────────────── */
function parseCallRecords(content) {
  // Remove BOM if present
  const cleanContent = content.replace(/^\uFEFF/, "").trim();

  // Try parsing as JSON array first
  try {
    const parsed = JSON.parse(cleanContent);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    // Single object
    return [parsed];
  } catch {
    // Fall back to NDJSON (newline-delimited JSON)
    return cleanContent
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
}

function stripHtml(html) {
  return html
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
