/**
 * InsightFlow - Storage Blob Trigger
 * Submits data to Azure OpenAI Batch API with dynamic prompts
 */

const { app } = require("@azure/functions");
const { AzureOpenAI } = require("openai");
const { BlobServiceClient } = require("@azure/storage-blob");
const batchJobStorage = require("./batchJobStorage");

const AZURE_OPENAI_ENDPOINT = process.env.AZURE_AI_PROJECT_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT_NAME;
const AZURE_OPENAI_BATCH_DEPLOYMENT = process.env.AZURE_OPENAI_BATCH_DEPLOYMENT_NAME || "gpt-4o-mini-batch";
const AZURE_OPENAI_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING || process.env.AzureWebJobsStorage;
const CONFIG_CONTAINER = "pipeline-configs";

// Preset configurations
const PRESETS = {
  "sales-call": {
    insights: ["summary", "sentiment", "products", "upsellOpportunity", "competitors", "nextSteps"],
    systemPrompt: "You analyze sales call records. Output only valid JSON.",
  },
  "support-ticket": {
    insights: ["summary", "sentiment", "escalationRisk", "technicalIssues", "resolution", "followUp"],
    systemPrompt: "You analyze support tickets. Output only valid JSON.",
  },
  "feedback": {
    insights: ["summary", "sentiment", "npsIndicator", "themes", "suggestions"],
    systemPrompt: "You analyze customer feedback. Output only valid JSON.",
  },
  "compliance": {
    insights: ["summary", "riskLevel", "violations", "recommendations"],
    systemPrompt: "You analyze records for compliance issues. Output only valid JSON.",
  },
  "general": {
    insights: ["summary", "sentiment", "keyPoints", "actionItems"],
    systemPrompt: "You analyze business records. Output only valid JSON.",
  },
};

/* ─────────────────────────────────────────────────────────────── */
/* BLOB TRIGGER - Process datasets and submit to Batch API         */
/* ─────────────────────────────────────────────────────────────── */
app.storageBlob("storageBlobTrigger", {
  path: "datasets/{name}",
  connection: "AzureWebJobsStorage",
  handler: async (blob, context) => {
    const fileName = context.triggerMetadata.name;
    const startTime = Date.now();

    context.log(`Blob trigger fired: ${fileName}`);

    try {
      // Parse records
      const content = blob.toString("utf8");
      const records = parseRecords(content);
      context.log(`Parsed ${records.length} records`);

      if (records.length === 0) {
        context.log("No records found, skipping");
        return;
      }

      // Load config based on object name from file path
      const objectName = extractObjectName(fileName);
      const config = await loadConfig(objectName, context);

      // Validate Azure OpenAI config
      if (!AZURE_OPENAI_ENDPOINT || !AZURE_OPENAI_API_KEY) {
        throw new Error("Azure OpenAI not configured");
      }

      // Initialize OpenAI client
      const openAIClient = new AzureOpenAI({
        endpoint: AZURE_OPENAI_ENDPOINT,
        apiKey: AZURE_OPENAI_API_KEY,
        deployment: AZURE_OPENAI_DEPLOYMENT,
        apiVersion: "2024-10-21",
      });

      // Create JSONL for batch API
      const jsonlContent = createBatchJsonl(records, config, objectName);

      // Upload to Azure OpenAI
      const inputFile = await uploadBatchFile(openAIClient, jsonlContent, fileName);
      context.log(`Batch input uploaded: ${inputFile.id}`);

      // Create batch job
      const batch = await openAIClient.batches.create({
        input_file_id: inputFile.id,
        endpoint: "/chat/completions",
        completion_window: "24h",
      });
      context.log(`Batch job created: ${batch.id}`);

      // Save job for tracking
      await batchJobStorage.createBatchJob({
        batchId: batch.id,
        inputFileId: inputFile.id,
        inputBlobPath: `datasets/${fileName}`,
        recordCount: records.length,
        objectName,
        config,
      });

      context.log(`Batch submitted successfully in ${Date.now() - startTime}ms`);
    } catch (err) {
      context.error(`Batch submission failed:`, err);
      throw err;
    }
  },
});

/* ─────────────────────────────────────────────────────────────── */
/* CREATE JSONL FOR BATCH API                                      */
/* ─────────────────────────────────────────────────────────────── */
function createBatchJsonl(records, config, objectName) {
  const lines = records.map((record, index) => {
    const recordId = record.Id || record.Name || `record-${index}`;
    const { systemPrompt, userPrompt } = buildPrompts(record, config, objectName);

    const request = {
      custom_id: recordId,
      method: "POST",
      url: "/v1/chat/completions",
      body: {
        model: AZURE_OPENAI_BATCH_DEPLOYMENT,
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
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

/* ─────────────────────────────────────────────────────────────── */
/* BUILD PROMPTS BASED ON CONFIG                                   */
/* ─────────────────────────────────────────────────────────────── */
function buildPrompts(record, config, objectName) {
  const analysis = config?.analysis || {};

  // Custom prompts with explicit systemPrompt/userPromptTemplate
  if (analysis.type === "custom" && analysis.systemPrompt) {
    let systemPrompt = analysis.systemPrompt || "You analyze records. Output only valid JSON.";
    let userPrompt = analysis.userPromptTemplate || "Analyze: {{RECORD_DATA}}";

    // Ensure JSON is mentioned
    if (!(systemPrompt + userPrompt).toLowerCase().includes("json")) {
      systemPrompt += "\n\nIMPORTANT: Respond with valid JSON only.";
    }

    // Replace placeholders
    userPrompt = replacePlaceholders(userPrompt, record, config?.source?.fields);

    return { systemPrompt, userPrompt };
  }

  // If a custom schema is provided, use it to build the prompt
  if (analysis.schema && Object.keys(analysis.schema).length > 0) {
    const schemaDescription = buildSchemaDescription(analysis.schema);
    const preset = PRESETS[analysis.preset] || PRESETS["general"];

    const systemPrompt = `${preset.systemPrompt}

You must return a JSON object with this exact structure:

${schemaDescription}

IMPORTANT:
- Follow the exact field names and types specified above
- Use null for missing or uncertain data
- For enum fields, only use the allowed values
- For arrays, return empty array [] if no items
- Be precise and do not hallucinate information`;

    const userPrompt = `Analyze this ${objectName} record:

${formatRecordData(record)}

Return a JSON object following the schema described in the system prompt.`;

    return { systemPrompt, userPrompt };
  }

  // Fallback to preset prompts
  const preset = PRESETS[analysis.preset] || PRESETS["general"];
  const insights = analysis.insights || preset.insights;

  const systemPrompt = `${preset.systemPrompt}

You must return a JSON object with these fields:
${insights.map((i) => `- ${i}`).join("\n")}

Be precise. Use null for missing data. Do not hallucinate.`;

  const userPrompt = `Analyze this ${objectName} record:

${formatRecordData(record)}

Return a JSON object with: ${insights.join(", ")}`;

  return { systemPrompt, userPrompt };
}

/* ─────────────────────────────────────────────────────────────── */
/* BUILD SCHEMA DESCRIPTION FROM CONFIG                            */
/* ─────────────────────────────────────────────────────────────── */
function buildSchemaDescription(schema) {
  const lines = [];

  for (const [fieldName, fieldConfig] of Object.entries(schema)) {
    lines.push(describeField(fieldName, fieldConfig, 0));
  }

  return lines.join("\n\n");
}

function describeField(name, config, indent = 0) {
  const prefix = "  ".repeat(indent);
  const lines = [];

  if (config.type === "flags") {
    lines.push(`${prefix}${name}: object with boolean flags:`);
    for (const flag of config.fields || []) {
      lines.push(`${prefix}  - ${flag}: boolean`);
    }
  } else if (config.type === "object" && config.fields) {
    lines.push(`${prefix}${name}: object with fields:`);
    for (const [subName, subConfig] of Object.entries(config.fields)) {
      lines.push(describeField(subName, subConfig, indent + 1));
    }
  } else if (config.type === "array" && config.itemType === "object" && config.itemFields) {
    lines.push(`${prefix}${name}: array of objects, each with:`);
    for (const [subName, subConfig] of Object.entries(config.itemFields)) {
      lines.push(describeField(subName, subConfig, indent + 1));
    }
  } else if (config.type === "array") {
    lines.push(`${prefix}${name}: array of ${config.itemType || "string"}s`);
  } else if (config.type === "enum") {
    const options = config.options || "value";
    lines.push(`${prefix}${name}: string, one of [${options.split("|").join(", ")}]`);
  } else if (config.type === "boolean") {
    lines.push(`${prefix}${name}: boolean`);
  } else if (config.type === "number") {
    const desc = config.description ? ` (${config.description})` : "";
    lines.push(`${prefix}${name}: number${desc}`);
  } else {
    const desc = config.description ? ` - ${config.description}` : "";
    lines.push(`${prefix}${name}: string${desc}`);
  }

  return lines.join("\n");
}

/* ─────────────────────────────────────────────────────────────── */
/* HELPERS                                                         */
/* ─────────────────────────────────────────────────────────────── */
function replacePlaceholders(template, record, fields) {
  let result = template;

  // Replace {{fieldName}} with actual values
  for (const [key, value] of Object.entries(record)) {
    result = result.replace(new RegExp(`\\{\\{${key}\\}\\}`, "gi"), value || "N/A");
  }

  // Replace {{RECORD_DATA}}
  result = result.replace(/\{\{RECORD_DATA\}\}/gi, formatRecordData(record));

  // Replace {{RECORD_JSON}}
  result = result.replace(/\{\{RECORD_JSON\}\}/gi, JSON.stringify(record, null, 2));

  return result;
}

function formatRecordData(record) {
  return Object.entries(record)
    .map(([key, value]) => `${key}: ${value || "N/A"}`)
    .join("\n");
}

function extractObjectName(fileName) {
  // File path: "ObjectName__c/ObjectName__c_timestamp.json" or "ObjectName__c_timestamp.json"
  // We need to preserve the full object name including __c suffix
  const parts = fileName.split("/");
  const folderOrFile = parts[0] || fileName;

  // If it's a folder path, use the folder name
  // If it contains a timestamp pattern (_YYYYMMDD_), extract before that
  const timestampMatch = folderOrFile.match(/^(.+?)_\d{8}_/);
  if (timestampMatch) {
    return timestampMatch[1].toLowerCase();
  }

  // Otherwise use the folder/file name as-is (without extension)
  return folderOrFile.replace(/\.json$/i, '').toLowerCase();
}

async function loadConfig(objectName, context) {
  try {
    if (!STORAGE_CONNECTION_STRING) {
      context.log(`No storage connection string, skipping config load`);
      return null;
    }

    const blobServiceClient = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
    const containerClient = blobServiceClient.getContainerClient(CONFIG_CONTAINER);
    const blobName = `${objectName}_config.json`;
    context.log(`Looking for config: ${CONFIG_CONTAINER}/${blobName}`);

    const blobClient = containerClient.getBlockBlobClient(blobName);

    if (!(await blobClient.exists())) {
      context.log(`No config found for ${objectName}, using defaults`);
      return null;
    }

    const downloadResponse = await blobClient.download(0);
    const content = await streamToString(downloadResponse.readableStreamBody);
    const config = JSON.parse(content);
    context.log(`Config loaded successfully, has schema: ${!!config?.analysis?.schema}`);
    return config;
  } catch (err) {
    context.error(`Failed to load config: ${err.message}`);
    return null;
  }
}

function parseRecords(content) {
  const clean = content.replace(/^\uFEFF/, "").trim();
  try {
    const parsed = JSON.parse(clean);
    return Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    return clean
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
  }
}

async function uploadBatchFile(client, jsonlContent, originalFileName) {
  const blob = new Blob([jsonlContent], { type: "application/jsonl" });
  const file = new File([blob], `batch_${originalFileName}.jsonl`, { type: "application/jsonl" });
  return client.files.create({ file, purpose: "batch" });
}

async function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (d) => chunks.push(d.toString()));
    stream.on("end", () => resolve(chunks.join("")));
    stream.on("error", reject);
  });
}
