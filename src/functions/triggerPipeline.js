/**
 * InsightFlow - Trigger Pipeline
 * HTTP trigger to start Azure Data Factory pipeline with config storage
 */

const { app } = require("@azure/functions");
const { DefaultAzureCredential } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");

const SUBSCRIPTION_ID = process.env.AZURE_SUBSCRIPTION_ID;
const RESOURCE_GROUP = process.env.AZURE_RESOURCE_GROUP;
const DYNAMIC_DATA_FACTORY_NAME = process.env.DYNAMIC_DATA_FACTORY_NAME || "insightflow-dynamic-adf-poc";
const DYNAMIC_PIPELINE_NAME = process.env.DYNAMIC_PIPELINE_NAME || "DynamicSalesforceExtractPipeline";
const STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING || process.env.AzureWebJobsStorage;

const CONFIG_CONTAINER = "pipeline-configs";

/* ─────────────────────────────────────────────────────────────── */
/* HTTP TRIGGER - Start Pipeline                                   */
/* ─────────────────────────────────────────────────────────────── */
app.http("triggerPipeline", {
  methods: ["POST", "OPTIONS"],
  authLevel: "function",
  handler: async (request, context) => {
    // CORS preflight
    if (request.method === "OPTIONS") {
      return {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization, x-functions-key",
        },
      };
    }

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json",
    };

    try {
      // Validate environment
      if (!SUBSCRIPTION_ID || !RESOURCE_GROUP) {
        return {
          status: 500,
          headers: corsHeaders,
          body: JSON.stringify({
            success: false,
            error: "Missing AZURE_SUBSCRIPTION_ID or AZURE_RESOURCE_GROUP",
          }),
        };
      }

      // Parse request body
      let config = {};
      try {
        const bodyText = await request.text();
        if (bodyText) config = JSON.parse(bodyText);
      } catch {
        return {
          status: 400,
          headers: corsHeaders,
          body: JSON.stringify({ success: false, error: "Invalid JSON body" }),
        };
      }

      // Validate required config
      const { source, analysis } = config;
      if (!source?.object || !source?.fields?.length) {
        return {
          status: 400,
          headers: corsHeaders,
          body: JSON.stringify({
            success: false,
            error: "Missing required: source.object and source.fields",
          }),
        };
      }

      context.log(`Triggering pipeline for object: ${source.object}`);

      // Trigger ADF pipeline
      const credential = new DefaultAzureCredential();
      const token = await credential.getToken("https://management.azure.com/.default");

      const runUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DYNAMIC_DATA_FACTORY_NAME}/pipelines/${DYNAMIC_PIPELINE_NAME}/createRun?api-version=2018-06-01`;

      const pipelineParams = {
        objectApiName: source.object,
        fieldNames: source.fields.join(","),
        whereClause: source.filter || "",
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
        return {
          status: response.status,
          headers: corsHeaders,
          body: JSON.stringify({
            success: false,
            error: `Pipeline trigger failed: ${response.status}`,
            details: errorText,
          }),
        };
      }

      const result = await response.json();
      context.log(`Pipeline triggered: ${result.runId}`);

      // Save config to blob storage for downstream functions
      const configId = await saveConfig(source.object, config, result.runId, context);

      return {
        status: 200,
        headers: corsHeaders,
        body: JSON.stringify({
          success: true,
          message: "Pipeline triggered successfully",
          runId: result.runId,
          configId,
          source: {
            object: source.object,
            fields: source.fields,
            filter: source.filter || null,
          },
          analysis: analysis?.preset || analysis?.type || "default",
          timestamp: new Date().toISOString(),
        }),
      };
    } catch (error) {
      context.error("Error:", error);
      return {
        status: 500,
        headers: corsHeaders,
        body: JSON.stringify({ success: false, error: error.message }),
      };
    }
  },
});

/* ─────────────────────────────────────────────────────────────── */
/* HTTP TRIGGER - Check Pipeline Status                            */
/* ─────────────────────────────────────────────────────────────── */
app.http("checkPipelineStatus", {
  methods: ["GET", "OPTIONS"],
  authLevel: "function",
  handler: async (request, context) => {
    if (request.method === "OPTIONS") {
      return {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization, x-functions-key",
        },
      };
    }

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json",
    };

    const runId = request.query.get("runId");
    if (!runId) {
      return {
        status: 400,
        headers: corsHeaders,
        body: JSON.stringify({ success: false, error: "runId parameter required" }),
      };
    }

    try {
      const credential = new DefaultAzureCredential();
      const token = await credential.getToken("https://management.azure.com/.default");

      const statusUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DYNAMIC_DATA_FACTORY_NAME}/pipelineruns/${runId}?api-version=2018-06-01`;

      const response = await fetch(statusUrl, {
        method: "GET",
        headers: { Authorization: `Bearer ${token.token}` },
      });

      if (!response.ok) {
        const errorText = await response.text();
        return {
          status: response.status,
          headers: corsHeaders,
          body: JSON.stringify({ success: false, error: errorText }),
        };
      }

      const result = await response.json();

      return {
        status: 200,
        headers: corsHeaders,
        body: JSON.stringify({
          success: true,
          runId: result.runId,
          status: result.status,
          message: result.message,
          runStart: result.runStart,
          runEnd: result.runEnd,
        }),
      };
    } catch (error) {
      return {
        status: 500,
        headers: corsHeaders,
        body: JSON.stringify({ success: false, error: error.message }),
      };
    }
  },
});

/* ─────────────────────────────────────────────────────────────── */
/* SAVE CONFIG TO BLOB                                             */
/* ─────────────────────────────────────────────────────────────── */
async function saveConfig(objectName, config, runId, context) {
  try {
    const blobServiceClient = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
    const containerClient = blobServiceClient.getContainerClient(CONFIG_CONTAINER);
    await containerClient.createIfNotExists();

    const configId = `${objectName.toLowerCase()}_${Date.now()}`;
    const configData = {
      ...config,
      configId,
      pipelineRunId: runId,
      createdAt: new Date().toISOString(),
    };

    // Save with object name prefix for blob trigger to find
    const blobName = `${objectName.toLowerCase()}_config.json`;
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    await blockBlobClient.upload(
      JSON.stringify(configData, null, 2),
      JSON.stringify(configData, null, 2).length,
      { blobHTTPHeaders: { blobContentType: "application/json" } }
    );

    context.log(`Config saved: ${blobName}`);
    return configId;
  } catch (err) {
    context.warn(`Failed to save config: ${err.message}`);
    return null;
  }
}
