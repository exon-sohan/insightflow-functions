const { app } = require("@azure/functions");
const { DefaultAzureCredential } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");

// Azure Data Factory configuration - all values from environment variables
const SUBSCRIPTION_ID = process.env.AZURE_SUBSCRIPTION_ID;
const RESOURCE_GROUP = process.env.AZURE_RESOURCE_GROUP;
const DATA_FACTORY_NAME = process.env.DATA_FACTORY_NAME;
const PIPELINE_NAME = process.env.PIPELINE_NAME || "SalesforceCallReportPipeline";

// Dynamic pipeline configuration
const DYNAMIC_DATA_FACTORY_NAME = process.env.DYNAMIC_DATA_FACTORY_NAME || "insightflow-dynamic-adf-poc";
const DYNAMIC_PIPELINE_NAME = process.env.DYNAMIC_PIPELINE_NAME || "DynamicSalesforceExtractPipeline";

// Storage configuration
const STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING || process.env.AzureWebJobsStorage;
const PROMPT_CONFIG_CONTAINER = "prompt-configs";

/* ───────────────────────────────────────────────────────────────────────────
   HTTP TRIGGER - Start Data Factory Pipeline
   ─────────────────────────────────────────────────────────────────────────── */
app.http("triggerPipeline", {
  methods: ["POST", "OPTIONS"],
  authLevel: "function",
  handler: async (request, context) => {
    // Handle CORS preflight
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

    context.log("Pipeline trigger requested");

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json",
    };

    try {
      // Validate required environment variables
      if (!SUBSCRIPTION_ID || !RESOURCE_GROUP || !DATA_FACTORY_NAME) {
        return {
          status: 500,
          headers: corsHeaders,
          body: JSON.stringify({
            success: false,
            error: "Missing required environment variables: AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, or DATA_FACTORY_NAME",
          }),
        };
      }

      // Parse request body for optional dynamic parameters
      let requestBody = {};
      try {
        const bodyText = await request.text();
        if (bodyText) {
          requestBody = JSON.parse(bodyText);
        }
      } catch (parseError) {
        context.log("No JSON body or parse error, using defaults");
      }

      // Extract dynamic parameters (if provided, use dynamic pipeline)
      const { objectApiName, fieldNames, whereClause, systemPrompt, userPromptTemplate } = requestBody;
      const useDynamicPipeline = objectApiName && fieldNames;

      // Use Managed Identity to authenticate
      const credential = new DefaultAzureCredential();
      const token = await credential.getToken("https://management.azure.com/.default");

      let runUrl, pipelineParams, targetPipelineName, targetFactoryName;

      if (useDynamicPipeline) {
        // Use the dynamic Salesforce extraction pipeline
        targetFactoryName = DYNAMIC_DATA_FACTORY_NAME;
        targetPipelineName = DYNAMIC_PIPELINE_NAME;
        runUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${targetFactoryName}/pipelines/${targetPipelineName}/createRun?api-version=2018-06-01`;

        pipelineParams = {
          objectApiName: objectApiName,
          fieldNames: fieldNames,
          whereClause: whereClause || ""
        };

        context.log(`Triggering dynamic pipeline: ${targetPipelineName} with objectApiName=${objectApiName}, fieldNames=${fieldNames}`);
      } else {
        // Use the default pipeline (original behavior)
        targetFactoryName = DATA_FACTORY_NAME;
        targetPipelineName = PIPELINE_NAME;
        runUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${targetFactoryName}/pipelines/${targetPipelineName}/createRun?api-version=2018-06-01`;
        pipelineParams = {};

        context.log(`Triggering default pipeline: ${targetPipelineName}`);
      }

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
        context.error("Pipeline trigger failed:", response.status, errorText);
        return {
          status: response.status,
          headers: corsHeaders,
          body: JSON.stringify({
            success: false,
            error: `Failed to trigger pipeline: ${response.status}`,
            details: errorText,
          }),
        };
      }

      const result = await response.json();
      context.log("Pipeline triggered successfully:", result.runId);

      // If custom prompts provided, store them in blob storage for the blob trigger to use
      let promptConfigSaved = false;
      if (useDynamicPipeline && (systemPrompt || userPromptTemplate)) {
        try {
          const promptConfig = {
            objectApiName,
            fieldNames,
            systemPrompt: systemPrompt || null,
            userPromptTemplate: userPromptTemplate || null,
            createdAt: new Date().toISOString(),
            pipelineRunId: result.runId
          };

          // Save prompt config to blob storage
          const blobServiceClient = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
          const containerClient = blobServiceClient.getContainerClient(PROMPT_CONFIG_CONTAINER);

          // Ensure container exists
          await containerClient.createIfNotExists();

          // Save config with objectApiName as the key (blob trigger will look for this)
          const configBlobName = `${objectApiName.toLowerCase()}_config.json`;
          const blockBlobClient = containerClient.getBlockBlobClient(configBlobName);
          await blockBlobClient.upload(JSON.stringify(promptConfig, null, 2), JSON.stringify(promptConfig, null, 2).length, {
            blobHTTPHeaders: { blobContentType: "application/json" }
          });

          context.log(`Prompt config saved to ${PROMPT_CONFIG_CONTAINER}/${configBlobName}`);
          promptConfigSaved = true;
        } catch (storageError) {
          context.warn("Failed to save prompt config (non-fatal):", storageError.message);
        }
      }

      const responseBody = {
        success: true,
        message: "Pipeline triggered successfully",
        runId: result.runId,
        pipelineName: targetPipelineName,
        dataFactoryName: targetFactoryName,
        timestamp: new Date().toISOString(),
      };

      // Include dynamic parameters in response if used
      if (useDynamicPipeline) {
        responseBody.parameters = {
          objectApiName,
          fieldNames,
          whereClause: whereClause || ""
        };
        if (systemPrompt || userPromptTemplate) {
          responseBody.customPrompts = {
            systemPromptProvided: !!systemPrompt,
            userPromptTemplateProvided: !!userPromptTemplate,
            configSaved: promptConfigSaved
          };
        }
      }

      return {
        status: 200,
        headers: corsHeaders,
        body: JSON.stringify(responseBody),
      };
    } catch (error) {
      context.error("Error triggering pipeline:", error);
      return {
        status: 500,
        headers: corsHeaders,
        body: JSON.stringify({
          success: false,
          error: error.message,
        }),
      };
    }
  },
});

/* ───────────────────────────────────────────────────────────────────────────
   HTTP TRIGGER - Check Pipeline Run Status
   ─────────────────────────────────────────────────────────────────────────── */
app.http("checkPipelineStatus", {
  methods: ["GET", "OPTIONS"],
  authLevel: "function",
  handler: async (request, context) => {
    // Handle CORS preflight
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

    const runId = request.query.get("runId");
    const factoryName = request.query.get("factoryName"); // Optional: specify which factory to check

    if (!runId) {
      return {
        status: 400,
        headers: { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" },
        body: JSON.stringify({ success: false, error: "runId parameter is required" }),
      };
    }

    // Use specified factory or default
    const targetFactoryName = factoryName || DATA_FACTORY_NAME;

    context.log(`Checking pipeline status for runId: ${runId} in factory: ${targetFactoryName}`);

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json",
    };

    try {
      // Validate required environment variables
      if (!SUBSCRIPTION_ID || !RESOURCE_GROUP || !DATA_FACTORY_NAME) {
        return {
          status: 500,
          headers: corsHeaders,
          body: JSON.stringify({
            success: false,
            error: "Missing required environment variables: AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, or DATA_FACTORY_NAME",
          }),
        };
      }

      const credential = new DefaultAzureCredential();
      const token = await credential.getToken("https://management.azure.com/.default");

      const statusUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${targetFactoryName}/pipelineruns/${runId}?api-version=2018-06-01`;

      const response = await fetch(statusUrl, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token.token}`,
        },
      });

      if (!response.ok) {
        const errorText = await response.text();
        return {
          status: response.status,
          headers: corsHeaders,
          body: JSON.stringify({
            success: false,
            error: `Failed to get pipeline status: ${response.status}`,
            details: errorText,
          }),
        };
      }

      const result = await response.json();

      return {
        status: 200,
        headers: corsHeaders,
        body: JSON.stringify({
          success: true,
          runId: result.runId,
          pipelineName: result.pipelineName,
          dataFactoryName: targetFactoryName,
          status: result.status,
          message: result.message,
          runStart: result.runStart,
          runEnd: result.runEnd,
          durationInMs: result.durationInMs,
          parameters: result.parameters || {},
        }),
      };
    } catch (error) {
      context.error("Error checking pipeline status:", error);
      return {
        status: 500,
        headers: corsHeaders,
        body: JSON.stringify({
          success: false,
          error: error.message,
        }),
      };
    }
  },
});
