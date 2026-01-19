const { app } = require("@azure/functions");
const { DefaultAzureCredential } = require("@azure/identity");

// Azure Data Factory configuration - all values from environment variables
const SUBSCRIPTION_ID = process.env.AZURE_SUBSCRIPTION_ID;
const RESOURCE_GROUP = process.env.AZURE_RESOURCE_GROUP;
const DATA_FACTORY_NAME = process.env.DATA_FACTORY_NAME;
const PIPELINE_NAME = process.env.PIPELINE_NAME || "SalesforceCallReportPipeline";

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

      // Use Managed Identity to authenticate
      const credential = new DefaultAzureCredential();
      const token = await credential.getToken("https://management.azure.com/.default");

      // Trigger the Data Factory pipeline using REST API
      const runUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATA_FACTORY_NAME}/pipelines/${PIPELINE_NAME}/createRun?api-version=2018-06-01`;

      context.log(`Triggering pipeline: ${PIPELINE_NAME}`);

      const response = await fetch(runUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token.token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
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

      return {
        status: 200,
        headers: corsHeaders,
        body: JSON.stringify({
          success: true,
          message: "Pipeline triggered successfully",
          runId: result.runId,
          pipelineName: PIPELINE_NAME,
          timestamp: new Date().toISOString(),
        }),
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

    if (!runId) {
      return {
        status: 400,
        headers: { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" },
        body: JSON.stringify({ success: false, error: "runId parameter is required" }),
      };
    }

    context.log(`Checking pipeline status for runId: ${runId}`);

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

      const statusUrl = `https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATA_FACTORY_NAME}/pipelineruns/${runId}?api-version=2018-06-01`;

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
          status: result.status,
          message: result.message,
          runStart: result.runStart,
          runEnd: result.runEnd,
          durationInMs: result.durationInMs,
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
