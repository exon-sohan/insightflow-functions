const { app } = require("@azure/functions");
const { AzureOpenAI } = require("openai");
const { DefaultAzureCredential, getBearerTokenProvider } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");

const AZURE_OPENAI_ENDPOINT = process.env.AZURE_AI_PROJECT_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT_NAME;
const STORAGE_ACCOUNT_NAME = process.env.AZURE_STORAGE_ACCOUNT_NAME;
const INPUT_CONTAINER = "datasets";
const OUTPUT_CONTAINER = "output";

function logger(context) {
  return {
    info: (msg, data) => context.log(`INFO: ${msg}`, data ?? ""),
    success: (msg, data) => context.log(`SUCCESS: ${msg}`, data ?? ""),
    error: (msg, err) => context.error(`ERROR: ${msg}`, err?.stack || err || ""),
  };
}

app.http("processBlobHttp", {
  methods: ["GET", "POST"],
  authLevel: "function",
  handler: async (request, context) => {
    const log = logger(context);

    log.info("HTTP trigger - Manual blob processing started");

    const blobName = request.query.get("blobName") || "latest";

    try {
      const credential = new DefaultAzureCredential();
      const blobServiceClient = new BlobServiceClient(
        `https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
        credential
      );
      const containerClient = blobServiceClient.getContainerClient(INPUT_CONTAINER);

      let blobClient;
      if (blobName === "latest") {
        log.info("Finding latest blob");
        const blobs = [];
        for await (const blob of containerClient.listBlobsFlat()) {
          blobs.push(blob);
        }
        if (blobs.length === 0) {
          return { status: 404, body: "No blobs found" };
        }
        blobs.sort((a, b) => b.properties.createdOn - a.properties.createdOn);
        blobClient = containerClient.getBlobClient(blobs[0].name);
        log.info(`Processing: ${blobs[0].name}`);
      } else {
        blobClient = containerClient.getBlobClient(blobName);
      }

      const downloadResponse = await blobClient.download();
      const chunks = [];
      for await (const chunk of downloadResponse.readableStreamBody) {
        chunks.push(chunk);
      }
      const content = Buffer.concat(chunks).toString("utf8");

      const records = content.split("\n")
        .map(l => l.trim())
        .filter(Boolean)
        .map(line => {
          try { return JSON.parse(line); }
          catch { return null; }
        })
        .filter(Boolean);

      log.info(`Found ${records.length} records`);

      log.info("Initializing Azure OpenAI");
      const scope = "https://cognitiveservices.azure.com/.default";
      const azureADTokenProvider = getBearerTokenProvider(credential, scope);

      const openAIClient = new AzureOpenAI({
        endpoint: AZURE_OPENAI_ENDPOINT,
        azureADTokenProvider,
        deployment: AZURE_OPENAI_DEPLOYMENT,
        apiVersion: "2024-10-21",
      });

      log.success("Azure OpenAI connected");

      const results = [];
      for (const record of records) {
        const recordId = record.Name || record.Id || "Unknown";
        try {
          log.info(`Processing ${recordId}`);
          const response = await openAIClient.chat.completions.create({
            model: AZURE_OPENAI_DEPLOYMENT,
            messages: [
              { role: "system", content: "Analyze call record and return JSON with callSummary and sentiment." },
              { role: "user", content: `Record: ${JSON.stringify(record)}` },
            ],
            temperature: 0.3,
            max_tokens: 300,
            response_format: { type: "json_object" },
          });

          const insights = JSON.parse(response.choices[0].message.content);
          results.push({ success: true, recordId, insights });
          log.success(`Processed ${recordId}`);
        } catch (err) {
          results.push({ success: false, recordId, error: err.message });
          log.error(`Failed ${recordId}`, err);
        }
      }

      const outputName = `${blobClient.name}_processed_${Date.now()}.json`;
      const outputClient = blobServiceClient
        .getContainerClient(OUTPUT_CONTAINER)
        .getBlockBlobClient(outputName);

      await outputClient.upload(
        JSON.stringify(results, null, 2),
        JSON.stringify(results).length,
        { blobHTTPHeaders: { blobContentType: "application/json" } }
      );

      log.success(`Saved to ${outputName}`);

      return {
        status: 200,
        jsonBody: {
          message: "Complete",
          blob: blobClient.name,
          output: outputName,
          totalRecords: records.length,
          successful: results.filter(r => r.success).length
        }
      };

    } catch (err) {
      log.error("Processing failed", err);
      return { status: 500, jsonBody: { error: err.message } };
    }
  },
});
