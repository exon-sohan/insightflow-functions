const { app } = require("@azure/functions");
const { BlobServiceClient } = require("@azure/storage-blob");

const STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;

// Salesforce Connected App credentials (OAuth 2.0 Client Credentials Flow - same as Data Factory)
// All values must be provided via environment variables - no hardcoded defaults
const SF_INSTANCE_URL = process.env.SF_INSTANCE_URL;
const SF_CLIENT_ID = process.env.SF_CLIENT_ID;
const SF_CLIENT_SECRET = process.env.SF_CLIENT_SECRET;
const SF_DESTINATION_OBJECT = process.env.SF_DESTINATION_OBJECT || 'azinsights__c';

/* ───────────────────────────────────────────────────────────── */
/* LOGGER                                                        */
/* ───────────────────────────────────────────────────────────── */
function logger(context) {
  return {
    info: (msg, data) => context.log(`ℹ️ [SF-SYNC] ${msg}`, data ?? ""),
    success: (msg, data) => context.log(`✅ [SF-SYNC] ${msg}`, data ?? ""),
    warn: (msg, data) => context.warn(`⚠️ [SF-SYNC] ${msg}`, data ?? ""),
    error: (msg, err) => context.error(`❌ [SF-SYNC] ${msg}`, err?.stack || err || ""),
  };
}

/* ───────────────────────────────────────────────────────────── */
/* BLOB TRIGGER - Sync processed insights to Salesforce          */
/* ───────────────────────────────────────────────────────────── */
app.storageBlob("salesforceSyncTrigger", {
  path: "output/{name}",
  connection: "AzureWebJobsStorage",
  handler: async (blob, context) => {
    const log = logger(context);
    const fileName = context.triggerMetadata.name;
    const startTime = Date.now();

    // Only process JSON files that contain processed insights
    if (!fileName.endsWith(".json") || !fileName.includes("_processed_")) {
      log.info(`Skipping non-processed file: ${fileName}`);
      return;
    }

    log.info("════════════════════════════════════════════");
    log.info("Salesforce Sync Trigger fired");
    log.info(`File name: ${fileName}`);
    log.info(`File size: ${blob.length} bytes`);
    log.info("════════════════════════════════════════════");

    try {
      // Parse the processed records
      const content = blob.toString("utf8");
      const records = JSON.parse(content);

      if (!Array.isArray(records)) {
        log.warn("Expected array of records, skipping");
        return;
      }

      log.info(`Found ${records.length} records to sync to Salesforce`);

      // Filter successful records only
      const successfulRecords = records.filter((r) => r.success !== false);
      log.info(`${successfulRecords.length} successful records to process`);

      if (successfulRecords.length === 0) {
        log.info("No successful records to sync");
        return;
      }

      // Authenticate with Salesforce
      log.info("Authenticating with Salesforce...");
      const sfAuth = await authenticateSalesforce(log);

      if (!sfAuth) {
        log.error("Failed to authenticate with Salesforce");
        return;
      }

      log.success("Authenticated with Salesforce", { instanceUrl: sfAuth.instanceUrl });

      // Transform and upload records
      const transformedRecords = successfulRecords.map(transformRecord);
      log.info(`Transformed ${transformedRecords.length} records for Salesforce`);
      log.info(`Target Salesforce object: ${SF_DESTINATION_OBJECT}`);

      // Upload in batches
      const batchSize = 25; // Salesforce Composite API limit is 25 subrequests
      const results = await uploadToSalesforce(sfAuth, transformedRecords, batchSize, log);

      const successCount = results.filter((r) => r.success).length;
      const failCount = results.filter((r) => !r.success).length;

      log.success("Salesforce sync completed", {
        total: transformedRecords.length,
        success: successCount,
        failed: failCount,
        durationMs: Date.now() - startTime,
      });

      // Save sync status to blob
      await saveSyncStatus(fileName, {
        syncedAt: new Date().toISOString(),
        totalRecords: transformedRecords.length,
        successCount,
        failCount,
        results: results.slice(0, 50), // Keep first 50 for reference
      }, log);

    } catch (err) {
      log.error("Salesforce sync failed", err);
      throw err;
    }
  },
});

/* ───────────────────────────────────────────────────────────── */
/* SALESFORCE AUTHENTICATION (OAuth 2.0 Client Credentials Flow) */
/* ───────────────────────────────────────────────────────────── */
async function authenticateSalesforce(log) {
  if (!SF_INSTANCE_URL || !SF_CLIENT_ID || !SF_CLIENT_SECRET) {
    log.error("Missing Salesforce credentials in environment variables");
    log.error("Required: SF_INSTANCE_URL, SF_CLIENT_ID, SF_CLIENT_SECRET");
    return null;
  }

  try {
    // Use OAuth 2.0 Client Credentials flow (same as Data Factory)
    const tokenUrl = `${SF_INSTANCE_URL}/services/oauth2/token`;

    const params = new URLSearchParams();
    params.append("grant_type", "client_credentials");
    params.append("client_id", SF_CLIENT_ID);
    params.append("client_secret", SF_CLIENT_SECRET);

    log.info(`Authenticating to ${SF_INSTANCE_URL}...`);

    const response = await fetch(tokenUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: params,
    });

    if (!response.ok) {
      const errorText = await response.text();
      log.error("Salesforce auth failed", { status: response.status, error: errorText });
      return null;
    }

    const data = await response.json();
    return {
      accessToken: data.access_token,
      instanceUrl: data.instance_url || SF_INSTANCE_URL,
    };
  } catch (err) {
    log.error("Salesforce auth error", err);
    return null;
  }
}

/* ───────────────────────────────────────────────────────────── */
/* TRANSFORM RECORD TO SALESFORCE FORMAT                         */
/* ───────────────────────────────────────────────────────────── */
function transformRecord(record) {
  const original = record.originalRecord || {};
  const insights = record.agentResponse?.insights || {};
  const flags = insights.flags || {};
  const sentiment = insights.sentiment || {};
  const businessImpact = insights.businessImpact || {};
  const customerIntent = insights.customerIntent || {};
  const products = insights.products || {};
  const escalationRisk = insights.escalationRisk || {};
  const followUp = insights.followUp || {};
  const analysisMeta = insights.analysisMeta || {};
  const upsell = insights.upsellOpportunity || {};
  const qualityComplaint = insights.qualityComplaint || {};
  const deliveryComplaint = insights.deliveryComplaint || {};
  const technicalIssue = insights.technicalIssue || {};
  const pricingConcern = insights.pricingConcern || {};
  const featureRequest = insights.featureRequest || {};
  const competitorMention = insights.competitorMention || {};

  // Build action items as JSON string
  const actionItemsStr = insights.actionItems?.length
    ? JSON.stringify(insights.actionItems, null, 2)
    : null;

  // Build key topics as string
  const keyTopicsStr = insights.keyTopics?.length
    ? insights.keyTopics.map((t) => (typeof t === "string" ? t : t.topic)).join("; ")
    : null;

  // Build competitors as string
  const competitorsStr = competitorMention.competitors?.length
    ? competitorMention.competitors.join(", ")
    : null;

  // Build products mentioned as string
  const productsMentionedStr = products.mentioned?.length
    ? products.mentioned.join(", ")
    : null;

  // Build upsell products as string
  const upsellProductsStr = upsell.products?.length
    ? upsell.products.join(", ")
    : null;

  return {
    OriginalRecordId__c: original["$"] || original.Id || null,
    RecordName__c: original.Name || null,
    CallSummary__c: truncate(insights.callSummary, 131072),

    // Sentiment
    SentimentScore__c: sentiment.score || null,
    SentimentConfidence__c: sentiment.confidence ? sentiment.confidence * 100 : null,
    SentimentEmotion__c: sentiment.emotion || null,
    SentimentReason__c: truncate(sentiment.reason, 255),

    // Customer Intent
    CustomerIntentPrimary__c: customerIntent.primary || null,
    IntentConfidence__c: customerIntent.confidence ? customerIntent.confidence * 100 : null,

    // Business Impact
    RevenueImpact__c: businessImpact.revenueImpact || null,
    ChurnRisk__c: businessImpact.churnRisk || null,
    AccountHealth__c: businessImpact.accountHealth || null,
    BusinessImpactReason__c: truncate(businessImpact.reason, 255),

    // Products
    PrimaryProduct__c: products.primary || null,
    ProductsMentioned__c: truncate(productsMentionedStr, 255),

    // Flags (checkboxes)
    HasProductMention__c: flags.hasProductMention || false,
    HasQualityComplaint__c: flags.hasQualityComplaint || false,
    HasDeliveryComplaint__c: flags.hasDeliveryComplaint || false,
    HasEscalationRisk__c: flags.hasEscalationRisk || false,
    HasCompetitorMention__c: flags.hasCompetitorMention || false,
    HasUpsellOpportunity__c: flags.hasUpsellOpportunity || false,
    HasActionItem__c: flags.hasActionItem || false,
    HasFeatureRequest__c: flags.hasFeatureRequest || false,
    HasPricingConcern__c: flags.hasPricingConcern || false,
    HasTechnicalIssue__c: flags.hasTechnicalIssue || false,

    // Escalation
    EscalationRiskLevel__c: escalationRisk.level || null,
    EscalationReason__c: truncate(escalationRisk.reason, 255),

    // Resolution & Follow-up
    ResolutionStatus__c: insights.resolutionStatus || null,
    FollowUpRequired__c: followUp.required || false,
    FollowUpDate__c: followUp.recommendedDate || null,
    FollowUpReason__c: truncate(followUp.reason, 255),

    // Detailed Issues
    QualityComplaintDetails__c: qualityComplaint.detected
      ? truncate(JSON.stringify(qualityComplaint), 131072)
      : null,
    DeliveryComplaintDetails__c: deliveryComplaint.detected
      ? truncate(JSON.stringify(deliveryComplaint), 131072)
      : null,
    TechnicalIssueDetails__c: technicalIssue.detected
      ? truncate(JSON.stringify(technicalIssue), 131072)
      : null,
    PricingConcernDetails__c: pricingConcern.detected
      ? truncate(JSON.stringify(pricingConcern), 131072)
      : null,
    FeatureRequestDetails__c: featureRequest.detected
      ? truncate(JSON.stringify(featureRequest), 131072)
      : null,

    // Upsell
    UpsellProducts__c: truncate(upsellProductsStr, 255),
    UpsellValue__c: upsell.estimatedValue || null,
    UpsellReason__c: truncate(upsell.reason, 255),

    // Competitors
    Competitors__c: truncate(competitorsStr, 255),

    // Action Items & Topics
    ActionItems__c: truncate(actionItemsStr, 131072),
    KeyTopics__c: truncate(keyTopicsStr, 255),

    // Meta
    AnalysisConfidence__c: analysisMeta.confidenceScore
      ? analysisMeta.confidenceScore * 100
      : null,
    RawInsightsJSON__c: truncate(JSON.stringify(record.agentResponse, null, 2), 131072),
    ProcessedDate__c: new Date().toISOString(),
  };
}

/* ───────────────────────────────────────────────────────────── */
/* UPLOAD TO SALESFORCE                                          */
/* ───────────────────────────────────────────────────────────── */
async function uploadToSalesforce(auth, records, batchSize, log) {
  const results = [];
  const batches = chunkArray(records, batchSize);

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    log.info(`Processing batch ${i + 1}/${batches.length} (${batch.length} records)`);

    try {
      // Use Salesforce Composite API for bulk insert
      const compositeRequest = {
        allOrNone: false,
        compositeRequest: batch.map((record, idx) => ({
          method: "POST",
          url: `/services/data/v65.0/sobjects/${SF_DESTINATION_OBJECT}`,
          referenceId: `record_${i}_${idx}`,
          body: record,
        })),
      };

      const response = await fetch(
        `${auth.instanceUrl}/services/data/v65.0/composite`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${auth.accessToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(compositeRequest),
        }
      );

      if (!response.ok) {
        const errorText = await response.text();
        log.error(`Batch ${i + 1} failed`, { status: response.status, error: errorText });

        // Mark all records in this batch as failed
        batch.forEach((_, idx) => {
          results.push({
            success: false,
            referenceId: `record_${i}_${idx}`,
            error: errorText,
          });
        });
        continue;
      }

      const data = await response.json();

      // Process composite response
      for (const result of data.compositeResponse || []) {
        results.push({
          success: result.httpStatusCode >= 200 && result.httpStatusCode < 300,
          referenceId: result.referenceId,
          id: result.body?.id,
          error: result.body?.message || (result.httpStatusCode >= 400 ? JSON.stringify(result.body) : null),
        });
      }

      const batchSuccess = (data.compositeResponse || []).filter(
        (r) => r.httpStatusCode >= 200 && r.httpStatusCode < 300
      ).length;

      log.info(`Batch ${i + 1} completed: ${batchSuccess}/${batch.length} successful`);

    } catch (err) {
      log.error(`Batch ${i + 1} error`, err);
      batch.forEach((_, idx) => {
        results.push({
          success: false,
          referenceId: `record_${i}_${idx}`,
          error: err.message,
        });
      });
    }
  }

  return results;
}

/* ───────────────────────────────────────────────────────────── */
/* SAVE SYNC STATUS                                              */
/* ───────────────────────────────────────────────────────────── */
async function saveSyncStatus(originalFileName, status, log) {
  try {
    if (!STORAGE_CONNECTION_STRING) {
      log.warn("AZURE_STORAGE_CONNECTION_STRING not set, skipping sync status save");
      return;
    }

    const blobServiceClient = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
    const containerClient = blobServiceClient.getContainerClient("sf-sync-status");
    await containerClient.createIfNotExists();

    const base = originalFileName.replace(/\.[^/.]+$/, "");
    const statusBlobName = `${base}_sf-sync-status.json`;

    const blockBlob = containerClient.getBlockBlobClient(statusBlobName);
    const body = JSON.stringify(status, null, 2);

    await blockBlob.upload(body, Buffer.byteLength(body), {
      blobHTTPHeaders: { blobContentType: "application/json" },
    });

    log.success("Sync status saved", { statusBlobName });
  } catch (err) {
    log.warn("Failed to save sync status", err.message);
  }
}

/* ───────────────────────────────────────────────────────────── */
/* HELPERS                                                       */
/* ───────────────────────────────────────────────────────────── */
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

function truncate(str, maxLength) {
  if (!str) return null;
  if (typeof str !== "string") str = String(str);
  return str.length > maxLength ? str.substring(0, maxLength - 3) + "..." : str;
}
