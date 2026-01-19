const { app } = require("@azure/functions");
const { BlobServiceClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");

const STORAGE_ACCOUNT_NAME = process.env.AZURE_STORAGE_ACCOUNT_NAME;
const OUTPUT_CONTAINER = "output";
const SUMMARY_CONTAINER = "summaries";

/* ───────────────────────────────────────────────────────────── */
/* HTTP TRIGGER - Generate Executive Summary                     */
/* ───────────────────────────────────────────────────────────── */
app.http("generateInsightsSummary", {
  methods: ["GET", "POST"],
  authLevel: "function",
  handler: async (request, context) => {
    context.log("Generating executive insights summary...");

    try {
      // Get date range from query params (optional)
      const url = new URL(request.url);
      const daysBack = parseInt(url.searchParams.get("days") || "30");
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysBack);

      // Validate storage account name
      if (!STORAGE_ACCOUNT_NAME) {
        return {
          status: 500,
          jsonBody: { error: "AZURE_STORAGE_ACCOUNT_NAME environment variable is not set" },
        };
      }

      // Connect to blob storage using Managed Identity
      const credential = new DefaultAzureCredential();
      const blobServiceClient = new BlobServiceClient(
        `https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
        credential
      );
      const containerClient = blobServiceClient.getContainerClient(OUTPUT_CONTAINER);

      // Find the latest processed file only
      const allInsights = [];
      let latestBlob = null;
      let latestDate = null;

      context.log(`Scanning for the latest processed file...`);

      // First pass: find the most recent file
      for await (const blob of containerClient.listBlobsFlat()) {
        if (blob.name.endsWith(".json")) {
          const blobDate = new Date(blob.properties.createdOn);
          if (!latestDate || blobDate > latestDate) {
            latestDate = blobDate;
            latestBlob = blob;
          }
        }
      }

      // Process only the latest file
      if (latestBlob) {
        context.log(`Processing latest file: ${latestBlob.name} (${latestDate.toISOString()})`);
        const blobClient = containerClient.getBlobClient(latestBlob.name);
        const downloadResponse = await blobClient.download();
        const content = await streamToString(downloadResponse.readableStreamBody);

        try {
          const records = JSON.parse(content);
          if (Array.isArray(records)) {
            for (const record of records) {
              if (record.success && record.agentResponse?.insights) {
                allInsights.push({
                  recordId: record.originalRecord?.Name || record.originalRecord?.$ || "Unknown",
                  processedAt: record.processedAt,
                  insights: record.agentResponse.insights,
                });
              }
            }
          }
        } catch (parseErr) {
          context.log(`Error parsing file: ${latestBlob.name}`, parseErr);
        }
      }

      context.log(`Found ${allInsights.length} records from latest file`);

      if (allInsights.length === 0) {
        return {
          status: 200,
          jsonBody: {
            message: "No processed records found in the specified date range",
            daysBack,
            cutoffDate: cutoffDate.toISOString(),
          },
        };
      }

      // Generate aggregated summary
      const summary = generateExecutiveSummary(allInsights, daysBack);

      // Save summary to blob storage
      const summaryContainerClient = blobServiceClient.getContainerClient(SUMMARY_CONTAINER);
      await summaryContainerClient.createIfNotExists();

      const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
      const summaryBlobName = `executive-summary-${timestamp}.json`;
      const summaryBlob = summaryContainerClient.getBlockBlobClient(summaryBlobName);

      const summaryJson = JSON.stringify(summary, null, 2);
      await summaryBlob.upload(summaryJson, Buffer.byteLength(summaryJson), {
        blobHTTPHeaders: { blobContentType: "application/json" },
      });

      context.log(`Summary saved to ${SUMMARY_CONTAINER}/${summaryBlobName}`);

      return {
        status: 200,
        jsonBody: summary,
      };
    } catch (err) {
      context.error("Failed to generate summary:", err);
      return {
        status: 500,
        jsonBody: { error: err.message },
      };
    }
  },
});

/* ───────────────────────────────────────────────────────────── */
/* AGGREGATION LOGIC                                             */
/* ───────────────────────────────────────────────────────────── */
function generateExecutiveSummary(insights, daysBack) {
  const now = new Date();
  const totalCalls = insights.length;

  // Initialize counters
  const flags = {
    hasProductMention: 0,
    hasQualityComplaint: 0,
    hasDeliveryComplaint: 0,
    hasEscalationRisk: 0,
    hasCompetitorMention: 0,
    hasUpsellOpportunity: 0,
    hasComplianceIssue: 0,
    hasActionItem: 0,
    hasFeatureRequest: 0,
    hasPricingConcern: 0,
    hasTechnicalIssue: 0,
  };

  const sentiments = { Positive: 0, Neutral: 0, Negative: 0 };
  const escalationLevels = { High: 0, Medium: 0, Low: 0 };
  const resolutionStatuses = {};

  // Track confidence scores for average
  let totalConfidence = 0;
  let confidenceCount = 0;

  // Track estimated values for pipeline
  const valueMapping = { High: 50000, Medium: 20000, Low: 5000 };
  let estimatedPipelineValue = 0;

  const productMentions = {};
  const qualityIssues = { categories: {}, details: [] };
  const deliveryIssues = { categories: {}, details: [] };
  const technicalIssues = { categories: {}, details: [] };
  const pricingConcerns = { types: {}, details: [] };
  const featureRequests = { productAreas: {}, details: [] };
  const competitors = {};
  const upsellOpportunities = [];
  const highRiskAccounts = [];
  const allActionItems = [];
  const allTopics = {};

  // Detailed records for drill-down
  const detailedRecords = {
    bySentiment: { Positive: [], Neutral: [], Negative: [] },
    byFlag: {
      productMention: [],
      qualityComplaint: [],
      deliveryComplaint: [],
      escalationRisk: [],
      competitorMention: [],
      upsellOpportunity: [],
      complianceIssue: [],
      actionItem: [],
      featureRequest: [],
      pricingConcern: [],
      technicalIssue: [],
    },
    byProduct: {},
    byEscalationLevel: { High: [], Medium: [], Low: [] },
    byResolutionStatus: {},
    all: [],
  };

  // Aggregate data
  for (const record of insights) {
    const i = record.insights;
    if (!i || i.parseError) continue;

    // Build record summary for drill-down
    const recordSummary = {
      recordId: record.recordId,
      processedAt: record.processedAt,
      callSummary: i.callSummary || "No summary available",
      sentiment: i.sentiment?.score || "Unknown",
      sentimentConfidence: i.sentiment?.confidence || 0,
      sentimentReason: i.sentiment?.reason || "",
      escalationRisk: i.escalationRisk?.level || "Unknown",
      escalationReason: i.escalationRisk?.reason || "",
      resolutionStatus: i.resolutionStatus || "Unknown",
      products: i.products?.mentioned || [],
      primaryProduct: i.products?.primary || null,
      qualityComplaint: i.qualityComplaint?.detected ? {
        category: i.qualityComplaint.category,
        severity: i.qualityComplaint.severity,
        details: i.qualityComplaint.details,
      } : null,
      deliveryComplaint: i.deliveryComplaint?.detected ? {
        category: i.deliveryComplaint.category,
        severity: i.deliveryComplaint.severity,
        details: i.deliveryComplaint.details,
      } : null,
      technicalIssue: i.technicalIssue?.detected ? {
        category: i.technicalIssue.category,
        severity: i.technicalIssue.severity,
        details: i.technicalIssue.details,
      } : null,
      pricingConcern: i.pricingConcern?.detected ? {
        type: i.pricingConcern.type,
        severity: i.pricingConcern.severity,
        details: i.pricingConcern.details,
      } : null,
      featureRequest: i.featureRequest?.detected ? {
        feature: i.featureRequest.feature,
        priority: i.featureRequest.priority,
        productArea: i.featureRequest.productArea,
        details: i.featureRequest.details,
      } : null,
      upsellOpportunity: i.upsellOpportunity?.detected ? {
        products: i.upsellOpportunity.products,
        estimatedValue: i.upsellOpportunity.estimatedValue,
        reason: i.upsellOpportunity.reason,
      } : null,
      competitors: i.competitorMention?.competitors || [],
      actionItems: i.actionItems || [],
      keyTopics: i.keyTopics || [],
      flags: i.flags || {},
      // New detailed fields
      emotion: i.emotion || null,
      customerIntent: i.customerIntent || null,
      businessImpact: i.businessImpact || null,
      complianceRisk: i.complianceRisk || null,
      callQuality: i.callQuality || null,
      followUp: i.followUp || null,
    };

    // Add to all records
    detailedRecords.all.push(recordSummary);

    // Flags
    for (const [flag, value] of Object.entries(i.flags || {})) {
      if (value && flags.hasOwnProperty(flag)) {
        flags[flag]++;
      }
    }

    // Index by flags
    if (i.flags?.hasProductMention) detailedRecords.byFlag.productMention.push(recordSummary);
    if (i.flags?.hasQualityComplaint) detailedRecords.byFlag.qualityComplaint.push(recordSummary);
    if (i.flags?.hasDeliveryComplaint) detailedRecords.byFlag.deliveryComplaint.push(recordSummary);
    if (i.flags?.hasEscalationRisk) detailedRecords.byFlag.escalationRisk.push(recordSummary);
    if (i.flags?.hasCompetitorMention) detailedRecords.byFlag.competitorMention.push(recordSummary);
    if (i.flags?.hasUpsellOpportunity) detailedRecords.byFlag.upsellOpportunity.push(recordSummary);
    if (i.flags?.hasComplianceIssue) detailedRecords.byFlag.complianceIssue.push(recordSummary);
    if (i.flags?.hasActionItem) detailedRecords.byFlag.actionItem.push(recordSummary);
    if (i.flags?.hasFeatureRequest) detailedRecords.byFlag.featureRequest.push(recordSummary);
    if (i.flags?.hasPricingConcern) detailedRecords.byFlag.pricingConcern.push(recordSummary);
    if (i.flags?.hasTechnicalIssue) detailedRecords.byFlag.technicalIssue.push(recordSummary);

    // Track confidence score for average
    if (i.sentiment?.confidence !== undefined && typeof i.sentiment.confidence === 'number') {
      totalConfidence += i.sentiment.confidence;
      confidenceCount++;
    }

    // Sentiment
    const sentScore = i.sentiment?.score;
    if (sentScore && sentiments.hasOwnProperty(sentScore)) {
      sentiments[sentScore]++;
      detailedRecords.bySentiment[sentScore].push(recordSummary);
    }

    // Escalation Risk
    const escLevel = i.escalationRisk?.level;
    if (escLevel && escalationLevels.hasOwnProperty(escLevel)) {
      escalationLevels[escLevel]++;
      detailedRecords.byEscalationLevel[escLevel].push(recordSummary);
    }

    // High risk accounts
    if (escLevel === "High" || i.escalationRisk?.accountAtRisk) {
      highRiskAccounts.push({
        recordId: record.recordId,
        reason: i.escalationRisk?.reason || "High risk detected",
        sentiment: sentScore,
      });
    }

    // Resolution Status
    const resStat = i.resolutionStatus;
    if (resStat) {
      resolutionStatuses[resStat] = (resolutionStatuses[resStat] || 0) + 1;
      if (!detailedRecords.byResolutionStatus[resStat]) {
        detailedRecords.byResolutionStatus[resStat] = [];
      }
      detailedRecords.byResolutionStatus[resStat].push(recordSummary);
    }

    // Products
    for (const product of i.products?.mentioned || []) {
      const key = product.toLowerCase().trim();
      productMentions[key] = (productMentions[key] || 0) + 1;
      if (!detailedRecords.byProduct[key]) {
        detailedRecords.byProduct[key] = [];
      }
      detailedRecords.byProduct[key].push(recordSummary);
    }

    // Quality Complaints
    if (i.qualityComplaint?.detected) {
      const cat = i.qualityComplaint.category || "Unknown";
      qualityIssues.categories[cat] = (qualityIssues.categories[cat] || 0) + 1;
      qualityIssues.details.push({
        recordId: record.recordId,
        category: cat,
        severity: i.qualityComplaint.severity,
        details: i.qualityComplaint.details,
      });
    }

    // Delivery Complaints
    if (i.deliveryComplaint?.detected) {
      const cat = i.deliveryComplaint.category || "Unknown";
      deliveryIssues.categories[cat] = (deliveryIssues.categories[cat] || 0) + 1;
      deliveryIssues.details.push({
        recordId: record.recordId,
        category: cat,
        severity: i.deliveryComplaint.severity,
        details: i.deliveryComplaint.details,
      });
    }

    // Technical Issues
    if (i.technicalIssue?.detected) {
      const cat = i.technicalIssue.category || "Unknown";
      technicalIssues.categories[cat] = (technicalIssues.categories[cat] || 0) + 1;
      technicalIssues.details.push({
        recordId: record.recordId,
        category: cat,
        severity: i.technicalIssue.severity,
        details: i.technicalIssue.details,
      });
    }

    // Pricing Concerns
    if (i.pricingConcern?.detected) {
      const type = i.pricingConcern.type || "Unknown";
      pricingConcerns.types[type] = (pricingConcerns.types[type] || 0) + 1;
      pricingConcerns.details.push({
        recordId: record.recordId,
        type: type,
        severity: i.pricingConcern.severity,
        details: i.pricingConcern.details,
      });
    }

    // Feature Requests
    if (i.featureRequest?.detected) {
      const area = i.featureRequest.productArea || "General";
      featureRequests.productAreas[area] = (featureRequests.productAreas[area] || 0) + 1;
      featureRequests.details.push({
        recordId: record.recordId,
        feature: i.featureRequest.feature,
        priority: i.featureRequest.priority,
        productArea: area,
        details: i.featureRequest.details,
      });
    }

    // Competitors
    for (const comp of i.competitorMention?.competitors || []) {
      const key = comp.toLowerCase().trim();
      competitors[key] = (competitors[key] || 0) + 1;
    }

    // Upsell Opportunities
    if (i.upsellOpportunity?.detected) {
      const estValue = i.upsellOpportunity.estimatedValue;
      upsellOpportunities.push({
        recordId: record.recordId,
        products: i.upsellOpportunity.products,
        estimatedValue: estValue,
        reason: i.upsellOpportunity.reason,
      });
      // Track pipeline value
      if (estValue && valueMapping[estValue]) {
        estimatedPipelineValue += valueMapping[estValue];
      }
    }

    // Action Items
    for (const action of i.actionItems || []) {
      allActionItems.push({
        recordId: record.recordId,
        ...action,
      });
    }

    // Topics - handle both string and object format
    for (const topic of i.keyTopics || []) {
      const topicName = typeof topic === 'string' ? topic : (topic?.topic || '');
      if (topicName) {
        const key = topicName.toLowerCase().trim();
        allTopics[key] = (allTopics[key] || 0) + 1;
      }
    }
  }

  // Sort and rank
  const topProducts = sortByValue(productMentions).slice(0, 10);
  const topTopics = sortByValue(allTopics).slice(0, 10);
  const topCompetitors = sortByValue(competitors).slice(0, 5);

  const highPriorityActions = allActionItems
    .filter((a) => a.priority === "High")
    .slice(0, 20);

  const upsellByValue = {
    High: upsellOpportunities.filter((u) => u.estimatedValue === "High"),
    Medium: upsellOpportunities.filter((u) => u.estimatedValue === "Medium"),
    Low: upsellOpportunities.filter((u) => u.estimatedValue === "Low"),
  };

  // Calculate percentages
  const pct = (n) => totalCalls > 0 ? ((n / totalCalls) * 100).toFixed(1) : 0;

  // Build summary
  return {
    metadata: {
      generatedAt: now.toISOString(),
      periodDays: daysBack,
      periodStart: new Date(now - daysBack * 24 * 60 * 60 * 1000).toISOString(),
      periodEnd: now.toISOString(),
      totalCallsAnalyzed: totalCalls,
    },

    executiveSummary: {
      headline: generateHeadline(sentiments, totalCalls, highRiskAccounts.length),
      keyMetrics: {
        totalCalls,
        positiveSentimentRate: `${pct(sentiments.Positive)}%`,
        negativeSentimentRate: `${pct(sentiments.Negative)}%`,
        qualityComplaintRate: `${pct(flags.hasQualityComplaint)}%`,
        deliveryComplaintRate: `${pct(flags.hasDeliveryComplaint)}%`,
        upsellOpportunityRate: `${pct(flags.hasUpsellOpportunity)}%`,
        highRiskAccountCount: highRiskAccounts.length,
        actionItemsGenerated: allActionItems.length,
      },
      keyInsights: generateKeyInsights({
        sentiments,
        totalCalls,
        highRiskAccounts,
        upsellOpportunities,
        flags,
        topProducts,
        qualityIssues,
      }),
    },

    sentimentAnalysis: {
      distribution: {
        positive: { count: sentiments.Positive, percentage: `${pct(sentiments.Positive)}%` },
        neutral: { count: sentiments.Neutral, percentage: `${pct(sentiments.Neutral)}%` },
        negative: { count: sentiments.Negative, percentage: `${pct(sentiments.Negative)}%` },
      },
      trend: sentiments.Positive >= sentiments.Negative ? "Favorable" : "Needs Attention",
      averageConfidence: confidenceCount > 0 ? totalConfidence / confidenceCount : 0,
    },

    flagsSummary: {
      productMentions: { count: flags.hasProductMention, percentage: `${pct(flags.hasProductMention)}%` },
      qualityComplaints: { count: flags.hasQualityComplaint, percentage: `${pct(flags.hasQualityComplaint)}%` },
      deliveryComplaints: { count: flags.hasDeliveryComplaint, percentage: `${pct(flags.hasDeliveryComplaint)}%` },
      escalationRisks: { count: flags.hasEscalationRisk, percentage: `${pct(flags.hasEscalationRisk)}%` },
      competitorMentions: { count: flags.hasCompetitorMention, percentage: `${pct(flags.hasCompetitorMention)}%` },
      upsellOpportunities: { count: flags.hasUpsellOpportunity, percentage: `${pct(flags.hasUpsellOpportunity)}%` },
      complianceIssues: { count: flags.hasComplianceIssue, percentage: `${pct(flags.hasComplianceIssue)}%` },
      actionItems: { count: flags.hasActionItem, percentage: `${pct(flags.hasActionItem)}%` },
      featureRequests: { count: flags.hasFeatureRequest, percentage: `${pct(flags.hasFeatureRequest)}%` },
      pricingConcerns: { count: flags.hasPricingConcern, percentage: `${pct(flags.hasPricingConcern)}%` },
      technicalIssues: { count: flags.hasTechnicalIssue, percentage: `${pct(flags.hasTechnicalIssue)}%` },
    },

    escalationRisk: {
      distribution: {
        high: { count: escalationLevels.High, percentage: `${pct(escalationLevels.High)}%` },
        medium: { count: escalationLevels.Medium, percentage: `${pct(escalationLevels.Medium)}%` },
        low: { count: escalationLevels.Low, percentage: `${pct(escalationLevels.Low)}%` },
      },
      highRiskAccounts: highRiskAccounts.slice(0, 20),
      recommendation: highRiskAccounts.length > 0
        ? `Immediate attention required for ${highRiskAccounts.length} high-risk account(s)`
        : "No high-risk accounts identified",
    },

    productInsights: {
      topMentionedProducts: topProducts,
      totalProductMentions: flags.hasProductMention,
    },

    qualityComplaintsAnalysis: {
      totalComplaints: flags.hasQualityComplaint,
      byCategory: qualityIssues.categories,
      topIssues: qualityIssues.details.slice(0, 10),
      recommendation: flags.hasQualityComplaint > 0
        ? `Review ${flags.hasQualityComplaint} quality complaint(s) for root cause analysis`
        : "No quality complaints reported",
    },

    deliveryComplaintsAnalysis: {
      totalComplaints: flags.hasDeliveryComplaint,
      byCategory: deliveryIssues.categories,
      topIssues: deliveryIssues.details.slice(0, 10),
      recommendation: flags.hasDeliveryComplaint > 0
        ? `Review ${flags.hasDeliveryComplaint} delivery issue(s) with logistics team`
        : "No delivery complaints reported",
    },

    technicalIssuesAnalysis: {
      totalIssues: flags.hasTechnicalIssue,
      byCategory: technicalIssues.categories,
      topIssues: technicalIssues.details.slice(0, 10),
      recommendation: flags.hasTechnicalIssue > 0
        ? `Review ${flags.hasTechnicalIssue} technical issue(s) with engineering team`
        : "No technical issues reported",
    },

    pricingConcernsAnalysis: {
      totalConcerns: flags.hasPricingConcern,
      byType: pricingConcerns.types,
      topConcerns: pricingConcerns.details.slice(0, 10),
      recommendation: flags.hasPricingConcern > 0
        ? `Review ${flags.hasPricingConcern} pricing concern(s) for competitive positioning`
        : "No pricing concerns reported",
    },

    featureRequestsAnalysis: {
      totalRequests: flags.hasFeatureRequest,
      byProductArea: featureRequests.productAreas,
      topRequests: featureRequests.details.slice(0, 10),
      recommendation: flags.hasFeatureRequest > 0
        ? `Review ${flags.hasFeatureRequest} feature request(s) for product roadmap`
        : "No feature requests reported",
    },

    competitiveIntelligence: {
      totalMentions: flags.hasCompetitorMention,
      topCompetitors: topCompetitors,
      recommendation: topCompetitors.length > 0
        ? `Monitor competitive activity from: ${topCompetitors.map((c) => c.name).join(", ")}`
        : "No significant competitor mentions",
    },

    salesOpportunities: {
      totalOpportunities: flags.hasUpsellOpportunity,
      estimatedTotalValue: formatCurrency(estimatedPipelineValue),
      byEstimatedValue: {
        high: upsellByValue.High.length,
        medium: upsellByValue.Medium.length,
        low: upsellByValue.Low.length,
      },
      byProduct: groupOpportunitiesByProduct(upsellOpportunities),
      topOpportunities: upsellOpportunities.slice(0, 10),
      recommendation: upsellByValue.High.length > 0
        ? `Prioritize ${upsellByValue.High.length} high-value upsell opportunities`
        : "Continue nurturing existing opportunities",
    },

    resolutionStatus: {
      distribution: resolutionStatuses,
      pendingFollowUps: resolutionStatuses["Requires Follow-up"] || 0,
      escalated: resolutionStatuses["Escalated"] || 0,
    },

    qualityMetrics: {
      resolutionRates: Object.fromEntries(
        Object.entries(resolutionStatuses).map(([status, count]) => [
          status,
          { count, percentage: `${pct(count)}%` }
        ])
      ),
      callQualityIndicators: {
        withActionItems: { count: flags.hasActionItem, percentage: `${pct(flags.hasActionItem)}%` },
        withProductMentions: { count: flags.hasProductMention, percentage: `${pct(flags.hasProductMention)}%` },
        withEscalation: { count: flags.hasEscalationRisk, percentage: `${pct(flags.hasEscalationRisk)}%` },
      },
      complianceFlags: {
        complianceIssues: flags.hasComplianceIssue,
      },
    },

    actionItemsSummary: {
      totalGenerated: allActionItems.length,
      highPriority: highPriorityActions.length,
      topHighPriorityActions: highPriorityActions,
      byOwner: groupActionsByOwner(allActionItems),
    },

    topDiscussionTopics: topTopics,

    recommendations: generateRecommendations({
      sentiments,
      highRiskAccounts,
      qualityIssues,
      deliveryIssues,
      technicalIssues,
      pricingConcerns,
      featureRequests,
      upsellOpportunities,
      totalCalls,
    }),

    // Detailed records for drill-down functionality
    detailedRecords,
  };
}

/* ───────────────────────────────────────────────────────────── */
/* HELPER FUNCTIONS                                              */
/* ───────────────────────────────────────────────────────────── */
function sortByValue(obj) {
  return Object.entries(obj)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ name, count }));
}

function groupActionsByOwner(actions) {
  const groups = {};
  for (const action of actions) {
    const owner = action.owner || "Unassigned";
    if (!groups[owner]) groups[owner] = 0;
    groups[owner]++;
  }
  return groups;
}

function formatCurrency(value) {
  if (value >= 1000000) {
    return `$${(value / 1000000).toFixed(1)}M`;
  } else if (value >= 1000) {
    return `$${(value / 1000).toFixed(0)}K`;
  } else if (value > 0) {
    return `$${value}`;
  }
  return "$0";
}

function groupOpportunitiesByProduct(opportunities) {
  const groups = {};
  for (const opp of opportunities) {
    for (const product of opp.products || []) {
      const key = product.toLowerCase().trim();
      if (!groups[key]) groups[key] = { count: 0, opportunities: [] };
      groups[key].count++;
      groups[key].opportunities.push(opp);
    }
  }
  return groups;
}

function generateKeyInsights(data) {
  const insights = [];
  const posRate = data.totalCalls > 0 ? (data.sentiments.Positive / data.totalCalls) * 100 : 0;
  const negRate = data.totalCalls > 0 ? (data.sentiments.Negative / data.totalCalls) * 100 : 0;

  // Sentiment insight
  if (posRate >= 70) {
    insights.push(`Strong customer satisfaction with ${posRate.toFixed(0)}% positive sentiment across ${data.totalCalls} calls analyzed.`);
  } else if (negRate > 30) {
    insights.push(`Customer satisfaction concerns detected with ${negRate.toFixed(0)}% negative sentiment requiring attention.`);
  } else {
    insights.push(`Mixed sentiment patterns observed across ${data.totalCalls} calls with ${posRate.toFixed(0)}% positive feedback.`);
  }

  // High risk accounts
  if (data.highRiskAccounts.length > 0) {
    insights.push(`${data.highRiskAccounts.length} high-risk account(s) identified requiring immediate executive attention.`);
  }

  // Upsell opportunities
  const highValueOps = data.upsellOpportunities.filter(u => u.estimatedValue === 'High').length;
  if (highValueOps > 0) {
    insights.push(`${highValueOps} high-value upsell opportunities detected representing significant revenue potential.`);
  } else if (data.upsellOpportunities.length > 0) {
    insights.push(`${data.upsellOpportunities.length} upsell opportunities identified across customer interactions.`);
  }

  // Quality issues
  if (data.flags.hasQualityComplaint > 0) {
    insights.push(`${data.flags.hasQualityComplaint} quality complaints logged - recommend root cause analysis.`);
  }

  // Top product
  if (data.topProducts.length > 0) {
    insights.push(`"${data.topProducts[0].name}" is the most discussed product with ${data.topProducts[0].count} mentions.`);
  }

  return insights.slice(0, 4);
}

function generateHeadline(sentiments, totalCalls, highRiskCount) {
  const posRate = totalCalls > 0 ? (sentiments.Positive / totalCalls) * 100 : 0;

  if (posRate >= 80) {
    return `Strong Performance: ${posRate.toFixed(0)}% positive sentiment across ${totalCalls} calls`;
  } else if (posRate >= 60) {
    return `Stable Performance: ${posRate.toFixed(0)}% positive sentiment with ${highRiskCount} accounts needing attention`;
  } else {
    return `Attention Required: Only ${posRate.toFixed(0)}% positive sentiment - review recommended`;
  }
}

function generateRecommendations(data) {
  const recs = [];

  // Sentiment-based
  const negRate = data.totalCalls > 0 ? (data.sentiments.Negative / data.totalCalls) * 100 : 0;
  if (negRate > 20) {
    recs.push({
      priority: "High",
      category: "Customer Satisfaction",
      recommendation: `Negative sentiment at ${negRate.toFixed(0)}% - conduct root cause analysis and implement improvement plan`,
    });
  }

  // High risk accounts
  if (data.highRiskAccounts.length > 0) {
    recs.push({
      priority: "High",
      category: "Account Retention",
      recommendation: `Schedule executive outreach for ${data.highRiskAccounts.length} high-risk account(s) within 48 hours`,
    });
  }

  // Quality issues
  if (data.qualityIssues.details.length > 5) {
    recs.push({
      priority: "High",
      category: "Quality",
      recommendation: `Escalate ${data.qualityIssues.details.length} quality complaints to QA team for immediate review`,
    });
  }

  // Delivery issues
  if (data.deliveryIssues.details.length > 5) {
    recs.push({
      priority: "Medium",
      category: "Logistics",
      recommendation: `Review delivery SLAs - ${data.deliveryIssues.details.length} complaints logged`,
    });
  }

  // Upsell opportunities
  const highValueUpsells = data.upsellOpportunities.filter((u) => u.estimatedValue === "High");
  if (highValueUpsells.length > 0) {
    recs.push({
      priority: "Medium",
      category: "Revenue",
      recommendation: `Pursue ${highValueUpsells.length} high-value upsell opportunities identified`,
    });
  }

  // Technical issues
  if (data.technicalIssues && data.technicalIssues.details.length > 3) {
    recs.push({
      priority: "High",
      category: "Technical",
      recommendation: `Address ${data.technicalIssues.details.length} technical issues with engineering team`,
    });
  }

  // Pricing concerns
  if (data.pricingConcerns && data.pricingConcerns.details.length > 3) {
    recs.push({
      priority: "Medium",
      category: "Pricing",
      recommendation: `Review ${data.pricingConcerns.details.length} pricing concerns for competitive positioning`,
    });
  }

  // Feature requests
  if (data.featureRequests && data.featureRequests.details.length > 3) {
    recs.push({
      priority: "Medium",
      category: "Product",
      recommendation: `Evaluate ${data.featureRequests.details.length} feature requests for product roadmap`,
    });
  }

  // Default if no issues
  if (recs.length === 0) {
    recs.push({
      priority: "Low",
      category: "General",
      recommendation: "Continue monitoring - no critical issues identified",
    });
  }

  return recs;
}

async function streamToString(readableStream) {
  const chunks = [];
  for await (const chunk of readableStream) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}
