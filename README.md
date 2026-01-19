# InsightFlow Azure Functions

Official function code repository for InsightFlow Azure Marketplace application.

## Overview

This repository contains the Azure Functions for InsightFlow, which processes Salesforce call reports using Azure OpenAI and syncs insights back to Salesforce. Functions are automatically deployed during marketplace installation via `WEBSITE_RUN_FROM_PACKAGE`.

## Architecture

```
Azure Data Factory → Blob Storage → Function App → Azure OpenAI → Salesforce
                          ↓               ↓
                      datasets/      AI insights
                      
Function App Components:
  - storageBlobTrigger: Process new datasets
  - generateInsightsSummary: Create AI summaries
  - salesforceSyncTrigger: Sync insights to Salesforce
  - triggerPipeline: Manual Data Factory trigger
```

## Functions Included

### 1. storageBlobTrigger.js
**Trigger**: Blob created in `datasets/` container  
**Purpose**: Processes uploaded Salesforce call reports  
**Flow**:
- Triggered when Data Factory uploads JSON to blob storage
- Reads call reports from blob
- Sends to Azure OpenAI for AI-powered analysis
- Writes insights to `output/` container
- Writes summaries to `summaries/` container

### 2. salesforceSyncTrigger.js  
**Trigger**: Blob created in `output/` container  
**Purpose**: Syncs AI insights back to Salesforce  
**Flow**:
- Triggered when insights are written to output container
- Reads processed insights from blob
- Authenticates with Salesforce using OAuth 2.0
- Uses Composite API to batch insert records (25 per request)
- Writes sync status to `sf-sync-status/` container

**Environment Variables**:
- `SF_INSTANCE_URL`: Salesforce instance (from Key Vault)
- `SF_CLIENT_ID`: OAuth client ID (from Key Vault)
- `SF_CLIENT_SECRET`: OAuth client secret (from Key Vault)
- `SF_DESTINATION_OBJECT`: Target Salesforce object (default: `azinsights__c`)

### 3. generateInsightsSummary.js
**Trigger**: HTTP (manual or scheduled)  
**Purpose**: Generates summary reports of AI insights  
**Flow**:
- Reads all insights from `output/` container
- Aggregates statistics and trends
- Creates executive summary
- Stores in `summaries/` container

### 4. triggerPipeline.js
**Trigger**: HTTP endpoint  
**Purpose**: Manually trigger Data Factory pipeline  
**Flow**:
- Authenticates to Azure using Managed Identity
- Calls Data Factory REST API
- Triggers `SalesforceCallReportPipeline`
- Returns pipeline run ID

**Environment Variables**:
- `AZURE_SUBSCRIPTION_ID`: Azure subscription ID
- `AZURE_RESOURCE_GROUP`: Resource group name
- `DATA_FACTORY_NAME`: Data Factory name
- `PIPELINE_NAME`: Pipeline to trigger (default: `SalesforceCallReportPipeline`)

## Deployment

### Automatic (Marketplace)

Functions are deployed automatically via ARM template using `WEBSITE_RUN_FROM_PACKAGE`:

```json
{
  "name": "WEBSITE_RUN_FROM_PACKAGE",
  "value": "https://github.com/exon-sohan/insightflow-functions/releases/download/v1.0.2/functions.zip"
}
```

### Manual Development

1. Clone repository:
```bash
git clone https://github.com/exon-sohan/insightflow-functions.git
cd insightflow-functions
```

2. Install dependencies:
```bash
npm install
```

3. Create `local.settings.json`:
```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "node",
    "AZURE_STORAGE_ACCOUNT_NAME": "your-storage-account",
    "SF_INSTANCE_URL": "https://your-instance.salesforce.com",
    "SF_CLIENT_ID": "your-client-id",
    "SF_CLIENT_SECRET": "your-client-secret",
    "SF_DESTINATION_OBJECT": "azinsights__c",
    "AI_MODEL": "gpt-5-nano",
    "AZURE_AI_PROJECT_ENDPOINT": "https://your-ai-project.openai.azure.com",
    "BATCH_SIZE": "10",
    "DATA_FACTORY_NAME": "your-data-factory",
    "AZURE_SUBSCRIPTION_ID": "your-subscription-id",
    "AZURE_RESOURCE_GROUP": "your-resource-group",
    "PIPELINE_NAME": "SalesforceCallReportPipeline"
  }
}
```

4. Start Functions runtime:
```bash
npm start
# or
func start
```

## Creating Releases

To create a new release for marketplace deployment:

1. Update version in `package.json`

2. Create deployment package:
```bash
# Install production dependencies only
npm ci --production

# Create zip (exclude dev files via .funcignore)
zip -r functions.zip . -x "*.git*" "test/*" "*.vscode/*"
```

3. Create GitHub release:
```bash
git tag v1.0.2
git push origin v1.0.2

# Upload functions.zip as release asset
# GitHub will make it available at:
# https://github.com/exon-sohan/insightflow-functions/releases/download/v1.0.2/functions.zip
```

4. Update marketplace template to point to new release URL

## Environment Variables

All environment variables are configured by the marketplace ARM template:

| Variable | Source | Description |
|----------|--------|-------------|
| `AzureWebJobsStorage` | ARM template | Connection string for blob triggers |
| `AZURE_STORAGE_ACCOUNT_NAME` | ARM template | Storage account name |
| `SF_INSTANCE_URL` | Key Vault | Salesforce instance URL |
| `SF_CLIENT_ID` | Key Vault | Salesforce OAuth client ID |
| `SF_CLIENT_SECRET` | Key Vault | Salesforce OAuth client secret |
| `SF_DESTINATION_OBJECT` | User input | Salesforce object for insights |
| `AI_MODEL` | User input | Azure OpenAI model (gpt-5-nano, etc.) |
| `AZURE_AI_PROJECT_ENDPOINT` | ARM template | Azure AI Project endpoint |
| `BATCH_SIZE` | User input | Records per batch (5-50) |
| `DATA_FACTORY_NAME` | ARM template | Data Factory name |
| `AZURE_SUBSCRIPTION_ID` | ARM template | Subscription ID |
| `AZURE_RESOURCE_GROUP` | ARM template | Resource group name |
| `PIPELINE_NAME` | ARM template | Data Factory pipeline name |

## Storage Containers

| Container | Purpose | Trigger |
|-----------|---------|---------|
| `datasets/` | Raw Salesforce data | storageBlobTrigger |
| `output/` | Processed AI insights | salesforceSyncTrigger |
| `summaries/` | Aggregated reports | - |
| `sf-sync-status/` | Sync operation logs | - |

## Dependencies

```json
{
  "@azure/ai-projects": "^1.0.1",
  "@azure/functions": "^4.10.0",
  "@azure/identity": "^4.13.0",
  "@azure/storage-blob": "^12.29.1"
}
```

## Node.js Version

Requires Node.js 20.x or later (configured in ARM template via `nodeVersion: '~20'`)

## Authentication

### Azure Services
Functions use **Managed Identity** to access:
- Azure Blob Storage (via Storage Blob Data Contributor role)
- Azure Key Vault (via Key Vault Secrets User role)
- Azure OpenAI (via endpoint URL)
- Data Factory (via Contributor role)

### Salesforce
Uses **OAuth 2.0 Client Credentials Flow**:
1. Credentials stored in Key Vault
2. Function retrieves from Key Vault using Managed Identity
3. Obtains access token from Salesforce
4. Makes API calls with bearer token

## Monitoring

All functions log to **Application Insights**:
- Function execution times
- Success/failure rates
- Custom metrics (records processed, insights generated)
- Error traces with stack traces

Access logs in Azure Portal:
```
Function App → Log Stream
Application Insights → Transaction Search
```

## Troubleshooting

### Function Not Triggering
1. Check blob container names match configuration
2. Verify storage account connection string
3. Check Function App → Configuration → Application Settings
4. Review Function App → Log Stream

### Salesforce Sync Fails
1. Verify SF credentials in Key Vault
2. Check SF_DESTINATION_OBJECT exists in Salesforce
3. Verify Salesforce API version compatibility
4. Check Salesforce Remote Site Settings allow Function App URL

### AI Processing Fails
1. Verify Azure OpenAI endpoint is accessible
2. Check AI_MODEL is deployed in Azure OpenAI
3. Verify Managed Identity has Cognitive Services User role
4. Review BATCH_SIZE (reduce if timeouts occur)

## Version History

### v1.0.2 (2026-01-19)
- Added dynamic Salesforce destination object support
- Updated salesforceSyncTrigger to use SF_DESTINATION_OBJECT env var
- Improved error logging
- GitHub repository created for public distribution

### v1.0.1 (2026-01-15)
- Initial marketplace release
- Blob storage migration from CloudFront
- Cost optimization features

### v1.0.0 (2026-01-10)
- Initial version
- Core AI processing functionality
- Salesforce bidirectional sync

## License

MIT License - See LICENSE file for details

## Support

For issues and questions:
- GitHub Issues: https://github.com/exon-sohan/insightflow-functions/issues
- Marketplace Support: Contact via Azure Portal

## Related Repositories

- Infrastructure Templates: https://github.com/exon-sohan/insightflow-bicep-templates
- Documentation: https://github.com/exon-sohan/insightflow-docs

---

**Note**: This repository is designed for Azure Marketplace deployment. For standalone deployments, additional configuration may be required.
