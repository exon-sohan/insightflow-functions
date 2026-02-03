// InsightFlow v1.0.3 - Entry point for Azure Functions
// AI-powered Salesforce analytics with ADF WriteBack integration

require("./functions/triggerPipeline");
require("./functions/storageBlobTrigger");
require("./functions/batchStatusChecker");
