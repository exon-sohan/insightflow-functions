// Entry point for Azure Functions v4 Programming Model
// This file imports and registers all function handlers

require("./functions/triggerPipeline");
require("./functions/storageBlobTrigger");
require("./functions/salesforceSyncTrigger");
require("./functions/generateInsightsSummary");
require("./functions/processBlobHttp");
