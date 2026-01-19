# InsightFlow Azure Functions

Official function code repository for InsightFlow Azure Marketplace application.

## Overview

This repository contains the Azure Functions code for InsightFlow, deployed automatically during marketplace installation.

## Functions Included

1. **blobTrigger** - Processes uploaded datasets
2. **salesforceSyncTrigger** - Syncs AI insights back to Salesforce
3. **healthCheck** - Health monitoring endpoint
4. **triggerPipeline** - Triggers Data Factory pipeline
5. **setupInstructions** - Returns setup guide

## Deployment

Functions are automatically deployed via WEBSITE_RUN_FROM_PACKAGE pointing to GitHub releases.

## Version

Current: v1.0.2

