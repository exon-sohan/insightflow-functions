/**
 * Batch Job Storage Helper
 * Uses Azure Blob Storage to track batch job status
 * Jobs are stored as JSON files in a "batch-jobs" container
 */

const { BlobServiceClient } = require("@azure/storage-blob");

const STORAGE_CONNECTION_STRING =
  process.env.AZURE_STORAGE_CONNECTION_STRING ||
  process.env.AzureWebJobsStorage;

const BATCH_JOBS_CONTAINER = "batch-jobs";

/**
 * Get or create the container client
 */
async function getContainerClient() {
  if (!STORAGE_CONNECTION_STRING) {
    throw new Error("Storage connection string not configured");
  }

  const blobServiceClient = BlobServiceClient.fromConnectionString(
    STORAGE_CONNECTION_STRING
  );
  const containerClient = blobServiceClient.getContainerClient(BATCH_JOBS_CONTAINER);

  // Create container if it doesn't exist
  await containerClient.createIfNotExists();

  return containerClient;
}

/**
 * Create a new batch job record
 * @param {Object} jobData - Batch job data
 * @returns {Promise<Object>} Created job data
 */
async function createBatchJob(jobData) {
  const containerClient = await getContainerClient();

  const job = {
    batchId: jobData.batchId,
    inputFileId: jobData.inputFileId,
    status: "pending",
    inputBlobPath: jobData.inputBlobPath,
    recordCount: jobData.recordCount,
    submittedAt: new Date().toISOString(),
    completedAt: null,
    outputFileId: null,
    errorFileId: null,
    errorMessage: null,
    retryCount: 0,
  };

  const blobName = `pending/${jobData.batchId}.json`;
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);

  const content = JSON.stringify(job, null, 2);
  await blockBlobClient.upload(content, Buffer.byteLength(content), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  return job;
}

/**
 * Get all pending batch jobs
 * @returns {Promise<Array>} List of pending jobs
 */
async function getPendingBatchJobs() {
  const containerClient = await getContainerClient();
  const jobs = [];

  // List all blobs in the "pending/" folder
  for await (const blob of containerClient.listBlobsFlat({ prefix: "pending/" })) {
    const blobClient = containerClient.getBlockBlobClient(blob.name);
    const downloadResponse = await blobClient.download(0);
    const content = await streamToString(downloadResponse.readableStreamBody);
    jobs.push(JSON.parse(content));
  }

  return jobs;
}

/**
 * Get a specific batch job by ID
 * @param {string} batchId - Batch job ID
 * @returns {Promise<Object|null>} Batch job or null
 */
async function getBatchJob(batchId) {
  const containerClient = await getContainerClient();

  // Check pending folder first
  let blobName = `pending/${batchId}.json`;
  let blobClient = containerClient.getBlockBlobClient(blobName);

  if (await blobClient.exists()) {
    const downloadResponse = await blobClient.download(0);
    const content = await streamToString(downloadResponse.readableStreamBody);
    return JSON.parse(content);
  }

  // Check completed folder
  blobName = `completed/${batchId}.json`;
  blobClient = containerClient.getBlockBlobClient(blobName);

  if (await blobClient.exists()) {
    const downloadResponse = await blobClient.download(0);
    const content = await streamToString(downloadResponse.readableStreamBody);
    return JSON.parse(content);
  }

  // Check failed folder
  blobName = `failed/${batchId}.json`;
  blobClient = containerClient.getBlockBlobClient(blobName);

  if (await blobClient.exists()) {
    const downloadResponse = await blobClient.download(0);
    const content = await streamToString(downloadResponse.readableStreamBody);
    return JSON.parse(content);
  }

  return null;
}

/**
 * Update and move batch job to completed status
 * @param {string} batchId - Batch job ID
 * @param {string} outputFileId - Output file ID from batch API
 * @param {string|null} errorFileId - Error file ID if any
 */
async function markBatchCompleted(batchId, outputFileId, errorFileId = null) {
  const containerClient = await getContainerClient();

  // Get existing job from pending
  const pendingBlobName = `pending/${batchId}.json`;
  const pendingBlobClient = containerClient.getBlockBlobClient(pendingBlobName);

  const downloadResponse = await pendingBlobClient.download(0);
  const content = await streamToString(downloadResponse.readableStreamBody);
  const job = JSON.parse(content);

  // Update job data
  job.status = "completed";
  job.outputFileId = outputFileId;
  job.errorFileId = errorFileId;
  job.completedAt = new Date().toISOString();

  // Save to completed folder
  const completedBlobName = `completed/${batchId}.json`;
  const completedBlobClient = containerClient.getBlockBlobClient(completedBlobName);
  const updatedContent = JSON.stringify(job, null, 2);

  await completedBlobClient.upload(updatedContent, Buffer.byteLength(updatedContent), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  // Delete from pending folder
  await pendingBlobClient.deleteIfExists();

  return job;
}

/**
 * Mark batch job as failed
 * @param {string} batchId - Batch job ID
 * @param {string} errorMessage - Error message
 */
async function markBatchFailed(batchId, errorMessage) {
  const containerClient = await getContainerClient();

  // Get existing job from pending
  const pendingBlobName = `pending/${batchId}.json`;
  const pendingBlobClient = containerClient.getBlockBlobClient(pendingBlobName);

  const downloadResponse = await pendingBlobClient.download(0);
  const content = await streamToString(downloadResponse.readableStreamBody);
  const job = JSON.parse(content);

  // Update job data
  job.status = "failed";
  job.errorMessage = errorMessage;
  job.completedAt = new Date().toISOString();

  // Save to failed folder
  const failedBlobName = `failed/${batchId}.json`;
  const failedBlobClient = containerClient.getBlockBlobClient(failedBlobName);
  const updatedContent = JSON.stringify(job, null, 2);

  await failedBlobClient.upload(updatedContent, Buffer.byteLength(updatedContent), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  // Delete from pending folder
  await pendingBlobClient.deleteIfExists();

  return job;
}

/**
 * Mark batch job as processed (results saved to output)
 * @param {string} batchId - Batch job ID
 * @param {string} outputBlobPath - Path to output blob
 */
async function markBatchProcessed(batchId, outputBlobPath) {
  const containerClient = await getContainerClient();

  // Get existing job from completed
  const completedBlobName = `completed/${batchId}.json`;
  const completedBlobClient = containerClient.getBlockBlobClient(completedBlobName);

  const downloadResponse = await completedBlobClient.download(0);
  const content = await streamToString(downloadResponse.readableStreamBody);
  const job = JSON.parse(content);

  // Update job data
  job.status = "processed";
  job.outputBlobPath = outputBlobPath;
  job.processedAt = new Date().toISOString();

  // Save to processed folder
  const processedBlobName = `processed/${batchId}.json`;
  const processedBlobClient = containerClient.getBlockBlobClient(processedBlobName);
  const updatedContent = JSON.stringify(job, null, 2);

  await processedBlobClient.upload(updatedContent, Buffer.byteLength(updatedContent), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  // Delete from completed folder
  await completedBlobClient.deleteIfExists();

  return job;
}

/**
 * Increment retry count for a pending batch job
 * @param {string} batchId - Batch job ID
 */
async function incrementRetryCount(batchId) {
  const containerClient = await getContainerClient();

  const blobName = `pending/${batchId}.json`;
  const blobClient = containerClient.getBlockBlobClient(blobName);

  const downloadResponse = await blobClient.download(0);
  const content = await streamToString(downloadResponse.readableStreamBody);
  const job = JSON.parse(content);

  job.retryCount = (job.retryCount || 0) + 1;

  const updatedContent = JSON.stringify(job, null, 2);
  await blobClient.upload(updatedContent, Buffer.byteLength(updatedContent), {
    blobHTTPHeaders: { blobContentType: "application/json" },
    overwrite: true,
  });

  return job;
}

/**
 * Helper: Convert stream to string
 */
async function streamToString(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", (data) => {
      chunks.push(data.toString());
    });
    readableStream.on("end", () => {
      resolve(chunks.join(""));
    });
    readableStream.on("error", reject);
  });
}

module.exports = {
  createBatchJob,
  getPendingBatchJobs,
  getBatchJob,
  markBatchCompleted,
  markBatchFailed,
  markBatchProcessed,
  incrementRetryCount,
};
