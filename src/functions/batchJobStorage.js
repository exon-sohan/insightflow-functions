/**
 * InsightFlow - Batch Job Storage Helper
 * Tracks batch jobs using Azure Blob Storage
 */

const { BlobServiceClient } = require("@azure/storage-blob");

const STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING || process.env.AzureWebJobsStorage;
const CONTAINER = "batch-jobs";

async function getContainer() {
  if (!STORAGE_CONNECTION_STRING) throw new Error("Storage not configured");
  const client = BlobServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);
  const container = client.getContainerClient(CONTAINER);
  await container.createIfNotExists();
  return container;
}

async function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (d) => chunks.push(d.toString()));
    stream.on("end", () => resolve(chunks.join("")));
    stream.on("error", reject);
  });
}

async function createBatchJob(data) {
  const container = await getContainer();
  const job = {
    batchId: data.batchId,
    inputFileId: data.inputFileId,
    inputBlobPath: data.inputBlobPath,
    recordCount: data.recordCount,
    objectName: data.objectName,
    config: data.config,
    status: "pending",
    submittedAt: new Date().toISOString(),
    completedAt: null,
    outputFileId: null,
    errorMessage: null,
    retryCount: 0,
  };

  const blobClient = container.getBlockBlobClient(`pending/${data.batchId}.json`);
  const content = JSON.stringify(job, null, 2);
  await blobClient.upload(content, Buffer.byteLength(content), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  return job;
}

async function getPendingJobs() {
  const container = await getContainer();
  const jobs = [];

  for await (const blob of container.listBlobsFlat({ prefix: "pending/" })) {
    const blobClient = container.getBlockBlobClient(blob.name);
    const response = await blobClient.download(0);
    const content = await streamToString(response.readableStreamBody);
    jobs.push(JSON.parse(content));
  }

  return jobs;
}

async function getJob(batchId) {
  const container = await getContainer();

  for (const folder of ["pending", "completed", "processed", "failed"]) {
    const blobClient = container.getBlockBlobClient(`${folder}/${batchId}.json`);
    if (await blobClient.exists()) {
      const response = await blobClient.download(0);
      const content = await streamToString(response.readableStreamBody);
      return JSON.parse(content);
    }
  }

  return null;
}

async function moveJob(batchId, fromFolder, toFolder, updates = {}) {
  const container = await getContainer();
  const fromBlob = container.getBlockBlobClient(`${fromFolder}/${batchId}.json`);

  const response = await fromBlob.download(0);
  const content = await streamToString(response.readableStreamBody);
  const job = { ...JSON.parse(content), ...updates };

  const toBlob = container.getBlockBlobClient(`${toFolder}/${batchId}.json`);
  const updated = JSON.stringify(job, null, 2);
  await toBlob.upload(updated, Buffer.byteLength(updated), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });

  await fromBlob.deleteIfExists();
  return job;
}

async function markCompleted(batchId, outputFileId, errorFileId = null) {
  return moveJob(batchId, "pending", "completed", {
    status: "completed",
    outputFileId,
    errorFileId,
    completedAt: new Date().toISOString(),
  });
}

async function markProcessed(batchId, outputBlobPath) {
  return moveJob(batchId, "completed", "processed", {
    status: "processed",
    outputBlobPath,
    processedAt: new Date().toISOString(),
  });
}

async function markFailed(batchId, errorMessage) {
  return moveJob(batchId, "pending", "failed", {
    status: "failed",
    errorMessage,
    completedAt: new Date().toISOString(),
  });
}

async function incrementRetry(batchId) {
  const container = await getContainer();
  const blobClient = container.getBlockBlobClient(`pending/${batchId}.json`);

  const response = await blobClient.download(0);
  const content = await streamToString(response.readableStreamBody);
  const job = JSON.parse(content);
  job.retryCount = (job.retryCount || 0) + 1;

  const updated = JSON.stringify(job, null, 2);
  await blobClient.upload(updated, Buffer.byteLength(updated), {
    blobHTTPHeaders: { blobContentType: "application/json" },
    overwrite: true,
  });

  return job;
}

module.exports = {
  createBatchJob,
  getPendingJobs,
  getJob,
  markCompleted,
  markProcessed,
  markFailed,
  incrementRetry,
};
