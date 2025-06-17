const fs = require("fs");
const axios = require("axios");
const readline = require("readline");
const path = require("path");

// Load environment variables from .env file if it exists
try {
  require("dotenv").config();
} catch (error) {
  // dotenv not installed or .env file doesn't exist - that's fine
}

// Configuration from environment variables with fallbacks
const POSTHOG_HOST = process.env.POSTHOG_HOST || "https://us.i.posthog.com";
const POSTHOG_KEY =
  process.env.POSTHOG_KEY || "phc_lMFe26SNqbJSIRzFhPurJc0UwsqtJgOP6ubY9BzwhfT";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;
const SLEEP_TIME = parseInt(process.env.SLEEP_TIME) || 2500;
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 3;
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY) || 5000;

const inputFilePath =
  process.env.INPUT_FILE || "data/posthog_events/processed/final.jsonl";
const trackingFilePath =
  process.env.TRACKING_FILE || "data/posthog_events/processed/tracking.txt";

// Stats tracking
let stats = {
  totalProcessed: 0,
  successfulBatches: 0,
  failedBatches: 0,
  startTime: Date.now(),
  lastBatchTime: Date.now(),
};

// Graceful shutdown handling
let isShuttingDown = false;
let isProcessing = false;

// Helper function to log with timestamp
function log(message, level = "INFO") {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] ${message}`);
}

// Helper function to sleep
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Read last processed line from tracking file
function getLastProcessedLine() {
  try {
    if (fs.existsSync(trackingFilePath)) {
      const content = fs.readFileSync(trackingFilePath, "utf8").trim();
      return parseInt(content) || 0;
    }
  } catch (error) {
    log(`Error reading tracking file: ${error.message}`, "WARN");
  }
  return 0;
}

// Update tracking file with current line number
function updateTracking(lineNumber) {
  try {
    // Ensure directory exists
    const dir = path.dirname(trackingFilePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(trackingFilePath, lineNumber.toString());
  } catch (error) {
    log(`Error updating tracking file: ${error.message}`, "ERROR");
  }
}

// Process a batch with retry logic
async function processBatch(batchIndex, events, retryCount = 0) {
  const batchId = `${batchIndex + 1}`;

  try {
    log(
      `Processing batch ${batchId} (${events.length} events)${
        retryCount > 0 ? ` - Retry ${retryCount}` : ""
      }`
    );

    await axios.post(
      `${POSTHOG_HOST}/batch/`,
      {
        batch: events,
        api_key: POSTHOG_KEY,
        historical_migration: true,
      },
      {
        headers: { "Content-Type": "application/json" },
        timeout: 30000, // 30 second timeout
      }
    );
    log(`Batch ${batchId} processed successfully`);

    // Only increment stats once per successful batch
    stats.successfulBatches++;
    stats.totalProcessed += events.length;

    return true;
  } catch (error) {
    const errorMsg = error.response?.data?.message || error.message;
    log(`Batch ${batchId} failed: ${errorMsg}`, "ERROR");

    // Retry logic
    if (retryCount < MAX_RETRIES && !isShuttingDown) {
      log(`Retrying batch ${batchId} in ${RETRY_DELAY}ms...`, "WARN");
      await sleep(RETRY_DELAY);
      return processBatch(batchIndex, events, retryCount + 1);
    } else {
      log(
        `Batch ${batchId} permanently failed after ${retryCount} retries`,
        "ERROR"
      );
      stats.failedBatches++;
      return false;
    }
  }
}

// Print progress statistics
function printStats() {
  const elapsed = (Date.now() - stats.startTime) / 1000;
  const rate = stats.totalProcessed / elapsed;

  log(
    `Progress: ${stats.totalProcessed} events processed | ` +
      `${stats.successfulBatches} successful batches | ` +
      `${stats.failedBatches} failed batches | ` +
      `${rate.toFixed(2)} events/sec | ` +
      `${elapsed.toFixed(1)}s elapsed`
  );
}

// Graceful shutdown handler
async function gracefulShutdown(signal) {
  log(`Received ${signal}, initiating graceful shutdown...`, "WARN");
  isShuttingDown = true;

  // Wait for current processing to complete
  while (isProcessing) {
    log("Waiting for current batch to complete...", "WARN");
    await sleep(1000);
  }

  printStats();
  log("Migration script stopped gracefully");
  process.exit(0);
}

// Main migration function
async function runMigration() {
  log("Starting PostHog migration...");
  log(
    `Configuration: BATCH_SIZE=${BATCH_SIZE}, SLEEP_TIME=${SLEEP_TIME}ms, MAX_RETRIES=${MAX_RETRIES}`
  );

  // Validate input file exists
  if (!fs.existsSync(inputFilePath)) {
    log(`Input file not found: ${inputFilePath}`, "ERROR");
    process.exit(1);
  }

  const lastProcessedLine = getLastProcessedLine();
  log(`Resuming from line ${lastProcessedLine + 1}`);

  const readStream = fs.createReadStream(inputFilePath, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });

  let events = [];
  let batchIndex = 0;
  let lineNumber = 0;
  let skippedLines = 0;
  let lineQueue = [];
  let processingQueue = false;

  // Queue-based processing to handle async properly
  async function processLineQueue() {
    if (processingQueue || isShuttingDown) return;
    processingQueue = true;

    while (lineQueue.length > 0 && !isShuttingDown) {
      const line = lineQueue.shift();
      lineNumber++;

      // Skip already processed lines
      if (lineNumber <= lastProcessedLine) {
        skippedLines++;
        continue;
      }

      try {
        const event = JSON.parse(line);
        events.push({
          event: event.event,
          timestamp: event.timestamp,
          properties: {
            ...event.properties,
            distinct_id: event.distinct_id,
          },
        });

        // Process batch when it's full
        if (events.length >= BATCH_SIZE) {
          isProcessing = true;

          await processBatch(batchIndex, events);

          // Update tracking after processing attempt
          updateTracking(lineNumber);

          // Print stats every 10 batches
          if (batchIndex % 10 === 0) {
            printStats();
          }

          events = [];
          batchIndex++;

          // Sleep between batches
          if (!isShuttingDown) {
            await sleep(SLEEP_TIME);
          }

          isProcessing = false;
        }
      } catch (error) {
        log(`Error parsing line ${lineNumber}: ${error.message}`, "ERROR");
      }
    }

    processingQueue = false;
  }

  // Add lines to queue
  rl.on("line", (line) => {
    lineQueue.push(line);
    // Process queue if not already processing
    if (!processingQueue) {
      setImmediate(processLineQueue);
    }
  });

  // Handle completion
  rl.on("close", async () => {
    // Wait for any remaining queued lines to be processed
    while (lineQueue.length > 0 && !isShuttingDown) {
      await processLineQueue();
      await sleep(100);
    }

    if (skippedLines > 0) {
      log(`Skipped ${skippedLines} already processed lines`);
    }

    // Process final batch if it has events
    if (events.length > 0 && !isShuttingDown) {
      log(`Processing final batch of ${events.length} events...`);
      isProcessing = true;
      await processBatch(batchIndex, events);
      updateTracking(lineNumber);
      isProcessing = false;
    }

    // Final statistics
    printStats();
    log("Migration completed successfully!");

    // Clean up tracking file on successful completion
    if (stats.failedBatches === 0) {
      try {
        fs.unlinkSync(trackingFilePath);
        log("Tracking file removed - migration fully completed");
      } catch (error) {
        log(`Could not remove tracking file: ${error.message}`, "WARN");
      }
    }

    // Exit process for PM2
    process.exit(stats.failedBatches > 0 ? 1 : 0);
  });

  // Handle read stream errors
  rl.on("error", (error) => {
    log(`Error reading file: ${error.message}`, "ERROR");
    process.exit(1);
  });
}

// Setup signal handlers for graceful shutdown
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// Handle uncaught exceptions
process.on("uncaughtException", (error) => {
  log(`Uncaught exception: ${error.message}`, "ERROR");
  log(error.stack, "ERROR");
  process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
  log(`Unhandled rejection at: ${promise}, reason: ${reason}`, "ERROR");
  process.exit(1);
});

// Start the migration
runMigration().catch((error) => {
  log(`Migration failed: ${error.message}`, "ERROR");
  process.exit(1);
});
