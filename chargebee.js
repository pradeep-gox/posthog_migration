const fs = require("fs");
const path = require("path");
const axios = require("axios");
const qs = require("qs");

// Load environment variables from .env file if it exists
try {
  require("dotenv").config();
} catch (error) {
  // dotenv not installed or .env file doesn't exist - that's fine
}

// Configuration
const CONFIG = {
  CHARGEBEE_HOST: process.env.CHARGEBEE_HOST,
  CHARGEBEE_KEY: process.env.CHARGEBEE_KEY,
  CHARGEBEE_EVENTS_FILE:
    process.env.CHARGEBEE_EVENTS_FILE || "chargebee_events.jsonl",
  PROGRESS_FILE:
    process.env.CHARGEBEE_PROGRESS_FILE || "chargebee_progress.json",
  TIMESTAMP_BOUND_END: parseInt(process.env.TIMESTAMP_BOUND_END) || 1739791186,
  MAX_RETRIES: parseInt(process.env.MAX_RETRIES) || 3,
  INITIAL_RETRY_DELAY: parseInt(process.env.INITIAL_RETRY_DELAY) || 5000,
  REQUEST_TIMEOUT: parseInt(process.env.REQUEST_TIMEOUT) || 30000,
  RATE_LIMIT_DELAY: parseInt(process.env.RATE_LIMIT_DELAY) || 5000,
  BATCH_SIZE: parseInt(process.env.BATCH_SIZE) || 100,
  MAX_EVENTS_LIMIT: parseInt(process.env.MAX_EVENTS_LIMIT) || null,
};

// Validate required environment variables
function validateConfig() {
  const required = ["CHARGEBEE_HOST", "CHARGEBEE_KEY"];
  const missing = required.filter((key) => !CONFIG[key]);

  if (missing.length > 0) {
    throw new Error(
      `Missing required environment variables: ${missing.join(", ")}`
    );
  }

  if (!CONFIG.CHARGEBEE_HOST.startsWith("http")) {
    throw new Error(
      "CHARGEBEE_HOST must be a valid URL starting with http/https"
    );
  }

  log(`Configuration validated successfully`);
  log(`Events file: ${CONFIG.CHARGEBEE_EVENTS_FILE}`);
  log(`Progress file: ${CONFIG.PROGRESS_FILE}`);
  log(
    `Timestamp bound: ${CONFIG.TIMESTAMP_BOUND_END} (${new Date(
      CONFIG.TIMESTAMP_BOUND_END * 1000
    ).toISOString()})`
  );
}

const plans = [
  ["gs", "pro-yearly-120", 2, "Pro"],
  ["gs", "pro-monthly-24", 2, "Pro"],
  ["gs", "free-trial-14", 1, "Free Trial"],
  ["gs", "basic", 3, "Basic"],
  ["gs", "pro-monthly-9", 2, "Pro"],
  ["gs", "pro-lifetime-159", 2, "Pro"],
  ["gs", "pro-monthly-30", 2, "Pro"],
  ["gs", "startup-monhtly-60", 6, "Startup"],
  ["gs", "solo-monthly-30", 5, "Solo"],
  ["gs", "pro-monthly-99", 2, "Pro"],
  ["gs", "pro-yearly-49", 2, "Pro"],
  ["gs", "pro-monthly-62", 2, "Pro"],
  ["gs", "basic-yearly-19", 3, "Basic"],
  ["gs", "basic-monthly-24", 3, "Basic"],
  ["gs", "lite-yearly-5", 7, "Lite"],
  ["gs", "lite-monthly-7", 7, "Lite"],
  ["gs", "free-limited", 4, "Free Limited"],
  ["gs", "pro-monthly-62-inr", 2, "Pro"],
  ["gs", "basic-monthly-24-inr", 3, "Basic"],
  ["gs", "pro-yearly-49-inr", 2, "Pro"],
  ["gs", "basic-annual-inr-19", 3, "Basic"],
  ["gs", "lite-Monthly-inr-7", 7, "Lite"],
  ["gs", "lite-yearly-inr-5", 7, "Lite"],
  ["gs", "free-limited-2", 4, "Free Limited"],
  ["gs", "free-limited-inr", 4, "Free Limited"],
  ["gs", "enterprise-yearly-249-inr", 8, "Enterprise"],
  ["gs", "enterprise-yearly-249", 8, "Enterprise"],
  ["gs", "enterprise-monthly-332-inr", 8, "Enterprise"],
  ["gs", "enterprise-monthly-332", 8, "Enterprise"],
  ["gs", "lite-v202303-USD-Monthly", 9, "Lite"],
  ["gs", "lite-v202303-USD-Yearly", 9, "Lite"],
  ["gs", "lite-v202303-INR-Monthly", 9, "Lite"],
  ["gs", "lite-v202303-INR-Yearly", 9, "Lite"],
  ["gs", "basic-v202303-USD-Monthly", 10, "Basic"],
  ["gs", "basic-v202303-USD-Yearly", 10, "Basic"],
  ["gs", "basic-v202303-INR-Monthly", 10, "Basic"],
  ["gs", "basic-v202303-INR-Yearly", 10, "Basic"],
  ["gs", "pro-v202303-USD-Monthly", 11, "Pro"],
  ["gs", "pro-v202303-USD-Yearly", 11, "Pro"],
  ["gs", "pro-v202303-INR-Monthly", 11, "Pro"],
  ["gs", "pro-v202303-INR-Yearly", 11, "Pro"],
  ["gs", "business-v202303-USD-Monthly", 12, "Business"],
  ["gs", "business-v202303-USD-Yearly", 12, "Business"],
  ["gs", "business-v202303-INR-Monthly", 12, "Business"],
  ["gs", "business-v202303-INR-Yearly", 12, "Business"],
  ["gs", "lite-v202401-USD-Monthly", 13, "Lite"],
  ["gs", "lite-v202401-USD-Yearly", 13, "Lite"],
  ["gs", "lite-v202401-INR-Monthly", 13, "Lite"],
  ["gs", "lite-v202401-INR-Yearly", 13, "Lite"],
  ["gs", "gs-lite-v202411-USD-Monthly", 14, "Lite"],
  ["gs", "gs-lite-v202411-USD-Yearly", 14, "Lite"],
  ["gs", "gs-lite-v202411-INR-Monthly", 14, "Lite"],
  ["gs", "gs-lite-v202411-INR-Yearly", 14, "Lite"],
  ["gs", "gs-basic-v202411-USD-Monthly", 15, "Basic"],
  ["gs", "gs-basic-v202411-USD-Yearly", 15, "Basic"],
  ["gs", "gs-basic-v202411-INR-Monthly", 15, "Basic"],
  ["gs", "gs-basic-v202411-INR-Yearly", 15, "Basic"],
  ["gs", "gs-pro-v202411-USD-Monthly", 16, "Pro"],
  ["gs", "gs-pro-v202411-USD-Yearly", 16, "Pro"],
  ["gs", "gs-pro-v202411-INR-Monthly", 16, "Pro"],
  ["gs", "gs-pro-v202411-INR-Yearly", 16, "Pro"],
  ["gs", "gs-business-v202411-USD-Monthly", 17, "Business"],
  ["gs", "gs-business-v202411-USD-Yearly", 17, "Business"],
  ["gs", "gs-business-v202411-INR-Monthly", 17, "Business"],
  ["gs", "gs-business-v202411-INR-Yearly", 17, "Business"],
  ["gs", "gs-pro-v202411-BF-USD-Monthly", 18, "Pro"],
  ["gs", "gs-pro-v202411-BF-USD-Yearly", 18, "Pro"],
  ["gs", "gs-pro-v202411-BF-INR-Monthly", 18, "Pro"],
  ["gs", "gs-pro-v202411-BF-INR-Yearly", 18, "Pro"],
  ["gs", "gs-business-v202411-BF-USD-Monthly", 19, "Business"],
  ["gs", "gs-business-v202411-BF-USD-Yearly", 19, "Business"],
  ["gs", "gs-business-v202411-BF-INR-Monthly", 19, "Business"],
  ["gs", "gs-business-v202411-BF-INR-Yearly", 19, "Business"],
  ["gds", "pro-yearly-49", 2, "Pro"],
  ["gds", "pro-monthly-62", 2, "Pro"],
  ["gds", "free-trial-14", 1, "Free Trial"],
  ["gds", "basic-yearly-19", 3, "Basic"],
  ["gds", "pro-monthly-9", 2, "Pro"],
  ["gds", "pro-lifetime-159", 2, "Pro"],
  ["gds", "pro-monthly-30", 2, "Pro"],
  ["gds", "startup-monthly-60", 6, "Startup"],
  ["gds", "solo-monthly-30", 5, "Solo"],
  ["gds", "pro-monthly-99", 2, "Pro"],
  ["gds", "basic-monthly-24", 3, "Basic"],
  ["gds", "lite-monthly-7", 7, "Lite"],
  ["gds", "lite-yearly-5", 7, "Lite"],
  ["gds", "free-limited", 4, "Free Limited"],
  ["gds", "gds-pro-yearly", 2, "Pro"],
  ["gds", "gds-pro-monthly", 2, "Pro"],
  ["gds", "gds-basic-yearly", 3, "Basic"],
  ["gds", "gds-basic-monthly", 3, "Basic"],
  ["gds", "gds-lite-yearly", 7, "Lite"],
  ["gds", "gds-lite-monthly", 7, "Lite"],
  ["gds", "gds-free-limited", 4, "Free Limited"],
  ["gds", "gds-lite-yearly-inr", 7, "Lite"],
  ["gds", "gds-lite-monthly-inr", 7, "Lite"],
  ["gds", "gds-pro-yearly-inr", 2, "Pro"],
  ["gds", "gds-pro-monthly-inr", 2, "Pro"],
  ["gds", "gds-basic-yearly-inr", 3, "Basic"],
  ["gds", "gds-basic-monthly-inr", 3, "Basic"],
  ["gds", "gds-lite-v202303-USD-Monthly", 9, "Lite"],
  ["gds", "gds-lite-v202303-USD-Yearly", 9, "Lite"],
  ["gds", "gds-lite-v202303-INR-Monthly", 9, "Lite"],
  ["gds", "gds-lite-v202303-INR-Yearly", 9, "Lite"],
  ["gds", "gds-basic-v202303-USD-Monthly", 10, "Basic"],
  ["gds", "gds-basic-v202303-USD-Yearly", 10, "Basic"],
  ["gds", "gds-basic-v202303-INR-Monthly", 10, "Basic"],
  ["gds", "gds-basic-v202303-INR-Yearly", 10, "Basic"],
  ["gds", "gds-pro-v202303-USD-Monthly", 11, "Pro"],
  ["gds", "gds-pro-v202303-USD-Yearly", 11, "Pro"],
  ["gds", "gds-pro-v202303-INR-Monthly", 11, "Pro"],
  ["gds", "gds-pro-v202303-INR-Yearly", 11, "Pro"],
  ["gds", "gds-business-v202303-USD-Monthly", 12, "Business"],
  ["gds", "gds-business-v202303-USD-Yearly", 12, "Business"],
  ["gds", "gds-business-v202303-INR-Monthly", 12, "Business"],
  ["gds", "gds-business-v202303-INR-Yearly", 12, "Business"],
  ["gds", "gds-free-limited-inr", 4, "Free Limited"],
  ["gds", "gds-lite-v202401-USD-Monthly", 13, "Lite"],
  ["gds", "gds-lite-v202401-USD-Yearly", 13, "Lite"],
  ["gds", "gds-lite-v202401-INR-Monthly", 13, "Lite"],
  ["gds", "gds-lite-v202401-INR-Yearly", 13, "Lite"],
  ["gds", "gds-lite-v202411-USD-Monthly", 14, "Lite"],
  ["gds", "gds-lite-v202411-USD-Yearly", 14, "Lite"],
  ["gds", "gds-lite-v202411-INR-Monthly", 14, "Lite"],
  ["gds", "gds-lite-v202411-INR-Yearly", 14, "Lite"],
  ["gds", "gds-basic-v202411-USD-Monthly", 15, "Basic"],
  ["gds", "gds-basic-v202411-USD-Yearly", 15, "Basic"],
  ["gds", "gds-basic-v202411-INR-Monthly", 15, "Basic"],
  ["gds", "gds-basic-v202411-INR-Yearly", 15, "Basic"],
  ["gds", "gds-pro-v202411-USD-Monthly", 16, "Pro"],
  ["gds", "gds-pro-v202411-USD-Yearly", 16, "Pro"],
  ["gds", "gds-pro-v202411-INR-Monthly", 16, "Pro"],
  ["gds", "gds-pro-v202411-INR-Yearly", 16, "Pro"],
  ["gds", "gds-business-v202411-USD-Monthly", 17, "Business"],
  ["gds", "gds-business-v202411-USD-Yearly", 17, "Business"],
  ["gds", "gds-business-v202411-INR-Monthly", 17, "Business"],
  ["gds", "gds-business-v202411-INR-Yearly", 17, "Business"],
  ["gds", "gds-pro-v202411-BF-USD-Monthly", 18, "Pro"],
  ["gds", "gds-pro-v202411-BF-USD-Yearly", 18, "Pro"],
  ["gds", "gds-pro-v202411-BF-INR-Monthly", 18, "Pro"],
  ["gds", "gds-pro-v202411-BF-INR-Yearly", 18, "Pro"],
  ["gds", "gds-business-v202411-BF-USD-Monthly", 19, "Business"],
  ["gds", "gds-business-v202411-BF-USD-Yearly", 19, "Business"],
  ["gds", "gds-business-v202411-BF-INR-Monthly", 19, "Business"],
  ["gds", "gds-business-v202411-BF-INR-Yearly", 19, "Business"],
];

// Progress tracking
class ProgressTracker {
  constructor(filePath) {
    this.filePath = filePath;
    this.data = this.load();
  }

  load() {
    try {
      if (fs.existsSync(this.filePath)) {
        const content = fs.readFileSync(this.filePath, "utf8");
        return JSON.parse(content);
      }
    } catch (error) {
      log(`Warning: Could not load progress file: ${error.message}`, "WARN");
    }

    return {
      lastOffset: null,
      totalEventsProcessed: 0,
      startTime: new Date().toISOString(),
      lastUpdateTime: new Date().toISOString(),
      errors: [],
      completed: false,
    };
  }

  save() {
    try {
      this.data.lastUpdateTime = new Date().toISOString();
      fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
    } catch (error) {
      log(`Error saving progress: ${error.message}`, "ERROR");
    }
  }

  update(offset, eventsCount) {
    this.data.lastOffset = offset;
    this.data.totalEventsProcessed += eventsCount;
    this.save();
  }

  addError(error) {
    this.data.errors.push({
      timestamp: new Date().toISOString(),
      error: error.toString(),
    });
    this.save();
  }

  markCompleted() {
    this.data.completed = true;
    this.data.completedTime = new Date().toISOString();
    this.save();
  }

  getStatus() {
    return {
      totalEvents: this.data.totalEventsProcessed,
      lastOffset: this.data.lastOffset,
      startTime: this.data.startTime,
      isCompleted: this.data.completed,
      errorCount: this.data.errors.length,
    };
  }
}

// Helper function to log with timestamp
function log(message, level = "INFO") {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] ${message}`);
}

// Helper function to sleep with exponential backoff
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const exponentialBackoff = (attempt) => {
  return (
    CONFIG.INITIAL_RETRY_DELAY * Math.pow(2, attempt) + Math.random() * 1000
  );
};

// Enhanced API request with retry logic
async function makeRetryableRequest(requestFn, context = "") {
  let lastError;

  for (let attempt = 0; attempt <= CONFIG.MAX_RETRIES; attempt++) {
    try {
      if (attempt > 0) {
        const delay = exponentialBackoff(attempt - 1);
        log(
          `Retrying ${context} (attempt ${attempt}/${
            CONFIG.MAX_RETRIES
          }) after ${Math.round(delay)}ms`,
          "WARN"
        );
        await sleep(delay);
      }

      return await requestFn();
    } catch (error) {
      lastError = error;

      // Check if error is retryable
      const isRetryable =
        error.code === "ECONNRESET" ||
        error.code === "ETIMEDOUT" ||
        error.code === "ENOTFOUND" ||
        (error.response &&
          [429, 500, 502, 503, 504].includes(error.response.status));

      if (!isRetryable || attempt === CONFIG.MAX_RETRIES) {
        log(
          `Non-retryable error or max retries reached for ${context}: ${error.message}`,
          "ERROR"
        );
        throw error;
      }

      log(`Retryable error for ${context}: ${error.message}`, "WARN");
    }
  }

  throw lastError;
}

const processChargebeeEventResponse = (data) => {
  const eventsList = data?.list ?? [];
  const nextOffset = data?.next_offset ?? null;

  const events = eventsList
    .map((e) => {
      const event = e.event;
      return {
        id: event.id,
        event_type: event.event_type,
        occurred_at: event.occurred_at,
        content: event.content,
      };
    })
    .filter((e) => e.occurred_at < CONFIG.TIMESTAMP_BOUND_END);

  return { events, next_offset: nextOffset };
};

const getChargebeeEvents = async (offset = null) => {
  const requestFn = async () => {
    let url = `${CONFIG.CHARGEBEE_HOST}/api/v2/events`;
    const params = {
      limit: CONFIG.BATCH_SIZE,
      "event_type[in]": [
        "subscription_created",
        "subscription_changed",
        "subscription_activated",
        "subscription_reactivated",
        "subscription_cancelled",
        "subscription_started",
        "subscription_renewed",
        "payment_succeeded",
      ],
    };

    if (offset) params["offset"] = offset;

    const strParams = qs.stringify(params);
    const response = await axios.get(`${url}?${strParams}`, {
      headers: {
        Authorization: `Basic ${CONFIG.CHARGEBEE_KEY}`,
        "Content-Type": "application/json",
      },
      timeout: CONFIG.REQUEST_TIMEOUT,
    });

    return processChargebeeEventResponse(response.data);
  };

  try {
    return await makeRetryableRequest(
      requestFn,
      `Chargebee API (offset: ${offset})`
    );
  } catch (error) {
    log(
      `Failed to fetch Chargebee events after all retries: ${error.message}`,
      "ERROR"
    );
    return { events: [], next_offset: null };
  }
};

const getOriginBySubscription = (subscriptionData) => {
  if (subscriptionData) {
    const subscription = subscriptionData?.subscription_items?.find(
      (item) => item.item_type == "plan"
    );
    if (subscription?.item_price_id) {
      switch (subscription.item_price_id.split("-")[0]) {
        case "gds":
          return "gds";
        default:
          return "gs";
      }
    }
  }
  return "gs";
};

const getFirstSourceDetails = (firstSource) => {
  const result = {
    tmrCustomerFirstSource: null,
    tmrCustomerFirstMedium: null,
    tmrCustomerFirstCampaign: null,
    tmrCustomerFirstLP: null,
  };
  if (!firstSource) return result;
  const firstSourceArray = firstSource.split("|");
  //? www.google.com|none|none|https://twominutereports.com/blog/dashthis-alternatives
  if (firstSourceArray.length === 4) {
    result.tmrCustomerFirstSource = firstSourceArray[0];
    result.tmrCustomerFirstMedium = firstSourceArray[1];
    result.tmrCustomerFirstCampaign = firstSourceArray[2];
    result.tmrCustomerFirstLP = firstSourceArray[3];
  }
  return result;
};

const getSignupSourceDetails = (signupSource) => {
  const result = {
    tmrCustomerSignupSource: null,
    tmrCustomerSignupMedium: null,
    tmrCustomerSignupCampaign: null,
    tmrCustomerSignupLP: null,
  };
  if (!signupSource) return result;
  const signupSourceArray = signupSource.split("|");
  //? Eg: www.google.com|none|none|https://twominutereports.com/
  if (signupSourceArray.length === 4) {
    result.tmrCustomerSignupSource = signupSourceArray[0];
    result.tmrCustomerSignupMedium = signupSourceArray[1];
    result.tmrCustomerSignupCampaign = signupSourceArray[2];
    result.tmrCustomerSignupLP = signupSourceArray[3];
  }
  return result;
};

const getProcessedEvent = (event) => {
  const { event_type, occurred_at, content } = event;

  const eventName = `tmr_${
    event_type === "payment_succeeded"
      ? "subscription_payment_succeeded"
      : event_type
  }`;
  const timestamp =
    new Date(occurred_at * 1000).toISOString().split(".")[0] + "Z";

  const customerEmailId = content?.customer?.email?.toLowerCase();
  const origin = getOriginBySubscription(content?.subscription);
  const signedUpFrom = content?.customer?.cf_signedup_from;

  const firstSourceDetails = getFirstSourceDetails(
    content?.customer?.cf_first_source_v2 ?? content?.customer?.cf_first_source
  );
  const signupSourceDetails = getSignupSourceDetails(
    content?.customer?.cf_signup_source_v2 ??
      content?.customer?.cf_signup_source
  );

  const subscriptionCreatedAt = content?.subscription?.created_at
    ? new Date(content?.subscription?.created_at * 1000)
        .toISOString()
        .split(".")[0] + "Z"
    : null;
  const subscriptionUpdatedAt = content?.subscription?.updated_at
    ? new Date(content?.subscription?.updated_at * 1000)
        .toISOString()
        .split(".")[0] + "Z"
    : null;
  const subscriptionStartedAt = content?.subscription?.started_at
    ? new Date(content?.subscription?.started_at * 1000)
        .toISOString()
        .split(".")[0] + "Z"
    : null;
  const subscriptionCancelledAt = content?.subscription?.cancelled_at
    ? new Date(content?.subscription?.cancelled_at * 1000)
        .toISOString()
        .split(".")[0] + "Z"
    : null;

  const subscriptionItems = content?.subscription?.subscription_items || [];

  const subscriptionPlanItem = subscriptionItems.find(
    (item) => item.item_type === "plan"
  );
  const subscriptionPlanId = subscriptionPlanItem?.item_price_id;
  const subscriptionPlanAmount = subscriptionPlanItem?.amount ?? 0;

  const subscriptionUserAddonItem = subscriptionItems.find(
    (item) => item.item_type === "addon" && item.item_price_id.includes("user")
  );

  const subscriptionUserAddonQuantity =
    subscriptionUserAddonItem?.quantity ?? 0;
  const subscriptionUserAddonAmount = subscriptionUserAddonItem?.amount ?? 0;
  const subscriptionUserAddonUnitPrice =
    subscriptionUserAddonItem?.unit_price ?? 0;

  const subscriptionAccountAddonItem = subscriptionItems.find(
    (item) =>
      item.item_type === "addon" &&
      (item.item_price_id.includes("datasource") ||
        item.item_price_id.includes("connection"))
  );

  const subscriptionAccountAddonQuantity =
    subscriptionAccountAddonItem?.quantity ?? 0;
  const subscriptionAccountAddonAmount =
    subscriptionAccountAddonItem?.amount ?? 0;
  const subscriptionAccountAddonUnitPrice =
    subscriptionAccountAddonItem?.unit_price ?? 0;

  const subscriptionTotalAmount =
    subscriptionPlanAmount +
    subscriptionUserAddonAmount +
    subscriptionAccountAddonAmount;

  const tmrPlan = plans.find(
    (p) => p[0] === origin && p[1] === subscriptionPlanId
  );
  const tmrPlanId = tmrPlan?.[2] ?? null;
  const tmrPlanName = tmrPlan?.[3] ?? null;

  const eventProperties = {
    distinct_id: customerEmailId,
    tmrOrigin: origin,
    tmrCustomerEmailId: customerEmailId,
    tmrCustomerId: content?.subscription?.customer_id ?? null,
    tmrCustomerSignedUpFrom: signedUpFrom ?? null,
    tmrSignedUpFrom: signedUpFrom ?? null,
    tmrPlanId: tmrPlanId,
    tmrSubscriptionVendorName: "chargebee",
    tmrSubscriptionId: content?.subscription?.id ?? null,
    tmrSubscriptionTotalAmount: subscriptionTotalAmount,
    tmrSubscriptionPlanName: tmrPlanName,
    tmrSubscriptionPlanId: subscriptionPlanId ?? null,
    tmrSubscriptionPlanAmount: subscriptionPlanAmount,
    tmrSubscriptionStatus: content?.subscription?.status ?? null,
    tmrSubscriptionBillingPeriod: content?.subscription?.billing_period ?? null,
    tmrSubscriptionBillingPeriodUnit:
      content?.subscription?.billing_period_unit,
    tmrSubscriptionCreatedAt: subscriptionCreatedAt,
    tmrSubscriptionUpdatedAt: subscriptionUpdatedAt,
    tmrSubscriptionStartedAt: subscriptionStartedAt,
    tmrSubscriptionCancelledAt: subscriptionCancelledAt,
    tmrSubscriptionCancelReason: content?.subscription?.cancel_reason ?? null,
    tmrSubscriptionCurrencyCode: content?.subscription?.currency_code ?? null,
    tmrSubscriptionUserAddonQuantity: subscriptionUserAddonQuantity,
    tmrSubscriptionUserAddonAmount: subscriptionUserAddonAmount,
    tmrSubscriptionUserAddonUnitPrice: subscriptionUserAddonUnitPrice,
    tmrSubscriptionAccountAddonQuantity: subscriptionAccountAddonQuantity,
    tmrSubscriptionAccountAddonAmount: subscriptionAccountAddonAmount,
    tmrSubscriptionAccountAddonUnitPrice: subscriptionAccountAddonUnitPrice,
    ...firstSourceDetails,
    ...signupSourceDetails,
  };

  return {
    event: eventName,
    timestamp: timestamp,
    properties: eventProperties,
  };
};

const writeEventsToFile = async (events, isNewFile = false) => {
  try {
    const fileStream = fs.createWriteStream(CONFIG.CHARGEBEE_EVENTS_FILE, {
      flags: isNewFile ? "w" : "a",
    });

    return new Promise((resolve, reject) => {
      fileStream.on("error", reject);
      fileStream.on("finish", resolve);

      for (const event of events) {
        try {
          fileStream.write(JSON.stringify(getProcessedEvent(event)) + "\n");
        } catch (error) {
          log(`Error processing event ${event.id}: ${error.message}`, "WARN");
          continue;
        }
      }

      fileStream.end();
    });
  } catch (error) {
    log(`Error writing events to file: ${error?.message}`, "ERROR");
    throw error;
  }
};

// Signal handling for graceful shutdown
let shouldStop = false;
process.on("SIGINT", () => {
  log("Received SIGINT, finishing current batch and stopping...", "INFO");
  shouldStop = true;
});

process.on("SIGTERM", () => {
  log("Received SIGTERM, finishing current batch and stopping...", "INFO");
  shouldStop = true;
});

const run = async () => {
  try {
    // Validate configuration
    validateConfig();

    // Initialize progress tracker
    const progressTracker = new ProgressTracker(CONFIG.PROGRESS_FILE);
    const status = progressTracker.getStatus();

    // Check if already completed
    if (status.isCompleted) {
      log(
        `Process already completed. Total events: ${status.totalEvents}`,
        "INFO"
      );
      log(
        `To restart, delete the progress file: ${CONFIG.PROGRESS_FILE}`,
        "INFO"
      );
      return;
    }

    // Resume from last position
    let next_offset = status.lastOffset;
    let totalEvents = status.totalEvents;
    const startTime = Date.now();

    log(`Starting extraction process...`, "INFO");
    if (next_offset) {
      log(`Resuming from offset: ${next_offset}`, "INFO");
      log(`Already processed: ${totalEvents} events`, "INFO");
    } else {
      log(`Starting fresh extraction`, "INFO");
      // Clear file only if starting fresh
      fs.writeFileSync(CONFIG.CHARGEBEE_EVENTS_FILE, "");
    }

    let batchCount = 0;
    let consecutiveEmptyBatches = 0;
    const maxConsecutiveEmptyBatches = 10000000;

    do {
      if (shouldStop) {
        log("Stopping due to signal...", "INFO");
        break;
      }

      // Check if we've hit the limit
      if (CONFIG.MAX_EVENTS_LIMIT && totalEvents >= CONFIG.MAX_EVENTS_LIMIT) {
        log(`Reached maximum events limit: ${CONFIG.MAX_EVENTS_LIMIT}`, "INFO");
        break;
      }

      batchCount++;
      log(
        `Processing batch ${batchCount} (offset: ${
          next_offset || "initial"
        })...`,
        "INFO"
      );

      try {
        const result = await getChargebeeEvents(next_offset);
        next_offset = result.next_offset;

        if (result.events.length === 0) {
          consecutiveEmptyBatches++;
          log(
            `Empty batch received (${consecutiveEmptyBatches}/${maxConsecutiveEmptyBatches})`,
            "WARN"
          );

          if (consecutiveEmptyBatches >= maxConsecutiveEmptyBatches) {
            log(
              `Received ${maxConsecutiveEmptyBatches} consecutive empty batches, stopping`,
              "INFO"
            );
            break;
          }
        } else {
          consecutiveEmptyBatches = 0;

          // Write events to file
          await writeEventsToFile(
            result.events,
            totalEvents === 0 && next_offset === null
          );

          // Update progress
          totalEvents += result.events.length;
          progressTracker.update(next_offset, result.events.length);

          // Calculate and log stats
          const elapsedTime = Date.now() - startTime;
          const eventsPerSecond = totalEvents / (elapsedTime / 1000);
          const oldestEvent = result.events.reduce(
            (oldest, event) =>
              event.occurred_at < oldest.occurred_at ? event : oldest,
            result.events[0]
          );
          const newestEvent = result.events.reduce(
            (newest, event) =>
              event.occurred_at > newest.occurred_at ? event : newest,
            result.events[0]
          );

          log(
            `Batch ${batchCount} completed: ${result.events.length} events processed`,
            "INFO"
          );
          log(
            `Total events: ${totalEvents} | Rate: ${eventsPerSecond.toFixed(
              2
            )} events/sec`,
            "INFO"
          );
          log(
            `Event date range: ${
              new Date(oldestEvent.occurred_at * 1000)
                .toISOString()
                .split("T")[0]
            } to ${
              new Date(newestEvent.occurred_at * 1000)
                .toISOString()
                .split("T")[0]
            }`,
            "INFO"
          );

          if (next_offset) {
            log(`Next offset: ${next_offset}`, "DEBUG");
          }
        }

        // Rate limiting
        if (CONFIG.RATE_LIMIT_DELAY > 0) {
          await sleep(CONFIG.RATE_LIMIT_DELAY);
        }
      } catch (error) {
        log(`Error in batch ${batchCount}: ${error.message}`, "ERROR");
        progressTracker.addError(error);

        // For critical errors, stop the process
        if (error.response?.status === 401 || error.response?.status === 403) {
          log("Authentication error, stopping process", "ERROR");
          break;
        }

        // For other errors, continue with backoff
        await sleep(exponentialBackoff(1));
      }
    } while (next_offset && !shouldStop);

    // Mark as completed if we finished normally
    if (!shouldStop && !next_offset) {
      progressTracker.markCompleted();
      log(`Extraction completed successfully!`, "INFO");
    }

    // Final statistics
    const finalStatus = progressTracker.getStatus();
    const totalTime = Date.now() - startTime;

    log(`=== EXTRACTION SUMMARY ===`, "INFO");
    log(`Total events processed: ${finalStatus.totalEvents}`, "INFO");
    log(`Total time: ${(totalTime / 1000 / 60).toFixed(2)} minutes`, "INFO");
    log(
      `Average rate: ${(finalStatus.totalEvents / (totalTime / 1000)).toFixed(
        2
      )} events/sec`,
      "INFO"
    );
    log(`Errors encountered: ${finalStatus.errorCount}`, "INFO");
    log(`Output file: ${CONFIG.CHARGEBEE_EVENTS_FILE}`, "INFO");
    log(`Progress file: ${CONFIG.PROGRESS_FILE}`, "INFO");

    if (finalStatus.isCompleted) {
      log(`Status: COMPLETED`, "INFO");
    } else if (shouldStop) {
      log(`Status: STOPPED BY USER - Can be resumed`, "INFO");
    } else {
      log(`Status: STOPPED DUE TO ERROR - Check logs and resume`, "WARN");
    }
  } catch (error) {
    log(`Fatal error: ${error.message}`, "ERROR");
    console.error(error);
    process.exit(1);
  }
};

// Start the function

run().catch((error) => {
  log(`Chargebee events extraction failed: ${error.message}`, "ERROR");
  process.exit(1);
});
