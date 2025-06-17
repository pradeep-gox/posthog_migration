const fs = require("fs");
const axios = require("axios");
const qs = require("qs");

// Load environment variables from .env file if it exists
try {
  require("dotenv").config();
} catch (error) {
  // dotenv not installed or .env file doesn't exist - that's fine
}

const CHARGEBEE_HOST = process.env.CHARGEBEE_HOST;
const CHARGEBEE_KEY = process.env.CHARGEBEE_KEY;

const CHARGEBEE_EVENTS_FILE = process.env.CHARGEBEE_EVENTS_FILE;
const TIMESTAMP_BOUND_END = 1739791186;

const plans = [
  ["gs", "pro-yearly-120", 2, "Pro"],
  ["gs", "pro-monthly-24", 2, "Pro"],
  ["gs", "free-trial-14", 1, "Free Trial"],
  ["gs", "basic", 3, "Basic"],
  ["gs", "pro-monthly-9", 2, "Pro"],
  ["gs", "pro-lifetime-159", 2, "Pro"],
  ["gs", "pro-monthly-30", 2, "Pro"],
  ["gs", "startup-monhtly-60,6,Startup"],
  ["gs", "solo-monthly-30,5,Solo"],
  ["gs", "pro-monthly-99,2,Pro"],
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

// Helper function to log with timestamp
function log(message, level = "INFO") {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] ${message}`);
}

// Helper function to sleep
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
    .filter((e) => e.occurred_at < TIMESTAMP_BOUND_END);

  return { events, next_offset: nextOffset };
};

const getChargebeeEvents = async (offset = null) => {
  try {
    let url = `${CHARGEBEE_HOST}/api/v2/events`;
    const params = {
      limit: 100,
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
        Authorization: `Basic ${CHARGEBEE_KEY}`,
        "Content-Type": "application/json",
      },
    });
    return processChargebeeEventResponse(response.data);
  } catch (error) {
    log(
      `Error fetching Chargebee events: ${offset} : ${error?.response?.data?.message}`,
      "ERROR"
    );
    return { events: [], next_offset: null };
  }
};

getOriginBySubscription = (subscriptionData) => {
  if (subscriptionData) {
    const subscription = subscriptionData?.subscription_items?.find(
      (item) => item.item_type == "plan"
    );
    switch (subscription.item_price_id.split("-")[0]) {
      case "gds":
        return "gds";
      default:
        return "gs";
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

const writeEventsToFile = async (events) => {
  try {
    const fileStream = fs.createWriteStream(CHARGEBEE_EVENTS_FILE, {
      flags: "a",
    });
    for (const event of events) {
      fileStream.write(JSON.stringify(getProcessedEvent(event)) + "\n");
    }
    fileStream.end();
    log(`Wrote ${events.length} events to file`);
  } catch (error) {
    log(`Error writing events to file: ${error?.message}`, "ERROR");
  }
};

const run = async () => {
  let next_offset = null;
  let totalEvents = 0;

  // clear file before writing
  fs.writeFileSync(CHARGEBEE_EVENTS_FILE, "");

  do {
    const result = await getChargebeeEvents(next_offset);
    next_offset = result.next_offset;

    if (result.events.length > 0) {
      console.log("getChargebeeEvents", result.events.length);
      await writeEventsToFile(result.events);
      totalEvents += result.events.length;
      log(`Processed ${totalEvents} events so far`);
    }

    // Add a small delay to avoid rate limiting
    await sleep(5000);
    if (totalEvents > 10000) {
      break;
    }
  } while (next_offset);

  log(`Completed! Total events processed: ${totalEvents}`);
};

// Start the function
run().catch((error) => {
  log(`Chargebee events write failed: ${error.message}`, "ERROR");
  process.exit(1);
});
