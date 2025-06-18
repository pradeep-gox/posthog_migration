const fs = require("fs");
const readline = require("readline");
const { PostHog } = require("posthog-node");

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
const CHARGEBEE_EVENTS_FILE =
  process.env.CHARGEBEE_EVENTS_FILE || "chargebee_events.jsonl";

const run = async () => {
  const client = new PostHog(POSTHOG_KEY, { host: POSTHOG_HOST });

  const readStream = fs.createReadStream(CHARGEBEE_EVENTS_FILE, {
    encoding: "utf8",
  });
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });

  let totalEvents = 0;

  for await (const line of rl) {
    const event = JSON.parse(line);
    console.log("event", event);
    client.capture(event);
    totalEvents++;
    if (totalEvents % 25 === 0) {
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  await client.shutdown();

  console.log(`Total events: ${totalEvents}`);
};

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
