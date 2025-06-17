// ecosystem.config.js
module.exports = {
  apps: [
    {
      name: "posthog-migration",
      script: "./index.js",
      instances: 1,
      autorestart: false, // Don't restart after completion
      watch: false,
      max_memory_restart: "4G",
      env: {
        NODE_ENV: "production",
        // Other env vars will be loaded from .env file
      },
      merge_logs: true,
      // Auto-delete app from PM2 after it stops (optional)
      kill_timeout: 30000,
      // Ensure PM2 doesn't restart the app
      min_uptime: "10s",
      max_restarts: 0,
    },
  ],
};
