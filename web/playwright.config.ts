import { devices, PlaywrightTestConfig } from "@playwright/test";
import dotenv from "dotenv";
import path from "path";

// Read from ".env" file.
dotenv.config({ path: path.resolve(__dirname, ".env") });

const config: PlaywrightTestConfig = {
  testDir: "playwright/",
  expect: {
    // Maximum time expect() should wait for the condition to be met.
    timeout: 40 * 1000,
  },

  timeout: 40 * 1000,

  // Run all tests in parallel.
  fullyParallel: true,

  // Fail the build on CI if you accidentally left test.only in the source code.
  forbidOnly: !!process.env.CI,

  // Retry on CI only.
  retries: process.env.CI ? 2 : 0,

  // Reporter to use
  reporter: "line",

  use: {
    headless: true,
    // Base URL to use in actions like `await page.goto('/')`.
    baseURL: process.env.BASE_URL || 'https://digital-research-books-beta.nypl.org/',

    /* When running tests locally, record a trace for each test, but remove it from successful runs.
     * On CI, turn this feature off. See https://playwright.dev/docs/trace-viewer */
    trace: process.env.CI ? "off" : "retain-on-failure",
  },

  // Configure projects for major browsers.
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
      },
    },
  ],
};

export default config;
