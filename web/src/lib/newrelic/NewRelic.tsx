import React from "react";
import Script from "next/script";

type NRConfig = {
  agentID: string;
  applicationID: string;
  accountID?: string;
  trustKey?: string;
  licenseKey?: string;
};

type NRInfo = {
  applicationID: string;
  beacon?: string;
  errorBeacon?: string;
  licenseKey?: string;
  sa?: number;
};

// Default QA configs
const defaultConfig: NRConfig = {
  accountID: "121334",
  trustKey: "121334",
  agentID: "1588858125",
  licenseKey: "NRBR-75b5fdeaf978a4a39e8",
  applicationID: "1447020806",
};
const defaultInfo: NRInfo = {
  beacon: "gov-bam.nr-data.net",
  errorBeacon: "gov-bam.nr-data.net",
  licenseKey: "NRBR-75b5fdeaf978a4a39e8",
  applicationID: "1447020806",
  sa: 1,
};
const devConfig: NRConfig = {
  agentID: "1588857514",
  applicationID: "1443695682",
};
const devInfo: NRInfo = {
  applicationID: "1443695682",
};
const qaConfig: NRConfig = {
  agentID: "1588858125",
  applicationID: "1447020806",
};
const qaInfo: NRInfo = {
  applicationID: "1447020806",
};

const prodConfig: NRConfig = {
  agentID: "1588862533",
  applicationID: "1473036261",
};
const prodInfo: NRInfo = {
  applicationID: "1473036261",
};

function setup(config: NRConfig, info: NRInfo) {
  if (typeof window.NREUM !== "undefined") {
    window.NREUM.loader_config = { ...defaultConfig, ...config };
    window.NREUM.info = { ...defaultInfo, ...info };
  }
}

export function log(error: Error, errorInfo: string) {
  if (typeof window !== "undefined") {
    window.newrelic.noticeError(error, { errorInfo: errorInfo });
  } else {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const newrelic = require("newrelic");
    newrelic.noticeError(error, { errorInfo: errorInfo });
  }
}

// Setup the Newrelic browser agent config for different deploy environment,
function NewRelicBrowserSetup(environment) {
  console.info("Setting up NR - ", environment);
  if (environment === "development") {
    setup(devConfig, devInfo);
  } else if (environment === "qa") {
    setup(qaConfig, qaInfo);
  } else if (environment === "production") {
    setup(prodConfig, prodInfo);
  }
}

const IS_SERVER = typeof window === "undefined";
// This code only embeds the new relic library to the browser, to enable the monitoring, invoke the NewRelicBrowserSetup() function to start.
export const NewRelicSnippet = () => {
  const appEnv = process.env.APP_ENV;
  if (appEnv === "testing" || IS_SERVER) return null;

  if (!process.env.NEW_RELIC_LICENSE_KEY) {
    console.warn("Missing New Relic License key ", appEnv);
    return null;
  }

  return (
    <Script
      type="text/javascript"
      src="/newrelic-browser.js"
      onLoad={() => {
        NewRelicBrowserSetup(appEnv);
      }}
    />
  );
};

export default NewRelicSnippet;
