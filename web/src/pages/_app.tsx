import type { AppProps } from "next/app";
import { useRouter } from "next/router";
import { useEffect } from "react";

import "@nypl/design-system-react-components/dist/styles.css";
import "@nypl/web-reader/dist/index.css";
import Head from "next/head";
import Script from "next/script";
import { ParsedUrlQuery } from "querystring";
import "react-pdf/dist/esm/Page/AnnotationLayer.css";
import "react-pdf/dist/esm/Page/TextLayer.css";
import appConfig from "~/config/appConfig";
import "~/styles/main.scss";
import ErrorBoundary from "../components/ErrorBoundary";
import { documentTitles } from "../constants/labels";
import { FeatureFlagProvider } from "../context/FeatureFlagContext";
import { FeedbackProvider } from "../context/FeedbackContext";
import { ResearchAssistantProvider } from "../context/ResearchAssistantContext";
import NewRelicSnippet from "../lib/newrelic/NewRelic";

if (process.env.APP_ENV === "testing") {
  const { initMocks } = await import("mocks");
  await initMocks();
}

/**
 * Determines if we are running on server or in the client.
 * @return {boolean} true if running on server
 */
function isServerRendered(): boolean {
  return typeof window === "undefined";
}

/**
 * Sets page title and sends analytics data
 * @param query the router query
 * @returns the title of the page (as shown in browser tab)
 */
const setTitle = (query: ParsedUrlQuery) => {
  if (query.workId) {
    return documentTitles.workItem;
  } else if (query.editionId) {
    return documentTitles.editionItem;
  } else if (query.query) {
    return documentTitles.search;
  } else if (query.linkId) {
    return documentTitles.readItem;
  } else if (query.collectionId) {
    return documentTitles.collection;
  } else {
    return documentTitles.home;
  }
};

const MyApp = ({ Component, pageProps }: AppProps) => {
  const router = useRouter();

  useEffect(() => {
    if (!isServerRendered()) {
      if (!router.query.linkId) {
        document.getElementById("nypl-header").style.display = "block";
        document.getElementById("nypl-footer").style.display = "block";
      }
    }
  });

  return (
    <>
      {/* OptinMonster */}
      <Script
        id="optinmonster"
        dangerouslySetInnerHTML={{
          __html: `(function(d,u,ac){var s=d.createElement('script');s.type='text/javascript';s.src='https://a.omappapi.com/app/js/api.min.js';s.async=true;s.dataset.user=u;s.dataset.account=ac;d.getElementsByTagName('head')[0].appendChild(s);})(document,12468,1044);`,
        }}
      ></Script>
      {/* /OptinMonster */}
      <Head>
        <title>{setTitle(router.query)}</title>

        <link rel="icon" href={appConfig.favIconPath} />
      </Head>
      <FeedbackProvider>
        <ResearchAssistantProvider>
          <ErrorBoundary>
            <NewRelicSnippet />
            <FeatureFlagProvider>
              <Component {...pageProps} />
            </FeatureFlagProvider>
          </ErrorBoundary>
        </ResearchAssistantProvider>
      </FeedbackProvider>
    </>
  );
};

export default MyApp;
