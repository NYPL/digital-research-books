import { Box, Flex } from "@nypl/design-system-react-components";
import React, { useEffect, useMemo, useRef } from "react";
import {
  HEADER_HEIGHT,
  MARGIN_BLEED,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { trackEvent } from "~/src/lib/gtag/Analytics";
import { isCatalogResults } from "~/src/util/ResearchAssistantUtils";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import EmptySearchPrompt from "../EmptySearchPrompt/EmptySearchPrompt";
import CatalogResults from "./CatalogResults/CatalogResults";
import CatalogResultsSkeleton from "./CatalogResults/CatalogResultsSkeleton";
import ResearchAssistantPanel from "./ResearchAssistantPanel";
import ResultsBanner from "./ResultsBanner";

const ResearchAssistant: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    historyStack,
    goToPreviousState,
    showChat,
    isLoading,
  } = useResearchAssistant();

  const isLandingPageQuery = useRef(false);

  useEffect(() => {
    if (!messages || messages.length === 0) {
      const initialMessage = sessionStorage.getItem(
        "researchAssistantInitialMessage"
      );
      if (initialMessage) {
        isLandingPageQuery.current = true;
        sendMessage(initialMessage);
        sessionStorage.removeItem("researchAssistantInitialMessage");
      }
    }
  }, [messages, sendMessage]);

  const gridTemplateColumns = showChat
    ? "1fr 640px 640px 1fr"
    : "1fr 1152px 128px 1fr";

  const latestResults = useMemo(() => {
    if (!results) return null;

    const exactMatch = results[messages.length];
    if (exactMatch) return exactMatch;

    const sortedResults = Object.entries(results)
      .filter(([, value]) => value && Object.keys(value).length > 0)
      .sort(([a], [b]) => Number(b) - Number(a));

    if (sortedResults.length > 0) {
      return sortedResults[0][1];
    }

    return null;
  }, [messages.length, results]);

  useEffect(() => {
    if (isLandingPageQuery.current && !isLoading && latestResults) {
      let resultsCount = 0;
      if (isCatalogResults(latestResults)) {
        resultsCount = latestResults.paging?.totalRecords || 0;
      }
      trackEvent({
        event: "view_query_results",
        query_type: "landing_page",
        results_count: resultsCount,
      });
      isLandingPageQuery.current = false;
    }
  }, [isLoading, latestResults]);

  return (
    <>
      <Box
        display="grid"
        gridTemplateColumns={gridTemplateColumns}
        width="100%"
        id="mainContent"
        role="main"
      >
        <Flex
          gridColumn="1 / span 2"
          flexDirection="column"
          minWidth="0"
          justifyContent="flex-end"
          alignItems="flex-end"
          bgColor="ui.bg.default"
        >
          <Flex
            width={showChat ? "640px" : "1152px"}
            flexDirection="column"
            height="100%"
            justifyContent="flex-end"
            alignItems="flex-end"
          >
            <Flex flexDirection="column" flex="1" width="100%">
              {latestResults && historyStack.length > 1 && (
                <Box
                  padding="s"
                  borderBottom="1px solid"
                  borderColor="ui.border.default"
                  height={HEADER_HEIGHT}
                  marginLeft={MARGIN_BLEED}
                  paddingLeft={PADDING_COUNTER}
                >
                  <BackToResultsButton
                    handleBackToResults={() => goToPreviousState()}
                  />
                </Box>
              )}
              {!latestResults && !isLoading && (
                <Box
                  padding="s"
                  borderBottom="1px solid"
                  borderColor="ui.border.default"
                  height={HEADER_HEIGHT}
                  marginLeft={MARGIN_BLEED}
                  paddingLeft={PADDING_COUNTER}
                />
              )}
              <Box paddingLeft="s" paddingRight="l" paddingBottom="l" flex="1">
                {isLoading ? (
                  <CatalogResultsSkeleton />
                ) : latestResults && Object.keys(latestResults).length > 0 ? (
                  <>
                    {isCatalogResults(latestResults) && (
                      <CatalogResults results={latestResults} />
                    )}
                  </>
                ) : (
                  <Box width="100%" marginTop="s">
                    <ResultsBanner />
                    <EmptySearchPrompt
                      message={
                        messages.length > 1
                          ? "No results found. Try a different topic."
                          : undefined
                      }
                    />
                  </Box>
                )}
              </Box>
            </Flex>
          </Flex>
        </Flex>
        <Flex
          gridColumn="3 / span 2"
          flexDirection="column"
          bgColor="section.research.primary"
          maxHeight="100vh"
          position="sticky"
          top="0"
          zIndex="1000"
          minWidth="0"
          justifyContent="flex-start"
          alignItems="flex-start"
        >
          <Flex
            width="640px"
            flexDirection="column"
            height="100%"
            justifyContent="flex-start"
            alignItems="flex-start"
          >
            <ResearchAssistantPanel />
          </Flex>
        </Flex>
      </Box>
    </>
  );
};

export default ResearchAssistant;
