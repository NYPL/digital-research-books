import { Box, Flex } from "@nypl/design-system-react-components";
import React, { useEffect, useMemo } from "react";
import {
  DEFAULT_MOBILE_PANEL_HEIGHT,
  HEADER_HEIGHT,
  MARGIN_BLEED,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { useResizablePanel } from "~/src/hooks/useResizablePanel";
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
    toggleChat,
    isLoading,
  } = useResearchAssistant();
  const {
    mobilePanelHeight,
    handleResizeStart,
    handleResizeKeyDown,
    handleExpandToFull,
    handleDecreaseToMin,
    getMaxPanelHeight,
  } = useResizablePanel({
    showChat,
    onHidePanel: toggleChat,
  });

  useEffect(() => {
    if (!messages || messages.length === 0) {
      const initialMessage = sessionStorage.getItem(
        "researchAssistantInitialMessage"
      );
      if (initialMessage) {
        sendMessage(initialMessage);
        sessionStorage.removeItem("researchAssistantInitialMessage");
      }
    }
  }, [messages, sendMessage]);

  const gridTemplateColumns = showChat
    ? { base: "1fr", md: "1fr minmax(0, 640px) minmax(0, 640px) 1fr" }
    : { base: "1fr", md: "1fr minmax(0, 1152px) minmax(0, 128px) 1fr" };

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

  return (
    <>
      <Box
        display="grid"
        gridTemplateColumns={gridTemplateColumns}
        width="100%"
        minHeight="auto"
        id="mainContent"
        role="main"
        sx={{
          "--mobile-panel-height": `${mobilePanelHeight}px`,
        }}
      >
        <Flex
          gridColumn={{ base: "1 / -1", md: "1 / span 2" }}
          flexDirection="column"
          minWidth="0"
          justifyContent="flex-end"
          alignItems="flex-end"
          bgColor="ui.bg.default"
          paddingBottom={{
            base: showChat ? "var(--mobile-panel-height)" : "auto",
            md: "0",
          }}
        >
          <Flex
            width={{ base: "100%", md: "100%" }}
            maxWidth={{ md: showChat ? "640px" : "1152px" }}
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
              <Box paddingBottom="l" flex="1">
                {isLoading ? (
                  <CatalogResultsSkeleton />
                ) : latestResults && Object.keys(latestResults).length > 0 ? (
                  <>
                    {isCatalogResults(latestResults) && (
                      <CatalogResults results={latestResults} />
                    )}
                  </>
                ) : (
                  <Box
                    width="100%"
                    marginTop="s"
                    paddingX={{ base: "s", md: "l" }}
                  >
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
          id="research-assistant-chat-panel"
          gridColumn={{ base: "1 / -1", md: "3 / span 2" }}
          flexDirection="column"
          bgColor="section.research.primary"
          height={{
            base: showChat ? "var(--mobile-panel-height)" : "auto",
            md: "100vh",
          }}
          maxHeight={{
            base: showChat ? "var(--mobile-panel-height)" : "auto",
            md: "none",
          }}
          position={{ base: "fixed", md: "sticky" }}
          top={{ base: "auto", md: "0" }}
          bottom={{ base: "0", md: "auto" }}
          left={{ base: "0", md: "auto" }}
          right={{ base: "0", md: "auto" }}
          zIndex="1000"
          minWidth="0"
          minHeight="0"
          width={{ base: "100%", md: "auto" }}
          justifyContent="flex-start"
          alignItems="flex-start"
          borderRadius={{ base: "8px 8px 0 0", md: "0" }}
        >
          <Flex
            width="100%"
            flexDirection="column"
            height="100%"
            minHeight="0"
            justifyContent="flex-start"
            alignItems="flex-start"
          >
            <ResearchAssistantPanel
              onResizeStart={handleResizeStart}
              onResizeKeyDown={handleResizeKeyDown}
              onExpandToFull={handleExpandToFull}
              onDecreaseToMin={handleDecreaseToMin}
              panelHeight={mobilePanelHeight}
              minPanelHeight={DEFAULT_MOBILE_PANEL_HEIGHT}
              maxPanelHeight={getMaxPanelHeight()}
            />
          </Flex>
        </Flex>
      </Box>
    </>
  );
};

export default ResearchAssistant;
