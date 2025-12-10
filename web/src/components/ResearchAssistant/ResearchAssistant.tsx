import React, { useEffect } from "react";
import { Box, Flex } from "@nypl/design-system-react-components";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import CatalogResults from "./CatalogResults";
import ItemResults from "./ItemResults";
import ResearchAssistantPanel from "./ResearchAssistantPanel";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import { proxyUrlConstructor } from "~/src/lib/api/SearchApi";
import EmptySearchPrompt from "../EmptySearchPrompt/EmptySearchPrompt";
import ResultsBanner from "./ResultsBanner";
import {
  MARGIN_BLEED,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";

const ResearchAssistant: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    historyStack,
    goToPreviousState,
    showWebReader,
    pdfData,
    linkResults,
    handleReadOnline,
  } = useResearchAssistant();

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

  const proxyUrl: string = proxyUrlConstructor();
  const backUrl = "/research-assistant";

  const gridTemplateColumns = "1fr 640px 640px 1fr";

  return (
    <ResultPageProvider
      value={{
        onReadOnline: handleReadOnline,
        page: "vra",
      }}
    >
      <Box
        display="grid"
        gridTemplateColumns={gridTemplateColumns}
        width="100%"
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
            width="640px"
            flexDirection="column"
            height="100%"
            justifyContent="flex-end"
            alignItems="flex-end"
          >
            <Flex flexDirection="column" flex="1" width="100%">
              {results && historyStack.length > 1 && (
                <Box
                  padding="s"
                  borderBottom="1px solid"
                  borderColor="ui.border.default"
                  height="58px"
                  marginLeft={MARGIN_BLEED}
                  paddingLeft={PADDING_COUNTER}
                >
                  <BackToResultsButton
                    handleBackToResults={() => goToPreviousState()}
                  />
                </Box>
              )}
              {!results && (
                <Box
                  padding="s"
                  borderBottom="1px solid"
                  borderColor="ui.border.default"
                  height="58px"
                  marginLeft={MARGIN_BLEED}
                  paddingLeft={PADDING_COUNTER}
                />
              )}
              {showWebReader ? (
                <Box flex="1" marginTop="s" marginRight="s">
                  {!pdfData && (
                    <ReaderLayout
                      linkResult={linkResults}
                      proxyUrl={proxyUrl}
                      backUrl={backUrl}
                    />
                  )}
                </Box>
              ) : (
                <Box
                  paddingLeft="s"
                  paddingRight="l"
                  paddingBottom="l"
                  flex="1"
                >
                  {results && Object.keys(results).length > 0 ? (
                    <>
                      {results.type === "catalog_search" && (
                        <CatalogResults results={results.data} />
                      )}
                      {results.type === "item_search" && (
                        <ItemResults results={results.data} />
                      )}
                    </>
                  ) : (
                    <Box width="100%" marginTop="s">
                      <ResultsBanner />
                      <EmptySearchPrompt />
                    </Box>
                  )}
                </Box>
              )}
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
    </ResultPageProvider>
  );
};

export default ResearchAssistant;
