import React, { useEffect } from "react";
import {
  Box,
  Button,
  Icon,
} from "@nypl/design-system-react-components";
import {
  ResearchAssistantProvider,
  useResearchAssistant,
} from "~/src/context/ResearchAssistantContext";
import CatalogResults from "./CatalogResults";
import ItemResults from "./ItemResults";
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import { proxyUrlConstructor } from "~/src/lib/api/SearchApi";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import ResearchAssistantPanel from "./ResearchAssistantPanel";

const ResearchAssistantInner: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    isLoading,
    error,
    historyStack,
    goToPreviousState,
    clearHistory,
    showWebReader,
    pdfData,
    linkResults,
    handleReadOnline,
    handlePreview,
  } = useResearchAssistant();

  useEffect(() => {
    const initialMessage = sessionStorage.getItem(
      "researchAssistantInitialMessage"
    );
    if (initialMessage) {
      sendMessage(initialMessage);
      sessionStorage.removeItem("researchAssistantInitialMessage");
    }
  }, [sendMessage]);

  const proxyUrl: string = proxyUrlConstructor();
  const backUrl = "/research-assistant";

  return (
    <ResultPageProvider
      value={{
        onPreview: handlePreview,
        onReadOnline: handleReadOnline,
        page: "vra",
      }}
    >
      <Box display="flex" flexDir="row">
        {((results && Object.keys(results).length > 0) || showWebReader) && (
          <Box display="flex" flexDirection="column" flex="1">
            {historyStack.length > 1 && (
              <Box padding="s" borderBottom="1px solid" borderColor="ui.border">
                <Button
                  variant="text"
                  id="back-button"
                  color="section.research.secondary"
                  onClick={goToPreviousState}
                >
                  <Icon
                    name="arrow"
                    iconRotation="rotate90"
                    align="left"
                    size="small"
                  />
                  Back to results
                </Button>
              </Box>
            )}
            {showWebReader ? (
              <Box flex="1">
                {!pdfData && (
                  <ReaderLayout
                    linkResult={linkResults}
                    proxyUrl={proxyUrl}
                    backUrl={backUrl}
                  />
                )}
              </Box>
            ) : (
              <Box paddingX="l" paddingBottom="l" flex="1" bgColor="ui.bg.default">
                {results && Object.keys(results).length > 0 && (
                  <>
                    {results.type === "catalog_search" && (
                      <CatalogResults results={results.data} />
                    )}
                    {results.type === "item_search" && (
                      <ItemResults results={results.data} />
                    )}
                  </>
                )}
              </Box>
            )}
          </Box>
        )}

        <ResearchAssistantPanel
          messages={messages}
          isLoading={isLoading}
          error={error}
          onSendMessage={sendMessage}
          clearHistory={clearHistory}
        />
      </Box>
    </ResultPageProvider>
  );
};

const ResearchAssistant: React.FC = () => (
  <ResearchAssistantProvider>
    <ResearchAssistantInner />
  </ResearchAssistantProvider>
);

export default ResearchAssistant;
