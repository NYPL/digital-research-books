import React, { useEffect } from "react";
import { Box } from "@nypl/design-system-react-components";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import CatalogResults from "./CatalogResults";
import ItemResults from "./ItemResults";
import ResearchAssistantPanel from "./ResearchAssistantPanel";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import { proxyUrlConstructor } from "~/src/lib/api/SearchApi";

const ResearchAssistant: React.FC = () => {
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

  return (
    <ResultPageProvider
      value={{
        onReadOnline: handleReadOnline,
        page: "vra",
      }}
    >
      <Box display="flex" flexDir="row" maxWidth="1280px" margin="0 auto">
        {((results && Object.keys(results).length > 0) || showWebReader) && (
          <Box display="flex" flexDirection="column" flex="1">
            {historyStack.length > 1 && (
              <Box padding="s" borderBottom="1px solid" borderColor="ui.border">
                <BackToResultsButton handleBackToResults={goToPreviousState} />
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
              <Box
                paddingX="l"
                paddingBottom="l"
                flex="1"
                bgColor="ui.bg.default"
              >
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

export default ResearchAssistant;
