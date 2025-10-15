import React, { useEffect } from "react";
import {
  Box,
  Button,
  Heading,
  Icon,
  Text,
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import {
  ResearchAssistantProvider,
  useResearchAssistant,
} from "~/src/context/ResearchAssistantContext";
import CatalogResults from "./CatalogResults";
import ItemResults from "./ItemResults";
import ResearchAssistantViewer from "./ResearchAssistantViewer";
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import ResearchAssistantInput from "./ResearchAssistantInput";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantNav from "./ResearchAssistantNav";
import { proxyUrlConstructor } from "~/src/lib/api/SearchApi";
import { ResultPageProvider } from "~/src/context/ResultPageContext";

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
    itemId,
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
        page: "researchAssistant",
      }}
    >
      <DrbBreakout
        breadcrumbsData={[
          { url: "/research-assistant", text: "Virtual Research Assistant" },
        ]}
      >
        <DrbHero />
        <ResearchAssistantNav />
      </DrbBreakout>
      <Box display="flex" flexDir="row" height="100vh">
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
                {pdfData ? (
                  <ResearchAssistantViewer itemId={itemId} pdfData={pdfData} />
                ) : (
                  <ReaderLayout
                    linkResult={linkResults}
                    proxyUrl={proxyUrl}
                    backUrl={backUrl}
                  />
                )}
              </Box>
            ) : (
              <Box paddingX="l" paddingBottom="l" flex="1">
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

        <Box
          flex="1"
          display="flex"
          flexDirection="column"
          bgColor="section.research.primary"
          maxHeight="100vh"
          position="sticky"
          top="0"
        >
          <Box
            bgColor="section.research.primary"
            display="flex"
            justifyContent="space-between"
            alignItems="center"
            paddingX="l"
            paddingY="s"
            borderBottom="1px white solid"
            position="sticky"
            top="0"
            zIndex="999"
          >
            <Heading
              level="h2"
              size="heading7"
              color="ui.white"
              display="flex"
              alignItems="center"
              gap="xs"
            >
              <ResearchAssistantIcon inCircle />
              <span>Virtual Research Assistant</span>
            </Heading>
            <Button onClick={clearHistory} id="clear-history-button">
              Clear chat
            </Button>
          </Box>

          <ResearchAssistantWindow messages={messages} isLoading={isLoading} />

          {error && <Text fontWeight="bold">{error}</Text>}

          <ResearchAssistantInput
            onSendMessage={sendMessage}
            isDisabled={isLoading}
            messages={messages}
          />
        </Box>
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
