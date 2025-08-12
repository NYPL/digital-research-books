import React, { useEffect } from "react";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";
import {
  Box,
  Button,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "./ResearchAssistantNav";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import {
  ResearchAssistantProvider,
  useResearchAssistant,
} from "~/src/context/ResearchAssistantContext";
import CatalogResults from "./CatalogResults";
import ItemResults from "./ItemResults";

const ResearchAssistantInner: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    isLoading,
    error,
    clearHistory,
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
      <Box display="flex" flexDir="row" overflow="hidden" height="80vh">
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

        <Box
          flex="1"
          display="flex"
          flexDirection="column"
          bgColor="section.research.primary"
          border="1px solid #e5e7eb"
          maxHeight="80vh"
        >
          <Box
            display="flex"
            justifyContent="space-between"
            alignItems="center"
            paddingX="l"
            paddingY="s"
            borderBottom="1px white solid"
          >
            <Heading level="h2" size="heading3" color="ui.white" margin="0">
              Virtual Research Assistant
            </Heading>
            <Button onClick={clearHistory} id="clear-history-button">
              Clear chat
            </Button>
          </Box>

          <ResearchAssistantWindow messages={messages} isLoading={isLoading} />

          {error && <Text>{error}</Text>}

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
