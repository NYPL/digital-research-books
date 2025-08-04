import React, { useEffect, useState } from "react";
import { useResearchAssistant } from "./useResearchAssistant";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";
import styles from "../../../styles/components/ResearchAssistant.module.scss";
import ResultsList from "../ResultsList/ResultsList";
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
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import { proxyUrlConstructor, readFetcher } from "~/src/lib/api/SearchApi";
import { LinkResult } from "~/src/types/LinkQuery";

const ResearchAssistant: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    isLoading,
    error,
    clearHistory,
  } = useResearchAssistant();

  const [showWebReader, setShowWebReader] = useState(false);
  const [linkResults, setLinkResults] = useState<LinkResult>();

  useEffect(() => {
    const initialMessage = sessionStorage.getItem(
      "researchAssistantInitialMessage"
    );
    if (initialMessage) {
      sendMessage(initialMessage);
      sessionStorage.removeItem("researchAssistantInitialMessage");
    }
  }, [sendMessage]);

  const handleReadOnline = async (linkId: number) => {
    setShowWebReader(true);
    const linkResult: LinkResult = await readFetcher(linkId);
    setLinkResults(linkResult);
  };

  const proxyUrl: string = proxyUrlConstructor();
  const backUrl = "/research-assistant";

  return (
    <ResultPageProvider
      value={{ onReadOnline: handleReadOnline, page: "researchAssistant" }}
    >
      <DrbBreakout
        breadcrumbsData={[
          { url: "/research-assistant", text: "Virtual Research Assistant" },
        ]}
      >
        <DrbHero />
        <ResearchAssistantNav />
      </DrbBreakout>

      <Box className={styles.pageContainer}>
        {results && (
          <Box className={styles.resultsPanel}>
            {showWebReader ? (
              linkResults && (
                <Box position="relative">
                  <Button
                    onClick={() => setShowWebReader(false)}
                    id="close-reader-button"
                    position="absolute"
                    right="s"
                    top="s"
                  >
                    Close reader
                  </Button>
                  <ReaderLayout
                    linkResult={linkResults}
                    proxyUrl={proxyUrl}
                    backUrl={backUrl}
                  />
                </Box>
              )
            ) : (
              <>
                {results.totalWorks ? (
                  <Heading
                    level="h3"
                    size="heading5"
                    className={styles.resultsHeader}
                  >
                    <>
                      {results.totalWorks} results matching your research
                      criteria
                    </>
                  </Heading>
                ) : null}

                <ResultsList works={results.works} />
              </>
            )}
          </Box>
        )}

        <section className={styles.chatPanel}>
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
            <Button
              onClick={clearHistory}
              className={styles.clearButton}
              id="clear-history-button"
            >
              Clear chat
            </Button>
          </Box>

          <ResearchAssistantWindow messages={messages} isLoading={isLoading} />

          {error && <Text className={styles.errorText}>{error}</Text>}

          <ResearchAssistantInput
            onSendMessage={sendMessage}
            isDisabled={isLoading}
            messages={messages}
          />
        </section>
      </Box>
    </ResultPageProvider>
  );
};

export default ResearchAssistant;
