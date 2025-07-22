import React, { useState } from "react";
import { useResearchAssistant } from "./useResearchAssistant";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";
import styles from "../../../styles/components/ResearchAssistant.module.scss";
import ResultsList from "../ResultsList/ResultsList";
import {
  Box,
  Button,
  Heading,
  TemplateAppContainer,
  Text,
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantLanding from "./ResearchAssistantLanding";

const ResearchAssistant: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    isLoading,
    error,
    clearHistory,
  } = useResearchAssistant();

  const [showChat, setShowChat] = useState(false);

  const handleLandingSubmit = (query: string) => {
    setShowChat(true);
    sendMessage(query);
  }

  const breakoutElement = (
    <DrbBreakout
      breadcrumbsData={[
        { url: "/research-assistant", text: "Virtual Research Assistant" },
      ]}
    >
      <DrbHero />
    </DrbBreakout>
  );

  const contentPrimaryElement = (
    <Box className={styles.pageContainer}>
      {results && (
        <Box className={styles.resultsPanel}>
          {results.totalWorks ? (
            <Heading
              level="h3"
              size="heading5"
              className={styles.resultsHeader}
            >
              <>{results.totalWorks} results matching your research criteria</>
            </Heading>
          ) : null}

          <ResultsList works={results.works} />
        </Box>
      )}

      <section className={styles.chatPanel}>
        <Box display="flex" justifyContent="space-between" alignItems="center" paddingX="l" paddingY="s"
          borderBottom="1px white solid">
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
  );

  // TODO: have separate page/endpoint for landing and chat ui
  if (showChat) {
    return (
        <TemplateAppContainer
          breakout = { breakoutElement }
          contentPrimary = { contentPrimaryElement }
          gridTemplateColumns = "1fr 100% 1fr"
        />
      )
  } else {
    return <ResearchAssistantLanding onSearchSubmit={handleLandingSubmit} />
  }
};

export default ResearchAssistant;
