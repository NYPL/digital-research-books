import React from "react";
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
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";

const ResearchAssistant: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    isLoading,
    error,
    clearHistory,
  } = useResearchAssistant();

  const breakoutElement = (
    <DrbBreakout
      breadcrumbsData={[
        { url: "/research-assistant", text: "Virtual Research Assistant" },
      ]}
    />
  );

  const contentPrimaryElement = (
    <Box className={styles.pageContainer}>
      {results && (
        <section className={styles.resultsPanel}>
          {results.totalWorks ? (
            <Heading level="h3" className={styles.resultsHeader}>
              <>{results.totalWorks} results matching your research criteria</>
            </Heading>
          ) : null}

          <ResultsList works={results.works} />
        </section>
      )}

      <section className={styles.chatPanel}>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Heading level="h2" size="heading3" color="ui.white">
            Virtual Research Assistant
          </Heading>
          <Button onClick={clearHistory} className={styles.clearButton} id="clear-history-button">
            Clear chat
          </Button>
        </Box>

        <ResearchAssistantWindow messages={messages} isLoading={isLoading} />

        {error && <p className={styles.errorText}>{error}</p>}

        <ResearchAssistantInput
          onSendMessage={sendMessage}
          isDisabled={isLoading}
          messages={messages}
        />
      </section>
    </Box>
  );

  return (
    <TemplateAppContainer
      breakout={breakoutElement}
      contentPrimary={contentPrimaryElement}
    />
  );
};

export default ResearchAssistant;
