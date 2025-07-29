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
  Pagination,
  TemplateAppContainer,
  Text,
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import { SearchQuery } from "~/src/types/SearchQuery";
import { searchResultsFetcher } from "~/src/lib/api/SearchApi";
import { SearchField } from "~/src/types/DataModel";
import { toApiQuery } from "~/src/util/apiConversion";

const ResearchAssistant: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    setResults,
    isLoading,
    error,
    clearHistory,
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

  const onPageChange = async (select: number) => {
    const searchQuery: SearchQuery = {
      queries:  [],
      page: select,
    }
    searchQuery.queries = results.searchParams.query.map(([field, queryStr]) => ({
      query: queryStr,
      field: field as SearchField,
    }));

    searchQuery.filters = results.searchParams.filters.map(([field, value]) => ({
      field: field,
      value: value,
    }));

    const newSearchResult = await searchResultsFetcher(toApiQuery(searchQuery));
    const chatResult = Object.assign({}, newSearchResult.data, {
      searchParams: results.searchParams
    });
    setResults(chatResult)
  };

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
      {results && Object.keys(results).length > 0 && (
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
          <Pagination
            pageCount={results.paging.lastPage ? results.paging.lastPage : 1}
            initialPage={results.paging.currentPage}
            onPageChange={(e) => onPageChange(e)}
            __css={{ paddingTop: "m" }}
          />
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
  );

  return (
    <TemplateAppContainer
      breakout={breakoutElement}
      contentPrimary={contentPrimaryElement}
      gridTemplateColumns="1fr 100% 1fr"
    />
  );
};

export default ResearchAssistant;
