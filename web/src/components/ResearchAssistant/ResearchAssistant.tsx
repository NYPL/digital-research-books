import React, { useEffect, useState } from "react";
import { useResearchAssistant } from "./useResearchAssistant";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";
import ResultsList from "../ResultsList/ResultsList";
import {
  Box,
  Button,
  Heading,
  Pagination,
  Text,
} from "@nypl/design-system-react-components";
import DrbBreakout from "../DrbBreakout/DrbBreakout";
import DrbHero from "../DrbHero/DrbHero";
import ResearchAssistantNav from "./ResearchAssistantNav";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import { proxyUrlConstructor, readFetcher } from "~/src/lib/api/SearchApi";
import { LinkResult } from "~/src/types/LinkQuery";
import { SearchQuery, SearchQueryDefaults } from "~/src/types/SearchQuery";
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
  const [searchQuery, setSearchQuery] = useState({ ...SearchQueryDefaults });

  const [showWebReader, setShowWebReader] = useState(false);
  const [linkResults, setLinkResults] = useState<LinkResult>();
  const numberOfWorks = results?.totalWorks;
  const resultsPaging = results?.paging;
  const firstElement =
    (resultsPaging?.currentPage - 1) * resultsPaging?.recordsPerPage + 1;
  const lastElement =
    searchQuery?.page <= resultsPaging?.lastPage
      ? resultsPaging?.currentPage * resultsPaging?.recordsPerPage
      : numberOfWorks;

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
  const onPageChange = async (select: number) => {
    const newSearchQuery: SearchQuery = {
      queries: [],
      page: select,
    };
    newSearchQuery.queries = results.searchParams.query.map(
      ([field, queryStr]) => ({
        query: queryStr,
        field: field as SearchField,
      })
    );

    newSearchQuery.filters = results.searchParams.filters.map(
      ([field, value]) => ({
        field: field,
        value: value,
      })
    );

    setSearchQuery(newSearchQuery);

    const searchResult = await searchResultsFetcher(toApiQuery(newSearchQuery));
    const chatResult = Object.assign({}, searchResult.data, {
      searchParams: results.searchParams,
    });
    setResults(chatResult);
  };
  
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
      <Box display="flex" flexDir="row" overflow="hidden">
        {results && Object.keys(results).length > 0 && (
          <Box
            padding="s"
            border="1px solid #e5e7eb"
            overflowY="auto"
            maxHeight="70vh"
            flex="1"
          >
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
              <Box>
                <Text fontSize="2" fontWeight="semibold" paddingY="xs" noSpace>
                  {numberOfWorks > 0
                    ? `${firstElement.toLocaleString()} - ${
                        numberOfWorks < lastElement
                          ? numberOfWorks.toLocaleString()
                          : lastElement.toLocaleString()
                      } of ${numberOfWorks.toLocaleString()} results matching your research criteria`
                    : "Viewing 0 items"}
                </Text>

                <ResultsList works={results.works} />

                <Pagination
                  pageCount={resultsPaging.lastPage ? resultsPaging.lastPage : 1}
                  initialPage={resultsPaging.currentPage}
                  onPageChange={(e) => onPageChange(e)}
                  __css={{ paddingTop: "m" }}
                />
              </Box>
            )}
          </Box>
        )}

        <Box
          flex="1"
          display="flex"
          flexDirection="column"
          bgColor="section.research.primary"
          border="1px solid #e5e7eb"
          maxHeight="70vh"
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
            <Button
              onClick={clearHistory}
              id="clear-history-button"
            >
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

export default ResearchAssistant;
