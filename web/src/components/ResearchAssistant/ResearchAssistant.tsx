import React, { useEffect, useState } from "react";
import {
  Box,
  Button,
  Heading,
  Pagination,
  Text,
} from "@nypl/design-system-react-components";
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import ResearchAssistantInput from "./ResearchAssistantInput";
import ResultsList from "../NewResultsList/ResultsList";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResultsBanner from "./ResultsBanner";
import { LinkResult } from "~/src/types/LinkQuery";
import { proxyUrlConstructor, readFetcher } from "~/src/lib/api/SearchApi";
import { SearchQuery, SearchQueryDefaults } from "~/src/types/SearchQuery";
import { searchResultsFetcher } from "~/src/lib/api/SearchApi";
import { SearchField } from "~/src/types/DataModel";
import { toApiQuery } from "~/src/util/apiConversion";
import { useResearchAssistant } from "./useResearchAssistant";
import { ResultPageProvider } from "~/src/context/ResultPageContext";

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
  const resultsPagingText =
    numberOfWorks > 0
      ? `${firstElement.toLocaleString()} - ${numberOfWorks < lastElement
        ? numberOfWorks.toLocaleString()
        : lastElement.toLocaleString()
      } of ${numberOfWorks.toLocaleString()} results matching your research criteria`
      : "Viewing 0 items";

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
    <ResultPageProvider value={{ onReadOnline: handleReadOnline, page: "vra" }}>
      <Box display="flex" flexDir="row">
        {results && Object.keys(results).length > 0 && (
          <Box bgColor="ui.bg.default" paddingX="l" flex="1">
            {showWebReader ? (
              linkResults && (
                <>
                  <Button
                    onClick={() => setShowWebReader(false)}
                    id="close-reader-button"
                  >
                    Close reader
                  </Button>
                  <ReaderLayout
                    linkResult={linkResults}
                    proxyUrl={proxyUrl}
                    backUrl={backUrl}
                  />
                </>
              )
            ) : (
              <Box paddingBottom="l">
                <Text
                  bgColor="ui.bg.default"
                  borderBottom="1px solid"
                  borderColor="ui.border.default"
                  boxSizing="content-box"
                  fontSize="2"
                  fontWeight="semibold"
                  lineHeight="40px"
                  marginX="-2rem"
                  paddingX="l"
                  paddingY="s"
                  position="sticky"
                  top="0"
                  zIndex="999"
                >
                  {resultsPagingText}
                </Text>
                <ResultsBanner />
                <ResultsList works={results.works} />
                <Pagination
                  pageCount={
                    resultsPaging.lastPage ? resultsPaging.lastPage : 1
                  }
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
