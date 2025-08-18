import { Box, Button, Pagination, Text } from "@nypl/design-system-react-components";
import { useState } from "react";
import { searchResultsFetcher, proxyUrlConstructor } from "~/src/lib/api/SearchApi";
import { SearchField } from "~/src/types/DataModel";
import { CatalogSearchResults, ChatResults } from "~/src/types/ResearchAssistant";
import { SearchQueryDefaults, SearchQuery } from "~/src/types/SearchQuery";
import { toApiQuery } from "~/src/util/apiConversion";
import ReaderLayout from "../ReaderLayout/ReaderLayout";
import ResultsList from "../ResultsList/ResultsList";
import ResearchAssistantViewer from "./ResearchAssistantViewer";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";


const CatalogResults: React.FC<{
  results: CatalogSearchResults;
}> = ({ results }) => {
  const {
    itemId,
    setResults,
    showWebReader,
    pdfData,
    linkResults,
  } = useResearchAssistant();

  const [searchQuery, setSearchQuery] = useState({ ...SearchQueryDefaults });

  const numberOfWorks = results?.totalWorks;
  const resultsPaging = results?.paging;
  const firstElement =
    (resultsPaging?.currentPage - 1) * resultsPaging?.recordsPerPage + 1;
  const lastElement =
    searchQuery?.page <= resultsPaging?.lastPage
      ? resultsPaging?.currentPage * resultsPaging?.recordsPerPage
      : numberOfWorks;

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
    const chatResult: ChatResults = {
      type: "catalog_search",
      data: {
        ...searchResult.data,
        searchParams: results.searchParams,
      },
    };
    setResults(chatResult);
  };

  const proxyUrl: string = proxyUrlConstructor();
  const backUrl = "/research-assistant";

  return (
    <>
      {((results && Object.keys(results).length > 0) || showWebReader) && (
        <Box
          padding="s"
          border="1px solid #e5e7eb"
          overflowY="auto"
          maxHeight="80vh"
          flex="1"
        >
          {showWebReader ? (
            <>
              {pdfData ? (
                <ResearchAssistantViewer itemId={itemId} pdfData={pdfData} />
              ) : (
                <ReaderLayout
                  linkResult={linkResults}
                  proxyUrl={proxyUrl}
                  backUrl={backUrl}
                />
              )}
            </>
          ) : (
            <Box>
              <Text fontSize="2" fontWeight="semibold" paddingY="xs" noSpace>
                {numberOfWorks > 0
                  ? `${firstElement.toLocaleString()} - ${numberOfWorks < lastElement
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
    </>
  );
};

export default CatalogResults;