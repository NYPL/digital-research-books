import {
  Box,
  Flex,
  Pagination,
  Text,
} from "@nypl/design-system-react-components";
import { useState } from "react";
import { searchResultsFetcher } from "~/src/lib/api/SearchApi";
import { SearchField } from "~/src/types/DataModel";
import {
  CatalogSearchResults,
  ChatResults,
} from "~/src/types/ResearchAssistant";
import { SearchQueryDefaults, SearchQuery } from "~/src/types/SearchQuery";
import { toApiQuery } from "~/src/util/apiConversion";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import ResultsBanner from "./ResultsBanner";
import ResultsList from "../NewResultsList/ResultsList";
import EmptySearchSvg from "../Svgs/EmptySearchSvg";

const CatalogResults: React.FC<{
  results: CatalogSearchResults;
}> = ({ results }) => {
  const { setViewState } = useResearchAssistant();

  const [searchQuery, setSearchQuery] = useState({ ...SearchQueryDefaults });

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
      : "No results matching your research criteria";

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
    setViewState((prev) => ({
      ...prev,
      results: chatResult,
    }));
  };
  
  const contentPaddingValue = "2rem"; 
  const outerMarginCalc = "calc((100vw - 1280px) / 2)";

  return (
    <Flex flexDir="column" bgColor="ui.bg.default" gap="s">
      <Text
        bgColor="ui.bg.default"
        borderBottom="1px solid"
        borderColor="ui.border.default"
        boxSizing="content-box"
        fontSize="desktop.heading.heading7"
        fontWeight="bold"
        lineHeight="40px"
        marginX="-2rem"
        paddingX="l"
        paddingY="s"
        position="sticky"
        top="0"
        zIndex="999"
        marginLeft={`calc(${outerMarginCalc} * -1 - ${contentPaddingValue})`}
        paddingLeft={`calc(${outerMarginCalc} + ${contentPaddingValue})`}
      >
        {resultsPagingText}
      </Text>
      <ResultsBanner />
      {Object.keys(results).length > 0 ? (
        <>
          <ResultsList works={results.works} />
          <Pagination
            pageCount={resultsPaging.lastPage ? resultsPaging.lastPage : 1}
            initialPage={resultsPaging.currentPage}
            onPageChange={(e) => onPageChange(e)}
            __css={{
              paddingTop: "m",
              "a, li > a[aria-current='page']": {
                color: "var(--nypl-colors-section-research-secondary)",
                borderColor:
                  "var(--nypl-colors-section-research-secondary)",
                svg: {
                  fill: "var(--nypl-colors-section-research-secondary)",
                },
              },
              "a[aria-disabled='true']": {
                color: "var(--nypl-colors-ui-disabled-primary)",
                svg: {
                  fill: "var(--nypl-colors-ui-disabled-primary)",
                },
              },
            }}
          />
        </>
      ) : (
        <>
          <Box>
            <EmptySearchSvg />
            <Box>No results were found.</Box>
          </Box>
        </>
      )}
    </Flex>
  );
};

export default CatalogResults;
