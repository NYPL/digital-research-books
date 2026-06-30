import { Flex, Pagination, Text } from "@nypl/design-system-react-components";
import { useState } from "react";
import {
  HEADER_HEIGHT,
  MARGIN_BLEED,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { CatalogSearchResults } from "~/src/types/ResearchAssistant";
import { SearchQueryDefaults } from "~/src/types/SearchQuery";
import ResultsList from "../../NewResultsList/ResultsList";
import ResultsBanner from "../ResultsBanner";

const CatalogResults: React.FC<{
  results: CatalogSearchResults;
}> = ({ results }) => {
  const [searchQuery] = useState({ ...SearchQueryDefaults });

  const numberOfRecords = results?.paging?.totalRecords || 0;
  const resultsPaging = results?.paging;
  const firstElement =
    (resultsPaging?.currentPage - 1) * resultsPaging?.recordsPerPage + 1;
  const lastElement =
    searchQuery?.page <= resultsPaging?.lastPage
      ? resultsPaging?.currentPage * resultsPaging?.recordsPerPage
      : numberOfRecords;
  const resultsPagingText =
    numberOfRecords > 0
      ? `${firstElement.toLocaleString()} - ${
          numberOfRecords < lastElement
            ? numberOfRecords.toLocaleString()
            : lastElement.toLocaleString()
        } of ${numberOfRecords.toLocaleString()} results matching your research criteria`
      : "No results matching your research criteria";

  // TODO: implement pagination when API supports it
  // const onPageChange = async (select: number) => {
  //   const newSearchQuery: SearchQuery = {
  //     queries: [],
  //     page: select,
  //   };
  //   newSearchQuery.queries = results.searchParams.query.map(
  //     ([field, queryStr]) => ({
  //       query: queryStr,
  //       field: field as SearchField,
  //     })
  //   );

  //   newSearchQuery.filters = results.searchParams.filters.map(
  //     ([field, value]) => ({
  //       field: field,
  //       value: value,
  //     })
  //   );

  //   setSearchQuery(newSearchQuery);

  //   const searchResult = await searchResultsFetcher(toApiQuery(newSearchQuery));
  //   const chatResult: ChatResults = {
  //     type: ConversationType.Catalog,
  //     data: {
  //       ...searchResult.data,
  //       searchParams: results.searchParams,
  //     },
  //   };
  //   setViewState((prev) => ({
  //     ...prev,
  //     results: chatResult,
  //   }));
  // };

  return (
    <Flex flexDir="column" bgColor="ui.bg.default" gap="s">
      <Text
        position="sticky"
        bgColor="ui.bg.default"
        borderBottom="1px solid"
        borderColor="ui.border.default"
        fontSize="desktop.heading.heading7"
        fontWeight="bold"
        lineHeight="125%"
        paddingY="s"
        top="0"
        zIndex="999"
        marginLeft={MARGIN_BLEED}
        height={{ base: "fit-content", md: HEADER_HEIGHT }}
        paddingLeft={PADDING_COUNTER}
      >
        {resultsPagingText}
      </Text>
      <Flex paddingX={{ base: "s", md: "m" }} flexDir="column" gap="s">
        <ResultsBanner />
        {Object.keys(results).length > 0 && (
          <>
            <ResultsList works={results.editions} />
            <Pagination
              pageCount={resultsPaging.lastPage ? resultsPaging.lastPage : 1}
              initialPage={resultsPaging.currentPage}
              // onPageChange={(e) => onPageChange(e)}
              __css={{
                paddingTop: "m",
                "a, li > a[aria-current='page']": {
                  color: "var(--nypl-colors-section-research-secondary)",
                  borderColor: "var(--nypl-colors-section-research-secondary)",
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
        )}
      </Flex>
    </Flex>
  );
};

export default CatalogResults;
