import React, { useState } from "react";
import {
  Heading,
  Pagination,
  Button,
  Icon,
  Box,
  HorizontalRule,
  Flex,
  Form,
  useModal,
  Template,
  Text,
  useNYPLBreakpoints,
  TemplateBreakout,
  TemplateContent,
  TemplateMain,
  TemplateSidebar,
  TemplateFull,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import { FacetItem, Query } from "~/src/types/DataModel";
import {
  ApiSearchResult,
  Filter,
  SearchQuery,
  SearchQueryDefaults,
} from "~/src/types/SearchQuery";
import { sortMap } from "~/src/constants/sorts";
import ResultsList from "../ResultsList/ResultsList";
import { toLocationQuery, toApiQuery } from "~/src/util/apiConversion";
import Filters from "../SearchFilters/ResultsFilters";
import ResultsSorts from "../ResultsSorts/ResultsSorts";
import SearchHeader from "../SearchHeader/SearchHeader";
import { ApiWork } from "~/src/types/WorkQuery";
import useFeatureFlags from "~/src/context/FeatureFlagContext";
import TotalWorks from "../TotalWorks/TotalWorks";
import filterFields from "~/src/constants/filters";
import { findFiltersForField } from "~/src/util/SearchQueryUtils";
import DrbBreakout from "../DrbBreakout/DrbBreakout";

const SearchResults: React.FC<{
  searchQuery: SearchQuery;
  searchResults: ApiSearchResult;
}> = (props) => {
  const searchResults = props.searchResults;
  const [searchQuery, setSearchQuery] = useState({
    ...SearchQueryDefaults,
    ...props.searchQuery,
  });
  const { isFlagActive } = useFeatureFlags();

  const { onClose, onOpen, Modal } = useModal();

  const { isLargerThanLarge } = useNYPLBreakpoints();

  const router = useRouter();

  const sendSearchQuery = async (searchQuery: SearchQuery) => {
    router.push({
      pathname: "/search",
      query: toLocationQuery(toApiQuery(searchQuery)),
    });
  };

  // The Display Items heading (Search Results for ... )
  const getDisplayItemsHeading = (searchQuery: SearchQuery) => {
    // If a display query is set, it is shown instead of the actual query
    if (searchQuery.display) {
      return `${searchQuery.display.field}: "${searchQuery.display.query}"`;
    }
    //If not, the actual query is shown.
    const queries = searchQuery.queries.map((query: Query, index: any) => {
      const joiner = index < searchQuery.queries.length - 1 ? " and " : "";
      return `${query.field}: "${query.query}"${joiner}`;
    });
    return queries && queries.join("");
  };

  const getAvailableLanguages = (
    searchResults: ApiSearchResult
  ): FacetItem[] => {
    const facets: FacetItem[] =
      searchResults &&
      searchResults.data.facets &&
      searchResults.data.facets["languages"];

    const selectedLanguages = findFiltersForField(
      searchQuery.filters,
      filterFields.language
    );
    // adds selected language to available languages if it doesn't exist
    if (selectedLanguages) {
      selectedLanguages.forEach((lang) => {
        if (!facets.find((facet) => facet.value === lang.value)) {
          facets.push({ value: lang.value.toString(), count: 0 });
        }
      });
    }

    return facets;
  };

  const numberOfWorks = searchResults.data.totalWorks;
  const works: ApiWork[] = searchResults.data.works;

  const searchPaging = searchResults.data.paging;
  const firstElement =
    (searchPaging.currentPage - 1) * searchPaging.recordsPerPage + 1;
  const lastElement =
    searchQuery.page <= searchPaging.lastPage
      ? searchPaging.currentPage * searchPaging.recordsPerPage
      : numberOfWorks;

  // When Filters change, it should reset the page number while preserving all other search preferences.
  const changeFilters = (newFilters?: Filter[]) => {
    const newSearchQuery: SearchQuery = {
      ...searchQuery,
      ...{ page: SearchQueryDefaults.page },
      ...(newFilters && { filters: newFilters }),
    };
    setSearchQuery(newSearchQuery);
    sendSearchQuery(newSearchQuery);
  };

  const changeShowAll = (showAll: boolean) => {
    const newSearchQuery: SearchQuery = {
      ...searchQuery,
      showAll: showAll,
    };
    setSearchQuery(newSearchQuery);
    sendSearchQuery(newSearchQuery);
  };

  const onChangePerPage = (e) => {
    e.preventDefault();
    const newPage = 0;
    const newPerPage = e.target.value;
    if (newPerPage !== searchQuery.perPage) {
      const newSearchQuery: SearchQuery = Object.assign({}, searchQuery, {
        page: newPage,
        perPage: newPerPage,
        total: numberOfWorks || 0,
      });
      setSearchQuery(newSearchQuery);
      sendSearchQuery(newSearchQuery);
    }
  };

  const onChangeSort = (e) => {
    e.preventDefault();
    if (
      e.target.value !==
      Object.keys(sortMap).find((key) => sortMap[key] === searchQuery.sort)
    ) {
      const newSearchQuery: SearchQuery = Object.assign({}, searchQuery, {
        sort: sortMap[e.target.value],
        page: SearchQueryDefaults.page,
      });
      setSearchQuery(newSearchQuery);
      sendSearchQuery(newSearchQuery);
    }
  };

  const onPageChange = (select: number) => {
    const newSearchQuery: SearchQuery = Object.assign({}, searchQuery, {
      page: select,
    });
    setSearchQuery(newSearchQuery);
    sendSearchQuery(newSearchQuery);
  };

  const breakoutElement = (
    <DrbBreakout
      breadcrumbsData={[
        {
          url: `/search`,
          text: "Search Results",
        },
      ]}
    >
      <SearchHeader searchQuery={searchQuery}></SearchHeader>
      <Flex
        flexDir={{ base: "column", md: "row" }}
        padding="s"
        paddingTop="0"
        bg="ui.gray.x-light-cool"
        gap="s"
        display={{ base: "flex", lg: "none" }}
      >
        <Button
          id="filter-button"
          onClick={onOpen}
          variant="secondary"
          sx={{
            width: { base: "100%", md: "fit-content" },
          }}
        >
          Filter results
        </Button>
        <Modal
          bodyContent={
            <Box data-testid="filters-modal-content">
              <Button variant="text" onClick={onClose} id="modal-button">
                <Flex align="center">
                  <Icon
                    decorative={true}
                    name="arrow"
                    size="medium"
                    iconRotation="rotate90"
                  />
                  Go Back
                </Flex>
              </Button>
              <Box>
                <ResultsSorts
                  isModal={true}
                  perPage={searchQuery.perPage}
                  sort={searchQuery.sort}
                  sortMap={sortMap}
                  onChangePerPage={(e) => onChangePerPage(e)}
                  onChangeSort={(e) => onChangeSort(e)}
                />
              </Box>
              <form name="filterForm">
                <Filters
                  filters={searchQuery.filters}
                  showAll={searchQuery.showAll}
                  languages={getAvailableLanguages(searchResults)}
                  isModal={true}
                  changeFilters={(filters: Filter[]) => {
                    changeFilters(filters);
                  }}
                  changeShowAll={(showAll: boolean) => {
                    changeShowAll(showAll);
                  }}
                />
              </form>
            </Box>
          }
        />
        {searchQuery.filters.length > 0 && !isLargerThanLarge && (
          <Button
            id="clear-filters-button"
            variant="secondary"
            type="reset"
            onClick={() => {
              changeFilters([]);
            }}
            sx={{
              width: { base: "100%", md: "fit-content" },
            }}
          >
            Clear Filters
          </Button>
        )}
      </Flex>
    </DrbBreakout>
  );

  const contentSidebarElement = (
    <Form
      id="search-filter-form"
      bgColor="ui.gray.x-light-cool"
      border="1px solid"
      borderColor="ui.border.default"
      borderRadius="8px"
      display={{ base: "none", md: "block" }}
      gap="grid.s"
      padding="s"
    >
      <Filters
        filters={searchQuery.filters}
        showAll={searchQuery.showAll}
        languages={getAvailableLanguages(searchResults)}
        changeFilters={(filters: Filter[]) => {
          changeFilters(filters);
        }}
        changeShowAll={(showAll: boolean) => {
          changeShowAll(showAll);
        }}
      />
    </Form>
  );

  const contentFullElement = (
    <>
      {isFlagActive("totalCount") && (
        <Box float="right">
          <TotalWorks totalWorks={numberOfWorks} />
        </Box>
      )}
      <Box className="search-heading">
        <Box role="alert">
          <Heading level="h1" size="heading3" id="page-title-heading">
            Search results for {getDisplayItemsHeading(searchQuery)}
          </Heading>
        </Box>
      </Box>
      <HorizontalRule bg="section.research.primary" marginY="s" />
      <Flex justify="space-between" align="center">
        <Text fontSize="1.75rem" className="page-counter" __css={{ m: "0" }}>
          {numberOfWorks > 0
            ? `Viewing ${firstElement.toLocaleString()} - ${numberOfWorks < lastElement
              ? numberOfWorks.toLocaleString()
              : lastElement.toLocaleString()
            } of ${numberOfWorks.toLocaleString()} items`
            : "Viewing 0 items"}
        </Text>
        <Form id="results-sorts-form" display={["none", "none", "block"]}>
          <ResultsSorts
            perPage={searchQuery.perPage}
            sort={searchQuery.sort}
            sortMap={sortMap}
            onChangePerPage={(e) => onChangePerPage(e)}
            onChangeSort={(e) => onChangeSort(e)}
          />
        </Form>
      </Flex>
    </>
  )

  const contentElement = (
    <>
      <ResultsList works={works} />
      <Pagination
        pageCount={searchPaging.lastPage ? searchPaging.lastPage : 1}
        initialPage={searchPaging.currentPage}
        onPageChange={(e) => onPageChange(e)}
        __css={{ paddingTop: "m", paddingBottom: "l" }}
      />
    </>
  );

  return (
    <Template variant="sidebarLeft">
      <TemplateBreakout>{breakoutElement}</TemplateBreakout>
      <TemplateMain>
        <TemplateFull>{contentFullElement}</TemplateFull>
        <TemplateSidebar>{contentSidebarElement}</TemplateSidebar>
        <TemplateContent>{contentElement}</TemplateContent>
      </TemplateMain>
    </Template>
  );
};

export default SearchResults;
