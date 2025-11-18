import React, { useState } from "react";
import {
  Heading,
  Pagination,
  Button,
  Icon,
  Box,
  Flex,
  Form,
  useModal,
  Template,
  useNYPLBreakpoints,
  TemplateBreakout,
  TemplateContent,
  TemplateMain,
  TemplateSidebar,
  TemplateFull,
  SkeletonLoader,
  SimpleGrid,
  Menu,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import { Query } from "~/src/types/DataModel";
import {
  ApiSearchResult,
  Filter,
  SearchQuery,
  SearchQueryDefaults,
} from "~/src/types/SearchQuery";
import { sortOptions } from "~/src/constants/sorts";
import { toLocationQuery, toApiQuery } from "~/src/util/apiConversion";
import Filters from "./SearchFilters/SearchFilters";
import { ApiWork } from "~/src/types/WorkQuery";
import useFeatureFlags from "~/src/context/FeatureFlagContext";
import TotalWorks from "../TotalWorks/TotalWorks";
import ActiveFilters from "./SearchFilters/ActiveFilters";
import { capitalizeFirstLetter, deepEqual } from "~/src/util/Util";
import { getAvailableLanguages } from "~/src/util/SearchUtils";
import ResultsList from "../NewResultsList/ResultsList";
import KeywordSearchForm from "../KeywordSearchForm/KeywordSearchForm";

interface KeywordSearchProps {
  searchQuery: SearchQuery;
  searchResults: ApiSearchResult;
}

const KeywordSearch: React.FC<KeywordSearchProps> = (props) => {
  const searchResults = props.searchResults;
  const [searchQuery, setSearchQuery] = useState({
    ...SearchQueryDefaults,
    ...props.searchQuery,
  });
  const [isLoading, setIsLoading] = useState(false);

  const buildTagSetData = (filters: Filter[]) => {
    return filters.map((filter) => {
      let labelStr = capitalizeFirstLetter(filter.value.toString());

      if (filter.value === "onlyGovDoc") labelStr = "Limit to US gov docs";
      else if (filter.field === "startYear") labelStr = `From ${filter.value}`;
      else if (filter.field === "endYear") labelStr = `To ${filter.value}`;

      return {
        id: `${filter.field}-${filter.value}`,
        label: labelStr,
      };
    });
  };
  const [tagSetData, setTagSetData] = useState(
    buildTagSetData(searchQuery.filters)
  );

  const { isFlagActive } = useFeatureFlags();

  const { onClose, onOpen, Modal } = useModal();

  const { isLargerThanLarge } = useNYPLBreakpoints();

  const router = useRouter();

  const sendSearchQuery = async (searchQuery: SearchQuery) => {
    setIsLoading(true);
    await router.push({
      pathname: "/keyword-search",
      query: toLocationQuery(toApiQuery(searchQuery)),
    });
    setIsLoading(false);
  };

  // The Display Items heading (Search Results for ... )
  const getDisplayItemsHeading = (searchQuery: SearchQuery) => {
    // If a display query is set, it is shown instead of the actual query
    if (searchQuery.display) {
      return `${searchQuery.display.field}: "${searchQuery.display.query}"`;
    }
    // If not, the actual query is shown.
    const queries = searchQuery.queries.map((query: Query, index: any) => {
      const joiner = index < searchQuery.queries.length - 1 ? " and " : "";
      return `${query.field}: "${query.query}"${joiner}`;
    });
    return queries && queries.join("");
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
  const resultsPagingText =
    numberOfWorks > 0
      ? `${firstElement.toLocaleString()} - ${
          numberOfWorks < lastElement
            ? numberOfWorks.toLocaleString()
            : lastElement.toLocaleString()
        } of ${numberOfWorks.toLocaleString()} results for ${getDisplayItemsHeading(
          searchQuery
        )}`
      : "Viewing 0 items";

  const currentSortId =
    sortOptions.find((opt) => deepEqual(opt.value, searchQuery.sort))?.id || "relevance";
  const currentSortLabel =
    sortOptions.find((opt) => deepEqual(opt.value, searchQuery.sort))?.label || "Relevance";

  // When Filters change, it should reset the page number while preserving all other search preferences.
  const changeFilters = (newFilters?: Filter[]) => {
    const newSearchQuery: SearchQuery = {
      ...searchQuery,
      ...{ page: SearchQueryDefaults.page },
      ...(newFilters && { filters: newFilters }),
    };
    setTagSetData(buildTagSetData(newSearchQuery.filters));
    setSearchQuery(newSearchQuery);
    sendSearchQuery(newSearchQuery);
  };

  const onSortMenuClick = (id: string) => {
    const selected = sortOptions.find((opt) => opt.id === id);
    if (selected && selected.value !== searchQuery.sort) {
      const newSearchQuery = {
        ...searchQuery,
        sort: selected.value,
        page: SearchQueryDefaults.page,
      };
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

  const onTagSetClear = (tagSet) => {
    if (tagSet.id === "clear-filters") {
      changeFilters([]);
    } else {
      const newFilters = searchQuery.filters.filter(
        (filter) => tagSet.id !== `${filter.field}-${filter.value}`
      );
      changeFilters(newFilters);
    }
  };

  const handleSearch = (newSearchQuery: SearchQuery) => {
    setTagSetData(buildTagSetData(newSearchQuery.filters));
    setSearchQuery(newSearchQuery);
    sendSearchQuery(newSearchQuery);
  };

  const breakoutElement = (
    <>
      <KeywordSearchForm searchQuery={searchQuery} onSearch={handleSearch} />
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
                <Menu
                  bg="ui.white"
                  labelText={`Sort by: ${currentSortLabel}`}
                  listItemsData={sortOptions.map((opt) => ({
                    type: "action",
                    id: opt.id,
                    label: opt.label,
                    onClick: () => onSortMenuClick(opt.id),
                  }))}
                  selectedItem={currentSortId}
                  width="100%"
                />
              </Box>
              <form name="filterForm">
                <Filters
                  filters={searchQuery.filters}
                  languages={getAvailableLanguages(searchResults, searchQuery)}
                  isModal={true}
                  changeFilters={(filters: Filter[]) => {
                    changeFilters(filters);
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
    </>
  );

  const sidebarElement = (
    <Form
      bgColor="ui.white"
      border="1px solid"
      borderColor="ui.border.default"
      borderRadius="8px"
      display={{ base: "none", md: "block" }}
      gap="grid.s"
      padding="s"
    >
      <Filters
        filters={searchQuery.filters}
        languages={getAvailableLanguages(searchResults, searchQuery)}
        changeFilters={(filters: Filter[]) => {
          changeFilters(filters);
        }}
      />
    </Form>
  );

  const contentElement = (
    <Box paddingBottom="55px">
      {isFlagActive("totalCount") && (
        <Box float="right">
          <TotalWorks totalWorks={numberOfWorks} />
        </Box>
      )}
      {isLoading ? (
        <SimpleGrid columns={1}>
          {tagSetData.length > 0 && (
            <SkeletonLoader layout="row" showImage={false} showContent={false} />
          )}
          <SkeletonLoader layout="row" showImage={false} showContent={false} />
          <SkeletonLoader layout="row" showButton />
          <SkeletonLoader layout="row" showButton />
          <SkeletonLoader layout="row" showButton />
          <SkeletonLoader layout="row" showButton />
          <SkeletonLoader layout="row" showButton />
        </SimpleGrid>
      ) : (
        <>
          {tagSetData.length > 0 && (
            <ActiveFilters onClick={onTagSetClear} tagSetData={tagSetData} />
          )}
          <Flex justify="space-between" align="center" marginBottom="l">
            <Heading size="heading5" role="alert">
              {resultsPagingText}
            </Heading>
            <Box display={["none", "none", "block"]}>
              <Menu
                bg="ui.white"
                labelText={`Sort by: ${currentSortLabel}`}
                listItemsData={sortOptions.map((opt) => ({
                  type: "action",
                  id: opt.id,
                  label: opt.label,
                  onClick: () => onSortMenuClick(opt.id),
                }))}
                selectedItem={currentSortId}
              />
            </Box>
          </Flex>
          <ResultsList works={works} />
        </>
      )}
      <Pagination
        pageCount={searchPaging.lastPage ? searchPaging.lastPage : 1}
        initialPage={searchPaging.currentPage}
        onPageChange={(e) => onPageChange(e)}
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
    </Box>
  );

  return (
    <Template variant="sidebarLeft">
      <TemplateBreakout>{breakoutElement}</TemplateBreakout>
      <TemplateFull bgColor="ui.bg.default" paddingTop="l">
        <TemplateMain>
          <TemplateSidebar>{sidebarElement}</TemplateSidebar>
          <TemplateContent>{contentElement}</TemplateContent>
        </TemplateMain>
      </TemplateFull>
    </Template>
  );
};

export default KeywordSearch;
