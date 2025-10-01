import React from "react";
import { Box } from "@nypl/design-system-react-components";
import SearchForm from "../SearchForm/SearchForm";
import { SearchQuery } from "~/src/types/SearchQuery";
import DrbHero from "../DrbHero/DrbHero";
/**
 * Search Header Component
 */

const SearchHeader: React.FC<{
  searchQuery?: SearchQuery;
}> = ({ searchQuery }) => {
  return (
    <>
      <DrbHero />
      <Box bg="ui.gray.x-light-cool">
        <Box m="0 auto" maxW="1280px" p="s">
          <Box>
            <SearchForm searchQuery={searchQuery} />
          </Box>
        </Box>
      </Box>
    </>
  );
};

export default SearchHeader;
