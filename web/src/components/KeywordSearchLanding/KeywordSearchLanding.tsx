import { Flex } from "@nypl/design-system-react-components";
import router from "next/router";
import React from "react";
import { SearchQuery } from "~/src/types/SearchQuery";
import { toApiQuery, toLocationQuery } from "~/src/util/apiConversion";
import EmptySearchPrompt from "../EmptySearchPrompt/EmptySearchPrompt";
import KeywordSearchForm from "../KeywordSearchForm/KeywordSearchForm";

const KeywordSearchLanding: React.FC = () => {
  const handleSearch = (query: SearchQuery) => {
    router.push({
      pathname: "/keyword-search",
      query: toLocationQuery(toApiQuery(query)),
    });
  };

  return (
    <Flex flexDir="column" height="100%">
      <KeywordSearchForm onSearch={handleSearch} paddingBottom="l" />
      <EmptySearchPrompt
        flexGrow="1"
        borderTop="1px solid"
        borderColor="ui.border.default"
      />
    </Flex>
  );
};

export default KeywordSearchLanding;
