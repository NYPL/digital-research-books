import React from "react";
import KeywordSearchForm from "../KeywordSearchForm/KeywordSearchForm";
import router from "next/router";
import { SearchQuery } from "~/src/types/SearchQuery";
import { toLocationQuery, toApiQuery } from "~/src/util/apiConversion";
import EmptySearchPrompt from "../EmptySearchPrompt/EmptySearchPrompt";

const KeywordSearchLanding: React.FC = () => {
    const handleSearch = (query: SearchQuery) => {
        router.push({
            pathname: "/keyword-search",
            query: toLocationQuery(toApiQuery(query)),
        });
    };

    return (
        <>
            <KeywordSearchForm onSearch={handleSearch} paddingBottom="l" />
            <EmptySearchPrompt />
        </>
    );
};

export default KeywordSearchLanding;
