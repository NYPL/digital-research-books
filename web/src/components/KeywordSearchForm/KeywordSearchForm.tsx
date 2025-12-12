import React, { useState } from "react";
import {
    Flex,
    Icon,
    SearchBar,
    Text,
} from "@nypl/design-system-react-components";
import { SEARCH_BAR_TEST_ID } from "~/src/constants/testIds";
import { SearchQuery, SearchQueryDefaults } from "~/src/types/SearchQuery";
import {
    errorMessagesText,
    inputTerms,
    SEARCH_FORM_OPTIONS,
} from "~/src/constants/labels";
import { Query, SearchField } from "~/src/types/DataModel";
import ResultsBanner from "../ResearchAssistant/ResultsBanner";

interface KeywordSearchFormProps {
    searchQuery?: SearchQuery;
    onSearch?: (query: SearchQuery) => void;
    [x: string]: any; // for ds styling props
}

const KeywordSearchForm: React.FC<KeywordSearchFormProps> = ({
    searchQuery,
    onSearch,
    ...rest
}) => {
    const initialDefaultQuery: Query = { query: "", field: SearchField.Keyword };

    // The display query is the query that's auto-populated in the searchbar.
    // If a displayQuery is passed,
    // If there is more than one query, the displayQuery is not prepopulated.
    // If the query is a viaf query, the displayQuery is the value that the user clicked
    const getDisplayQuery = (query: Query) => {
        if (searchQuery.display) {
            return searchQuery.display;
        }
        return query;
    };

    const [shownQuery, setShownQuery] = useState(
        searchQuery && searchQuery.queries && searchQuery.queries.length === 1
            ? getDisplayQuery(searchQuery.queries[0])
            : initialDefaultQuery
    );
    const [isFormError, setFormError] = useState(false);

    const submitSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (!shownQuery.query) {
            setFormError(true);
            return;
        }

        const newSearchQuery = { ...SearchQueryDefaults, queries: [shownQuery] };

        if (onSearch) {
            onSearch(newSearchQuery);
        }
    };

    const onQueryChange = (
        e:
            | React.ChangeEvent<HTMLInputElement>
            | React.ChangeEvent<HTMLTextAreaElement>
    ) => {
        setShownQuery({ query: e.target.value, field: shownQuery.field });
    };

    const onFieldChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
        setShownQuery({
            field: e.target.value as SearchField,
            query: shownQuery.query,
        });
    };

    return (
        <Flex
            flexDir="column"
            gap="s"
            margin="0 auto"
            maxWidth="1280px"
            paddingX="s"
            paddingTop="l"
            {...rest}
        >
            <Flex alignItems="center" gap="xxs">
                <Icon name="actionInfo" size="medium" />
                <Text size="body2">
                    <strong>Search tip:</strong>{" "}
                    {SEARCH_FORM_OPTIONS[shownQuery.field].searchTip}
                </Text>
            </Flex>
            <SearchBar
                id="search-bar"
                invalidText={errorMessagesText.emptySearch}
                isInvalid={isFormError}
                onSubmit={(e) => submitSearch(e)}
                selectProps={{
                    labelText: "Select a search category",
                    name: "selectName",
                    optionsData: inputTerms,
                    onChange: (e: React.ChangeEvent<HTMLSelectElement>) =>
                        onFieldChange(e),
                    value: shownQuery.field,
                }}
                textInputProps={{
                    isClearable: true,
                    labelText: "Item Search",
                    name: "textInputName",
                    placeholder: SEARCH_FORM_OPTIONS[shownQuery.field].placeholder,
                    value: shownQuery.query,
                    onChange: (e) => onQueryChange(e),
                }}
                labelText="Search"
                data-testid={SEARCH_BAR_TEST_ID}
                sx={{
                    "button[type='submit']": {
                        bgColor: "section.research.secondary", // TODO: update hover state colors
                    },
                }}
                marginBottom="s"
            />
            <ResultsBanner />
        </Flex>
    );
};

export default KeywordSearchForm;
