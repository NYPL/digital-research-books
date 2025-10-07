import filterFields from "../constants/filters";
import { FacetItem } from "../types/DataModel";
import { ApiSearchResult, SearchQuery } from "../types/SearchQuery";
import { findFiltersForField } from "./SearchQueryUtils";

export const getAvailableLanguages = (
    searchResults: ApiSearchResult,
    searchQuery: SearchQuery
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
