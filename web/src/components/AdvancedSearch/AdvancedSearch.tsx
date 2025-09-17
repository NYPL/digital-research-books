import React, { useEffect, useState } from "react";
import { useRouter } from "next/router";

import {
  findFilterForField,
  findFiltersForField,
  findQueryForField,
} from "~/src/util/SearchQueryUtils";
import {
  errorMessagesText,
  breadcrumbTitles,
  inputTermRows,
} from "~/src/constants/labels";
import FilterYears from "~/src/components/SearchFilters/FilterYears";
import {
  Filter,
  SearchQuery,
  SearchQueryDefaults,
} from "~/src/types/SearchQuery";

import {
  Button,
  ButtonGroup,
  Checkbox,
  Form,
  FormField,
  FormRow,
  Heading,
  HelperErrorText,
  SelectedItems,
  TemplateAppContainer,
  TextInput,
} from "@nypl/design-system-react-components";
import FilterLanguage from "../SearchFilters/FilterLanguage";
import FilterBookFormat from "../SearchFilters/FilterBookFormat";
import { FacetItem, Query } from "~/src/types/DataModel";
import { toLocationQuery, toApiQuery } from "~/src/util/apiConversion";
import filterFields from "~/src/constants/filters";
import { ApiLanguageResponse } from "~/src/types/LanguagesQuery";
import { trackCtaClick } from "~/src/lib/adobe/Analytics";
import DrbBreakout from "../DrbBreakout/DrbBreakout";

const AdvancedSearch: React.FC<{
  languages: ApiLanguageResponse;
}> = ({ languages: previousLanguages }) => {
  const router = useRouter();

  const [searchQuery] = useState<SearchQuery>({
    ...SearchQueryDefaults,
  });

  const [emptySearchError, setEmptySearchError] = useState(false);
  const [dateRangeError, setDateRangeError] = useState("");

  const [languages, setLanguages] = useState<FacetItem[]>([]);
  const [languageFilters, setLanguageFilters] = useState(
    findFiltersForField(searchQuery.filters, filterFields.language)
  );
  const [queries, setQueries] = useState<Query[]>([]);
  const [formatFilters, setFormatFilters] = useState<Filter[]>(
    findFiltersForField(searchQuery.filters, filterFields.format)
  );
  const [startFilter, setStartFilter] = useState<Filter>(
    findFilterForField(searchQuery.filters, filterFields.startYear)
  );
  const [endFilter, setEndFilter] = useState<Filter>(
    findFilterForField(searchQuery.filters, filterFields.endYear)
  );
  const [govDocFilter, setGovDocFilter] = useState<Filter>(
    findFilterForField(searchQuery.filters, filterFields.govDoc)
  );

  useEffect(() => {
    setLanguages(
      previousLanguages
        ? previousLanguages.data.map((language) => {
          return {
            value: language.language,
            count: language.count,
          };
        })
        : []
    );
  }, [previousLanguages]);

  const submit = (e) => {
    e.preventDefault();
    if (!queries || queries.length < 1) {
      setEmptySearchError(true);
      return;
    } else {
      setEmptySearchError(false);
    }
    const startYear = startFilter;
    const endYear = endFilter;
    if (startYear && endYear && endYear.value < startYear.value) {
      setDateRangeError(errorMessagesText.invalidDate);
      return;
    } else {
      setDateRangeError("");
    }

    const filters = [...languageFilters];
    if (startFilter) filters.push(startFilter);
    if (endFilter) filters.push(endFilter);
    filters.push(...formatFilters);
    if (!!govDocFilter && govDocFilter.value === "onlyGovDoc")
      filters.push(govDocFilter);

    const newSearchQuery = {
      ...searchQuery,
      filters: filters,
      queries: queries,
    };
    trackCtaClick({
      cta_section: "Advanced Search",
      cta_text: "Search",
      destination_url: `/search`,
    });
    router.push({
      pathname: "/search",
      query: toLocationQuery(toApiQuery(newSearchQuery)),
    });
  };

  const clearSearch = () => {
    setQueries([]);
    setLanguageFilters([]);
    setStartFilter(undefined);
    setEndFilter(undefined);
    setFormatFilters([]);
    setGovDocFilter(undefined);
  };

  const onQueryChange = (e, queryKey) => {
    const newQuery = {
      field: queryKey,
      query: e.target.value,
    };

    const allQueries = queries.filter((query) => {
      return query.field !== queryKey;
    });
    // If the new query is not empty, add it
    if (newQuery.query.length > 0) {
      allQueries.push(newQuery);
    }
    setQueries([...allQueries]);
  };

  const onLanguageChange = (languages: SelectedItems) => {
    const multiSelectId = "languages-multiselect";
    const selectedLanguages = languages[multiSelectId]?.items || [];
    const languageFilters = selectedLanguages
      ? selectedLanguages.map((language) => ({
        field: filterFields.language,
        value: language,
      }))
      : [];
    setLanguageFilters(languageFilters);
  };

  const onBookFormatChange = (formats) => {
    const multiSelectId = "format-multiselect";
    const selectedFormats = formats[multiSelectId]?.items || [];
    const formatFilters = selectedFormats
      ? selectedFormats.map((format) => ({
        field: filterFields.format,
        value: format,
      }))
      : [];
    setFormatFilters(formatFilters);
  };

  const onDateChange = (
    e: React.ChangeEvent<HTMLInputElement>,
    isStart: boolean
  ) => {
    const field = isStart ? filterFields.startYear : filterFields.endYear;
    const newFilter = {
      field: field,
      value: e.currentTarget.value,
    };
    if (isStart) setStartFilter(newFilter);
    else setEndFilter(newFilter);
  };

  const onGovDocChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.checked) {
      setGovDocFilter({ field: filterFields.govDoc, value: "onlyGovDoc" });
    } else {
      setGovDocFilter({ field: filterFields.govDoc, value: "all" });
    }
  };

  const breakoutElement = (
    <DrbBreakout
      breadcrumbsData={[
        { url: "/advanced-search", text: breadcrumbTitles.advancedSearch },
      ]}
    />
  );

  const contentTopElement = (
    <>
      <Heading level="h1">Advanced Search</Heading>
      {emptySearchError && (
        <HelperErrorText text={errorMessagesText.emptySearch} isInvalid />
      )}
      {dateRangeError && (
        <HelperErrorText text={errorMessagesText.invalidDate} isInvalid />
      )}
    </>
  );

  const contentPrimaryElement = (
    <Form action="/search" method="get" id="search-form" paddingBottom="l">
      {/* Search Terms */}
      {inputTermRows.map(
        (inputTerms: { key: string; label: string }[], i: number) => {
          return (
            <FormRow key={`input-row-${inputTerms[i].key}`}>
              {inputTerms.map((field: { key: string; label: string }) => {
                return (
                  <FormField key={`input-field-${field.key}`}>
                    <TextInput
                      id={`search-${field.label}`}
                      labelText={field.label}
                      value={
                        findQueryForField(queries, field.key)
                          ? findQueryForField(queries, field.key).query
                          : ""
                      }
                      onChange={(e) => onQueryChange(e, field.key)}
                      showLabel
                      type="text"
                    />
                  </FormField>
                );
              })}
            </FormRow>
          );
        }
      )}
      <FormField>
        <Checkbox
          id="gov-doc-checkbox"
          labelText="Show only US government documents"
          onChange={(e) => {
            onGovDocChange(e);
          }}
          isChecked={!!govDocFilter && govDocFilter.value === "onlyGovDoc"}
        />
      </FormField>
      <FormField>
        {languages && languages.length > 0 && (
          <FilterLanguage
            languages={languages}
            showCount={false}
            selectedLanguages={languageFilters}
            onLanguageChange={(languages) => onLanguageChange(languages)}
          />
        )}
      </FormField>
      <FormField>
        <FilterYears
          startFilter={startFilter}
          endFilter={endFilter}
          onDateChange={(
            e: React.ChangeEvent<HTMLInputElement>,
            isStart: boolean
          ) => {
            onDateChange(e, isStart);
          }}
        />
      </FormField>
      <FormField>
        <FilterBookFormat
          selectedFormats={formatFilters}
          onFormatChange={(formats) => {
            onBookFormatChange(formats);
          }}
        />
      </FormField>
      <FormField>
        <ButtonGroup>
          <Button
            type="submit"
            buttonType="primary"
            onClick={(e) => {
              submit(e);
            }}
            id="submit-button"
          >
            Search
          </Button>
          <Button
            type="reset"
            buttonType="secondary"
            onClick={() => clearSearch()}
            id="reset-button"
          >
            Clear
          </Button>
        </ButtonGroup>
      </FormField>
    </Form>
  );
  return (
    <TemplateAppContainer
      breakout={breakoutElement}
      contentTop={contentTopElement}
      contentPrimary={contentPrimaryElement}
    />
  );
};

export default AdvancedSearch;
