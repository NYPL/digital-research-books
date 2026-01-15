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
import FilterYears from "~/src/components/FilterYears/FilterYears";
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
  Template,
  TemplateBreakout,
  TemplateContent,
  TemplateMain,
  TextInput,
} from "@nypl/design-system-react-components";
import LanguageAccordion from "../LanguageAccordion/LanguageAccordion";
import FilterBookFormat from "../FilterBookFormat/FilterBookFormat";
import { FacetItem, Query } from "~/src/types/DataModel";
import { toLocationQuery, toApiQuery } from "~/src/util/apiConversion";
import filterFields from "~/src/constants/filters";
import { ApiLanguageResponse } from "~/src/types/LanguagesQuery";
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

  const onLanguageChange = (e, language) => {
    setLanguageFilters([
      ...(e.target.checked
        ? [
          ...languageFilters,
          { field: filterFields.language, value: language },
        ]
        : languageFilters.filter((filter) => {
          return filter.value !== language;
        })),
    ]);
  };

  const onBookFormatChange = (e, format) => {
    const newFilters = [
      ...(e.target.checked
        ? [...formatFilters, { field: filterFields.format, value: format }]
        : formatFilters.filter((filter) => {
          return filter.value !== format;
        })),
    ];

    setFormatFilters(newFilters);
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

  const contentElement = (
    <>
      <Heading level="h1" marginBottom="s">Advanced Search</Heading>
      {emptySearchError && (
        <HelperErrorText marginTop="xxs" text={errorMessagesText.emptySearch} isInvalid />
      )}
      {dateRangeError && (
        <HelperErrorText marginTop="xxs"  text={errorMessagesText.invalidDate} isInvalid />
      )}
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
            <LanguageAccordion
              languages={languages}
              showCount={false}
              selectedLanguages={languageFilters}
              onLanguageChange={(e, language) => onLanguageChange(e, language)}
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
            onFormatChange={(e, format) => {
              onBookFormatChange(e, format);
            }}
          />
        </FormField>
        <FormField>
          <ButtonGroup>
            <Button
              type="submit"
              variant="primary"
              onClick={(e) => {
                submit(e);
              }}
              id="submit-button"
            >
              Search
            </Button>
            <Button
              type="reset"
              variant="secondary"
              onClick={() => clearSearch()}
              id="reset-button"
            >
              Clear
            </Button>
          </ButtonGroup>
        </FormField>
      </Form>
    </>
  );
  return (
    <Template>
      <TemplateBreakout>{breakoutElement}</TemplateBreakout>
      <TemplateMain>
        <TemplateContent>{contentElement}</TemplateContent>
      </TemplateMain>
    </Template>
  );
};

export default AdvancedSearch;
