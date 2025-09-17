import React, { useState } from "react";
import {
  Button,
  Checkbox,
  FilterBarInline,
  SelectedItems,
  Toggle,
} from "@nypl/design-system-react-components";
import FilterLanguage from "./FilterLanguage";
import FilterBookFormat from "./FilterBookFormat";
import FilterYears from "./FilterYears";
import { FacetItem } from "~/src/types/DataModel";
import { Filter } from "~/src/types/SearchQuery";
import {
  findFilterForField,
  findFiltersExceptField,
  findFiltersForField,
} from "~/src/util/SearchQueryUtils";
import { errorMessagesText } from "~/src/constants/labels";
import filterFields from "~/src/constants/filters";

/**
 * Shows a form with the Languages, Format and Year filters
 *
 *
 * submitOnChange: Toggles whether to automatically submit state changes
 */

const Filters: React.FC<{
  filters: Filter[];
  showAll: boolean;
  languages: FacetItem[];
  isModal?: boolean;
  changeFilters: (newFilters?: Filter[]) => void;
  changeShowAll: (showAll: boolean) => void;
}> = ({
  filters: propFilters,
  showAll: propShowAll,
  languages,
  isModal,
  changeFilters,
  changeShowAll,
}) => {
    const [dateRangeError, setDateRangeError] = useState("");
    const [filters, setFilters] = useState(propFilters);
    const [showAll, setShowAll] = useState(propShowAll);

    const onLanguageChange = (languages: SelectedItems) => {
      const multiSelectId = isModal
        ? "languages-multiselect-modal"
        : "languages-multiselect";
      const selectedLanguages = languages[multiSelectId]?.items || [];
      const languageFilters = selectedLanguages
        ? selectedLanguages.map((language) => ({
          field: filterFields.language,
          value: language,
        }))
        : [];
      const newFilters = [
        ...findFiltersExceptField(filters, filterFields.language),
        ...languageFilters,
      ];
      setFilters(newFilters);
      changeFilters(newFilters);
    };

    const onBookFormatChange = (formats) => {
      const multiSelectId = isModal
        ? "format-multiselect-modal"
        : "format-multiselect";
      const selectedFormats = formats[multiSelectId]?.items || [];
      const formatFilters = selectedFormats
        ? selectedFormats.map((format) => ({
          field: filterFields.format,
          value: format,
        }))
        : [];
      const newFilters = [
        ...findFiltersExceptField(filters, filterFields.format),
        ...formatFilters,
      ];
      setFilters(newFilters);
      changeFilters(newFilters);
    };

    const onDateChange = (
      e: React.ChangeEvent<HTMLInputElement>,
      isStart: boolean
    ) => {
      const field = isStart ? filterFields.startYear : filterFields.endYear;
      const newFilters = [
        ...findFiltersExceptField(filters, field),
        ...[{ field: field, value: e.currentTarget.value }],
      ];
      setFilters(newFilters);
    };

    const removeEmptyFilters = (filters: Filter[]) => {
      return filters.filter((filter) => {
        return !!filter.value;
      });
    };

    const submitDateForm = () => {
      const startYear = findFilterForField(filters, filterFields.startYear);
      const endYear = findFilterForField(filters, filterFields.endYear);
      if (!startYear && !endYear) {
        setDateRangeError(errorMessagesText.emptySearch);
      }

      if (startYear && endYear && endYear.value < startYear.value) {
        setDateRangeError(errorMessagesText.invalidDate);
      } else {
        changeFilters(removeEmptyFilters(filters));
      }
    };

    const clearFilters = () => {
      setFilters([]);
      changeFilters([]);
      setDateRangeError("");
    };

    /**
     * Toggles the "Show All" filter.
     * If we should show only what's available online,
     *  showAll=false and this checkbox is checked
     */

    const toggleShowAll = (e) => {
      setShowAll(!e.target.checked);
      changeShowAll(!e.target.checked);
    };

    const toggleGovDoc = (e: React.ChangeEvent<HTMLInputElement>) => {
      const newFilters = [
        ...findFiltersExceptField(filters, filterFields.govDoc),
      ];
      if (e.target.checked) {
        newFilters.push({ field: filterFields.govDoc, value: "onlyGovDoc" });
      }
      setFilters(newFilters);
      changeFilters(newFilters);
    };

    const yearStart = findFilterForField(filters, filterFields.startYear);
    const yearEnd = findFilterForField(filters, filterFields.endYear);
    const govDocFilter = findFilterForField(filters, filterFields.govDoc);

    const renderFilterComponets = () => (
      <>
        <Toggle
          labelText="Available Online"
          onChange={(e) => {
            toggleShowAll(e);
          }}
          isChecked={!showAll}
          size="small"
          id={
            isModal ? "available-online-toggle-modal" : "available-online-toggle"
          }
        />
        <Toggle
          labelText="Limit to US government documents"
          onChange={(e) => {
            toggleGovDoc(e);
          }}
          isChecked={!!govDocFilter && govDocFilter.value === "onlyGovDoc"}
          size="small"
          id="gov-doc-toggle"
        />
        <FilterBookFormat
          selectedFormats={findFiltersForField(filters, filterFields.format)}
          isModal={isModal}
          onFormatChange={(format) => onBookFormatChange(format)}
        />
        <FilterLanguage
          languages={languages}
          showCount={true}
          selectedLanguages={findFiltersForField(filters, filterFields.language)}
          isModal={isModal}
          onLanguageChange={(languages) => {
            onLanguageChange(languages);
          }}
        />
        <FilterYears
          startFilter={yearStart}
          endFilter={yearEnd}
          isModal={isModal}
          onDateChange={(
            e: React.ChangeEvent<HTMLInputElement>,
            isStart: boolean
          ) => {
            onDateChange(e, isStart);
          }}
          dateRangeError={dateRangeError}
          onSubmit={() => submitDateForm()}
        />
      </>
    );

    return (
      <FilterBarInline
        heading="Filter results"
        layout="column"
        onClear={clearFilters}
        renderChildren={renderFilterComponets}
      />
    );
  };

export default Filters;
