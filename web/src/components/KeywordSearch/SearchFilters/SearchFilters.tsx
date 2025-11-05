import React, { useEffect, useState } from "react";
import {
  FilterBarInline,
  SelectedItems,
  Toggle,
} from "@nypl/design-system-react-components";
import FilterLanguage from "./FilterLanguage";
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

const Filters: React.FC<{
  filters: Filter[];
  languages: FacetItem[];
  isModal?: boolean;
  changeFilters: (newFilters?: Filter[]) => void;
}> = ({
  filters: propFilters,
  languages,
  isModal,
  changeFilters,
}) => {
    const [dateRangeError, setDateRangeError] = useState("");
    const [filters, setFilters] = useState(propFilters);

    useEffect(() => {
      setFilters(propFilters);
    }, [propFilters]);

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

      if (
        startYear &&
        endYear &&
        startYear.value !== "" &&
        endYear.value !== "" &&
        endYear.value < startYear.value
      ) {
        setDateRangeError(errorMessagesText.invalidDate);
      } else {
        changeFilters(removeEmptyFilters(filters));
      }
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

    const renderFilterComponents = () => (
      <>
        <Toggle
          labelText="Limit to US government documents"
          onChange={(e) => {
            toggleGovDoc(e);
          }}
          isChecked={!!govDocFilter && govDocFilter.value === "onlyGovDoc"}
          size="small"
          id="gov-doc-toggle"
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
        renderChildren={renderFilterComponents}
      />
    );
  };

export default Filters;
