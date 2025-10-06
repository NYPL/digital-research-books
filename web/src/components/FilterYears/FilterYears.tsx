import React from "react";
import {
  Button,
  Fieldset,
  FormField,
  FormRow,
  HelperErrorText,
  TextInput,
} from "@nypl/design-system-react-components";

import { Filter } from "~/src/types/SearchQuery";

/**
 * Year Filters
 * Can be passed as a form or as a fieldset
 *
 *
 * @param props
 */
const FilterYears: React.FC<{
  startFilter: Filter;
  endFilter: Filter;
  isModal?: boolean;
  onDateChange: (
    e: React.ChangeEvent<HTMLInputElement>,
    isStart: boolean
  ) => void;
  // The date range error to show.
  // If no error should be shown, this should be an empty string
  dateRangeError?: string;
  onSubmit?: () => void;
}> = (props) => {
  const {
    startFilter,
    endFilter,
    isModal,
    onDateChange,
    dateRangeError,
    onSubmit,
  } = props;

  const changeDate = (
    e: React.ChangeEvent<HTMLInputElement>,
    isStart: boolean
  ) => {
    onDateChange(e, isStart);
  };

  // FilterYears can either be a form on its own (widescreen sidebar) or it can be a part of a larger form (advanced search)
  // If it is a part of a larger form, error checking should happen on form submit, and the error should appear at the top of the form rather than
  // next to the FilterYears inputs
  if (dateRangeError && !onSubmit) {
    console.warn(
      "Found a dateRangeError but no onSubmit.  Errors should be shown at the top of the form when this is used as a fieldset."
    );
  }

  return (
    <Fieldset legendText="Publication Year" id="date-range">
      <FormRow>
        <FormField>
          <TextInput
            labelText="From"
            type="number"
            value={startFilter ? startFilter.value.toString() : ""}
            helperText="EX. 1901"
            id={isModal ? "date-filter-from-modal" : "date-filter-from"}
            name="Date From"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              changeDate(e, true);
            }}
          />
        </FormField>
        <FormField>
          <TextInput
            labelText="To"
            type="number"
            value={endFilter ? endFilter.value.toString() : ""}
            helperText="EX. 2000"
            id={isModal ? "date-filter-to-modal" : "date-filter-to"}
            name="Date To"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              changeDate(e, false);
            }}
          />
        </FormField>
      </FormRow>
      {dateRangeError && (
        <HelperErrorText marginTop="xxs" isInvalid={true} text={dateRangeError} />
      )}
      {onSubmit && (
        <Button
          id={isModal ? "year-filter-button-modal" : "year-filter-button"}
          onClick={() => onSubmit()}
        >
          Apply year
        </Button>
      )}
    </Fieldset>
  );
};

export default FilterYears;
