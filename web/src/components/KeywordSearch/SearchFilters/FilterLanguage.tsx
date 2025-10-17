import React from "react";
import { SelectedItems } from "@nypl/design-system-react-components";
import FilterMultiSelect from "./FilterMultiSelect";
import { FacetItem } from "~/src/types/DataModel";
import { Filter } from "~/src/types/SearchQuery";

interface LanguageMultiSelectProps {
  languages: FacetItem[];
  showCount: boolean;
  selectedLanguages: Filter[];
  isModal?: boolean;
  onLanguageChange: (selectedItems: SelectedItems) => void;
}

const FilterLanguage: React.FC<LanguageMultiSelectProps> = ({
  languages,
  showCount,
  selectedLanguages,
  isModal,
  onLanguageChange,
}) => {
  const multiSelectId = isModal
    ? "languages-multiselect-modal"
    : "languages-multiselect";

  const items = languages.map((lang) => ({
    id: String(lang.value),
    name: showCount ? `${lang.value} (${lang.count})` : lang.value,
  }));

  return (
    <FilterMultiSelect
      items={items}
      selectedFilters={selectedLanguages}
      multiSelectId={multiSelectId}
      buttonText="Language"
      isBlockElement
      isSearchable
      isDefaultOpen
      onChange={onLanguageChange}
    />
  );
};

export default FilterLanguage;