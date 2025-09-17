import React from "react";
import { SelectedItems } from "@nypl/design-system-react-components";
import FilterMultiSelect from "./FilterMultiSelect";
import { Filter } from "~/src/types/SearchQuery";
import { FormatTypes } from "~/src/constants/labels";

interface FilterBookFormatProps {
  selectedFormats: Filter[];
  isModal?: boolean;
  onFormatChange: (selectedFormats: SelectedItems) => void;
}

const FilterBookFormat: React.FC<FilterBookFormatProps> = ({
  selectedFormats,
  isModal,
  onFormatChange,
}) => {
  const multiSelectId = isModal
    ? "format-multiselect-modal"
    : "format-multiselect";

  const items = FormatTypes.map((formatType: any) => ({
    id: String(formatType.value),
    name: formatType.label,
  }));

  return (
    <FilterMultiSelect
      items={items}
      selectedFilters={selectedFormats}
      multiSelectId={multiSelectId}
      buttonText="Format"
      isBlockElement
      isSearchable={false}
      isDefaultOpen
      onChange={onFormatChange}
    />
  );
};

export default FilterBookFormat;