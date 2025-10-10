import {
  MultiSelect,
  SelectedItems,
} from "@nypl/design-system-react-components";
import React, { useEffect, useState } from "react";
import { Filter } from "~/src/types/SearchQuery";

interface FilterMultiSelectProps {
  items: { id: string; name: string }[];
  selectedFilters: Filter[];
  multiSelectId: string;
  buttonText: string;
  isBlockElement?: boolean;
  isSearchable?: boolean;
  isDefaultOpen?: boolean;
  onChange: (selected: SelectedItems) => void;
}

const FilterMultiSelect: React.FC<FilterMultiSelectProps> = ({
  items,
  selectedFilters,
  multiSelectId,
  buttonText,
  isBlockElement = true,
  isSearchable = false,
  isDefaultOpen = false,
  onChange,
}) => {
  const initialSelectedItems = selectedFilters
      ? {
          [multiSelectId]: {
            items: selectedFilters.map((f) => String(f.value)),
          },
        }
      : {};
  const [selectedItems, setSelectedItems] = useState(initialSelectedItems);

  useEffect(() => {
    const initialSelectedItems = selectedFilters
      ? {
          [multiSelectId]: {
            items: selectedFilters.map((f) => String(f.value)),
          },
        }
      : {};
    setSelectedItems(initialSelectedItems)
  }, [selectedFilters, multiSelectId])


  const handleChange = (selectedId: string) => {
    let itemIds = selectedItems[multiSelectId]?.items ?? [];
    itemIds = itemIds.includes(selectedId)
      ? itemIds.filter((id) => id !== selectedId)
      : [...itemIds, selectedId];
    const newSelectedItems = {
      ...selectedItems,
      [multiSelectId]: { items: itemIds },
    };
    setSelectedItems(newSelectedItems);
    onChange(newSelectedItems);
  };

  const handleClear = () => {
    setSelectedItems({});
    onChange({});
  };

  return (
    <MultiSelect
      id={multiSelectId}
      buttonText={buttonText}
      items={items}
      selectedItems={selectedItems}
      isBlockElement={isBlockElement}
      isSearchable={isSearchable}
      onChange={(e) => handleChange(e.target.id)}
      onClear={handleClear}
      width="full"
      isDefaultOpen={isDefaultOpen}
      sx={{
        ".chakra-accordion__button": {
          fontWeight: "medium",
        }
      }}
    />
  );
};

export default FilterMultiSelect;