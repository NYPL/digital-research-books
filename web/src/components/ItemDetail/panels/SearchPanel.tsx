import { Box, SearchBar } from "@nypl/design-system-react-components";

const SearchPanel: React.FC = () => (
  <Box>
    {/* TODO: Implement search functionality */}
    <SearchBar
      labelText="Search inside this item"
      textInputProps={{
        isClearable: true,
        labelText: "Search inside this item",
        name: "textInputName",
        placeholder: "Enter keywords",
      }}
      sx={{
        "button[type='submit']": {
          bgColor: "section.research.secondary",
        },
        "button[type='submit']:hover": {
          bgColor: "section.research.primary",
        },
      }}
    />
  </Box>
);

export default SearchPanel;
