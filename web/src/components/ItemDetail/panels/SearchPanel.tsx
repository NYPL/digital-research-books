import { Box, SearchBar } from "@nypl/design-system-react-components";
import { trackEvent } from "~/src/lib/gtag/Analytics";

const SearchPanel: React.FC = () => {
  const handleSearchSubmit = () => {
    // GTM Tagging: ereader_search_submit
    trackEvent({
      event: "ereader_search_submit",
      interaction: "User Input",
      location: "Item Page",
    });
  };

  return (
    <Box>
      {/* TODO: Implement search functionality */}
      <SearchBar
        onSubmit={handleSearchSubmit}
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
};

export default SearchPanel;
