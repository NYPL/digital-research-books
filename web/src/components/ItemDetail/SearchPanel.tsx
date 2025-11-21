import { Box, SearchBar } from "@nypl/design-system-react-components";

const SearchPanel: React.FC = () => (
    <Box>
        {/* TODO: Implement search functionality */}
        <SearchBar
            labelText="Search inside this item"
            textInputProps={{
                isClearable: true,
                labelText: "Item Search",
                name: "textInputName",
                placeholder: "Enter keywords",
            }}
            sx={{
                "button[type='submit']": {
                    bgColor: "section.research.secondary", // TODO: update hover state colors
                },
            }}
        />
    </Box>
);

export default SearchPanel;
