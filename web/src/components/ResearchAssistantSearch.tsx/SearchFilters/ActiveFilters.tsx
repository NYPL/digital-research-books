import { Flex, TagSet, Text } from "@nypl/design-system-react-components";

const ActiveFilters = ({ onClick, tagSetData }) => {
    return (
        <Flex gap="s" alignItems="center">
            <Text isBold size="body2">Active filters</Text>
            <TagSet
                id="applied-filters-tagset"
                isDismissible
                variant="filter"
                onClick={onClick}
                tagSetData={tagSetData}
            />
        </Flex>
    );
};

export default ActiveFilters;
