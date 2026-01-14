import { Box, Text } from "@nypl/design-system-react-components";
import Link from "../../Link/Link";

// TODO: Replace with real data when implementing Related Books functionality
const BookCard: React.FC = () => (
  <Box
    border="1px solid"
    borderColor="ui.border.default"
    padding="s"
    backgroundColor="ui.white"
    borderTop="2px solid"
    borderTopColor="section.research.primary"
    fontSize="desktop.body.body2"
  >
    <Link to="#" isUnderlined={false} fontSize="-1">
      Placeholder Book Title
    </Link>
    <Box minHeight="1.5rem">
      By{" "}
      <Text fontWeight="bold" fontSize="-2" as="span">
        Placeholder Author
      </Text>
    </Box>
  </Box>
);

export default BookCard;
