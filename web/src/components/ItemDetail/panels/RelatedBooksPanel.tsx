import { Flex } from "@nypl/design-system-react-components";
import BookCard from "./BookCard";

// TODO: Replace with real data when implementing Related Books functionality
const RelatedBooksPanel: React.FC = () => (
  <Flex gap="xs" flexDir="column">
    <BookCard />
    <BookCard />
  </Flex>
);

export default RelatedBooksPanel;
