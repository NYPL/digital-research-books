import { Flex } from "@nypl/design-system-react-components";
import AiGeneratedText from "../../AiGeneratedText/AiGeneratedText";
import FeedbackButtons from "../../ResearchAssistant/FeedbackButtons";
import BookCard from "./BookCard";

// TODO: Replace with real data when implementing Related Books functionality
const RelatedBooksPanel: React.FC = () => (
  <Flex gap="xs" flexDir="column">
    <BookCard />
    <BookCard />
    <Flex alignItems="center" justifyContent="space-between" height="1.125rem">
      <AiGeneratedText />
      <FeedbackButtons label="related books feedback" />
    </Flex>
  </Flex>
);

export default RelatedBooksPanel;
