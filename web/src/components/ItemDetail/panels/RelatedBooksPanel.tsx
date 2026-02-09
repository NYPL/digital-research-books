import { Flex } from "@nypl/design-system-react-components";
import AiGeneratedText from "../../AiGeneratedText/AiGeneratedText";
import FeedbackButtons from "../../ResearchAssistant/FeedbackButtons";
import BookCard from "./BookCard";

// TODO: Replace with real data when implementing Related Books functionality
const RelatedBooksPanel: React.FC = () => (
  <Flex gap="xs" flexDir="column">
    <BookCard />
    <BookCard />
    <Flex alignItems="center" justifyContent="space-between">
      <AiGeneratedText />
      <FeedbackButtons />
    </Flex>
  </Flex>
);

export default RelatedBooksPanel;
