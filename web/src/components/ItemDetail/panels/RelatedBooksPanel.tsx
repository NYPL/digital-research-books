import { Box, Flex } from "@nypl/design-system-react-components";
import { trackEvent } from "~/src/lib/gtag/Analytics";
import AiGeneratedText from "../../AiGeneratedText/AiGeneratedText";
import FeedbackButtons from "../../ResearchAssistant/FeedbackButtons";
import BookCard from "./BookCard";

// TODO: Replace with real data when implementing Related Books functionality
const RelatedBooksPanel: React.FC = () => {
  const handleRelatedBookClick = () => {
    // GTM Tagging: related_content_click
    trackEvent({
      event: "related_content_click",
      interaction: "Click",
      // element_id: "", // add when implemented
    });
  };

  return (
    <Flex gap="xs" flexDir="column">
      {/* Box element is temporary to allow GA tagging functionality */}
      <Box onClick={handleRelatedBookClick}>
        <BookCard />
      </Box>
      {/* Box element is temporary to allow GA tagging functionality */}
      <Box onClick={handleRelatedBookClick}>
        <BookCard />
      </Box>
      <Flex
        alignItems="center"
        justifyContent="space-between"
        height="1.125rem"
      >
        <AiGeneratedText />
        <FeedbackButtons label="related books feedback" />
      </Flex>
    </Flex>
  );
};

export default RelatedBooksPanel;
