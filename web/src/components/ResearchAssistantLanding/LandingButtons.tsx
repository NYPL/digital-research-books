import { Button, Flex } from "@nypl/design-system-react-components";
import ArrowIcon from "../ResearchAssistant/icons/ArrowIcon";

interface LandingButtonsProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const LandingButtons: React.FC<LandingButtonsProps> = ({ heroSectionRef }) => {
  return (
    <Flex marginTop="l" width="fit-content" gap="s" marginX="auto">
      <Button
        id="try-it-button"
        variant="primary"
        backgroundColor="section.research.secondary"
        margin="0 auto"
        borderRadius="8px"
        fontWeight="medium"
        onClick={() =>
          heroSectionRef.current?.scrollIntoView({ behavior: "smooth" })
        }
        _hover={{
          backgroundColor: "section.research.primary",
        }}
      >
        Try it out <ArrowIcon direction="up" color="#FFF" />
      </Button>
      <Button
        id="learn-more-button"
        variant="secondary"
        borderColor="section.research.secondary"
        color="section.research.secondary"
        margin="0 auto"
        borderRadius="8px"
        fontWeight="medium"
        _hover={{
          backgroundColor: "section.research.primary-05",
        }}
      >
        Learn more
      </Button>
    </Flex>
  );
};

export default LandingButtons;
