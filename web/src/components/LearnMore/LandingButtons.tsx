import { Button, Flex, Link } from "@nypl/design-system-react-components";
import ArrowIcon from "../ResearchAssistant/icons/ArrowIcon";

interface LandingButtonsProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const LandingButtons: React.FC<LandingButtonsProps> = ({ heroSectionRef }) => {
  const handleTryItClick = () => {
    if (heroSectionRef.current) {
      heroSectionRef.current.scrollIntoView({ behavior: "smooth" });
    } else {
      console.error("Element not found");
    }
  };

  return (
    <Flex marginTop="xxl" width="fit-content" gap="s" marginX="auto">
      <Link href="/research-assistant-landing">
        <Button
          id="try-it-button"
          variant="primary"
          backgroundColor="section.research.secondary"
          margin="0 auto"
          borderRadius="8px"
          fontWeight="medium"
          _hover={{
            backgroundColor: "section.research.primary",
          }}
        >
          Try Enhanced Search
        </Button>
      </Link>
      <Button
        id="learn-more-button"
        variant="secondary"
        aria-label="Learn more about the project"
        backgroundColor="ui.white"
        borderColor="section.research.secondary"
        borderRadius="8px"
        color="section.research.secondary"
        fontWeight="medium"
        onClick={handleTryItClick}
        margin="0 auto"
        _hover={{
          backgroundColor: "section.research.primary-05",
        }}
      >
        Back to top <ArrowIcon direction="up" color="#006166" />
      </Button>
    </Flex>
  );
};

export default LandingButtons;
