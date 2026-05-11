import { Button, Flex } from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import ArrowIcon from "../ResearchAssistant/icons/ArrowIcon";

interface LearnMoreButtonsProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const LearnMoreButtons: React.FC<LearnMoreButtonsProps> = ({
  heroSectionRef,
}) => {
  const handleBackToTopClick = () => {
    if (heroSectionRef.current) {
      heroSectionRef.current.scrollIntoView({ behavior: "smooth" });
    } else {
      console.error("Element not found");
    }
  };

  return (
    <Flex marginTop="xxl" width="fit-content" gap="s" marginX="auto">
      <Link
        to="/research-assistant-landing"
        variant="buttonPrimary"
        aria-label="Try out the project"
        id="try-it-button"
        width="auto"
        backgroundColor="section.research.secondary"
        borderRadius="8px"
        fontWeight="medium"
        color="ui.white"
        _hover={{
          backgroundColor: "section.research.primary",
          textDecor: "none",
        }}
      >
        Try Enhanced Search
      </Link>
      <Button
        id="back-to-top-button"
        variant="secondary"
        backgroundColor="ui.white"
        borderColor="section.research.secondary"
        borderRadius="8px"
        color="section.research.secondary"
        fontWeight="medium"
        onClick={handleBackToTopClick}
        _hover={{
          backgroundColor: "section.research.primary-05",
        }}
      >
        Back to top <ArrowIcon direction="up" color="#006166" />
      </Button>
    </Flex>
  );
};

export default LearnMoreButtons;
