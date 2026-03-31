import {
  Button,
  Flex,
  TextInputRefType,
} from "@nypl/design-system-react-components";
import ArrowIcon from "../ResearchAssistant/icons/ArrowIcon";

interface LandingButtonsProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
  textInputRef: React.RefObject<TextInputRefType>;
}

const LandingButtons: React.FC<LandingButtonsProps> = ({
  heroSectionRef,
  textInputRef,
}) => {
  const handleTryItClick = () => {
    heroSectionRef.current?.scrollIntoView({ behavior: "smooth" });
    window.addEventListener("scrollend", () => textInputRef.current?.focus(), {
      once: true,
    });
  };

  return (
    <Flex marginTop="xxl" width="fit-content" gap="s" marginX="auto">
      <Button
        id="try-it-button"
        variant="primary"
        backgroundColor="section.research.secondary"
        margin="0 auto"
        borderRadius="8px"
        fontWeight="medium"
        onClick={handleTryItClick}
        _hover={{
          backgroundColor: "section.research.primary",
        }}
      >
        Try it out <ArrowIcon direction="up" color="#FFF" />
      </Button>
      <Button
        id="learn-more-button"
        variant="secondary"
        aria-label="Learn more about the project"
        backgroundColor="ui.white"
        borderColor="section.research.secondary"
        borderRadius="8px"
        color="section.research.secondary"
        fontWeight="medium"
        margin="0 auto"
        _hover={{
          backgroundColor: "section.research.primary-05",
        }}
        isDisabled
      >
        Learn more
      </Button>
    </Flex>
  );
};

export default LandingButtons;
