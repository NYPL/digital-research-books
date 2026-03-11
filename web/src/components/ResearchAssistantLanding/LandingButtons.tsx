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
    textInputRef.current?.focus();
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
        backgroundColor="ui.white"
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
