import { Box, Heading, Text } from "@nypl/design-system-react-components";
import SectionContainer from "../ResearchAssistantLanding/SectionContainer";

const TechnologySection: React.FC = () => {
  return (
    <SectionContainer
      backgroundColor="#FAFDFD"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      color="ui.typography.body"
      textAlign="left"
      tabIndex={-1}
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Heading
          level="h2"
          size="heading3"
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="xs"
        >
          The technology
        </Heading>
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
          marginBottom="s"
        >
          A collaborative tool designed to enhance and support your research
        </Text>
      </Box>
    </SectionContainer>
  );
};

TechnologySection.displayName = "TechnologySection";

export default TechnologySection;
