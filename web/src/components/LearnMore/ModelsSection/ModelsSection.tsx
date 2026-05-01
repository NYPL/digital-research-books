import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";
import Link from "../../Link/Link";
import ArrowIcon from "../../ResearchAssistant/icons/ArrowIcon";
import LandingCard from "../../ResearchAssistantLanding/LandingCard";
import SectionContainer from "../../ResearchAssistantLanding/SectionContainer";
import LandingButtons from "../LandingButtons";

// remove this later !

interface HelpSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const ModelsSection: React.FC<HelpSectionProps> = ({ heroSectionRef }) => {
  return (
    <SectionContainer
      backgroundImage={`
        radial-gradient(circle, rgba(0, 131, 138, 0.025) 2px, transparent 2px)`}
      backgroundSize="16px 16px"
      backgroundPosition="center"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      color="ui.typography.body"
      display="flex"
      flexDir="column"
      alignItems="center"
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Heading
          level="h2"
          size="heading2"
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="xs"
        >
          Our AI models
        </Heading>
        <Text
          fontSize="desktop.heading.heading5"
          color="ui.gray.dark"
          fontWeight="semibold"
        >
          Chosen to balance speed, cost, performance, and environmental impact.
        </Text>
        <Flex gap="l" marginTop="xxl" marginBottom="xxl">
          <LandingCard
            gap="l"
            icon={
              <img
                src="/images/GeminiFlashIcon.png"
                alt="Gemini Flash icon"
                width="48px"
              />
            }
            heading={
              <Heading level="h3" size="heading4">
                Google Gemini Flash
              </Heading>
            }
            body={
              <Flex gap="l" flexDir="column">
                <Text fontSize="desktop.subtitle.subtitle1">
                  Large Language Model
                </Text>
                <Link
                  to="https://deepmind.google/models/gemini/flash/"
                  hasVisitedState={false}
                  isUnderlined={false}
                  color="section.research.secondary"
                  fontWeight="bold"
                  display="flex"
                  alignItems="center"
                  gap="xxs"
                  _hover={{
                    color: "section.research.primary",
                    path: { stroke: "#00838A" },
                  }}
                >
                  Learn more about Flash
                  <ArrowIcon direction="right" color="#006166" />
                </Link>
              </Flex>
            }
          />
          <LandingCard
            gap="l"
            icon={
              <img
                src="/images/GeminiFlashIcon.png"
                alt="Gemini Embeddings icon"
                width="48px"
              />
            }
            heading={
              <Heading level="h3" size="heading4">
                Google Gemini Embeddings
              </Heading>
            }
            body={
              <Flex gap="l" flexDir="column">
                <Text fontSize="desktop.subtitle.subtitle1">
                  Embeddings Model
                </Text>
                <Link
                  to="https://ai.google.dev/gemini-api/docs/embeddings"
                  aria-label="Learn more about the project"
                  hasVisitedState={false}
                  isUnderlined={false}
                  color="section.research.secondary"
                  fontWeight="bold"
                  display="flex"
                  alignItems="center"
                  gap="xxs"
                  _hover={{
                    color: "section.research.primary",
                    path: { stroke: "#00838A" },
                  }}
                >
                  <Text>Learn more about Embeddings</Text>
                  <ArrowIcon direction="right" color="#006166" />
                </Link>
              </Flex>
            }
          />
        </Flex>
        <LandingButtons heroSectionRef={heroSectionRef} />
      </Box>
    </SectionContainer>
  );
};

ModelsSection.displayName = "ModelsSection";

export default ModelsSection;
