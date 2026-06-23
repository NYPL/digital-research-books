import {
  Box,
  Flex,
  Heading,
  Image,
  Text,
} from "@nypl/design-system-react-components";
import Link from "../../Link/Link";
import ArrowIcon from "../../ResearchAssistant/icons/ArrowIcon";
import LandingCard from "../../ResearchAssistantLanding/LandingCard";
import SectionContainer from "../../ResearchAssistantLanding/SectionContainer";
import LearnMoreButtons from "../LearnMoreButtons";

interface ModelsSectionProps {
  heroSectionRef: React.RefObject<HTMLDivElement>;
}

const ModelsSection: React.FC<ModelsSectionProps> = ({ heroSectionRef }) => {
  return (
    <SectionContainer
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      color="ui.typography.body"
      display="flex"
      flexDir="column"
      alignItems="center"
      paddingX={{ base: "0px", md: "16px" }}
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Heading
          level="h2"
          fontSize={{
            base: "mobile.heading.heading3",
            md: "desktop.heading.heading2",
          }}
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="xs"
          paddingX={{ base: "s", md: "none" }}
        >
          Our AI models
        </Heading>
        <Text
          fontSize={{
            base: "mobile.heading.heading5",
            md: "desktop.heading.heading5",
          }}
          color="ui.gray.dark"
          fontWeight="semibold"
          paddingX={{ base: "s", md: "none" }}
        >
          Selected to balance speed, cost, performance, and environmental
          impact.
        </Text>
        <Flex
          gap={{ base: "s", md: "l" }}
          marginTop={{ base: "l", md: "xxl" }}
          marginBottom={{ base: "none", md: "xxl" }}
          flexDir={{ base: "column", md: "row" }}
        >
          <LandingCard
            gap="s"
            icon={
              <Image
                src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/GeminiFlashIcon.png"
                alt=""
                width="48px"
                flexShrink="0"
                backgroundColor="transparent"
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
                  target="_blank"
                  rel="noopener noreferrer"
                  hasVisitedState={false}
                  isUnderlined={false}
                  color="section.research.secondary"
                  fontWeight="bold"
                  display="flex"
                  alignItems="center"
                  gap="xxs"
                  paddingY={{ base: "xs", md: "none" }}
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
            gap="s"
            icon={
              <Image
                src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/GeminiEmbeddingsIcon.png"
                alt=""
                width="48px"
                flexShrink="0"
                backgroundColor="transparent"
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
                  target="_blank"
                  rel="noopener noreferrer"
                  hasVisitedState={false}
                  isUnderlined={false}
                  color="section.research.secondary"
                  fontWeight="bold"
                  display="flex"
                  alignItems="center"
                  gap="xxs"
                  paddingY={{ base: "xs", md: "none" }}
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
        <LearnMoreButtons heroSectionRef={heroSectionRef} />
      </Box>
    </SectionContainer>
  );
};

ModelsSection.displayName = "ModelsSection";

export default ModelsSection;
