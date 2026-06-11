import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";
import SectionContainer from "../../ResearchAssistantLanding/SectionContainer";
import AiWorkflow from "./AIWorkflow";
import IngestionPipeline from "./IngestionPipeline";
import TechnologyPipeline from "./TechnologyPipeline";

const TechnologySection: React.FC = () => {
  return (
    <SectionContainer
      backgroundColor="#FAFDFD"
      borderTop="1px solid"
      borderColor="section.research.primary-10"
      color="ui.typography.body"
      textAlign="left"
      paddingX="s"
      tabIndex={-1}
    >
      <Box maxWidth="55rem" margin="0 auto">
        <Heading
          level="h2"
          fontSize={{ base: "mobile.heading.heading3", md: "heading2" }}
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom={{ base: "s", md: "l" }}
        >
          The technology
        </Heading>
        <Flex flexDir="column" gap="s">
          <Text
            fontSize={{
              base: "mobile.heading.heading5",
              md: "desktop.heading.heading5",
            }}
            color="ui.gray.dark"
            fontWeight="medium"
          >
            The collection is accessed via Enhanced Search, an AI-enabled tool
            that connects users with relevant content through a natural language
            chat interface.
          </Text>
          <Text
            fontSize={{
              base: "mobile.heading.heading5",
              md: "desktop.heading.heading5",
            }}
            color="ui.gray.dark"
            fontWeight="medium"
          >
            Developed with the library community and backed by our institutional
            values, Enhanced Search serves to democratize access to knowledge.
            It has been designed to help users quickly and deeply engage with
            trusted scholarly sources.
          </Text>
        </Flex>
        <Flex
          margin="0 auto"
          justifyContent="center"
          marginTop={{ base: "l", md: "xxl" }}
          marginBottom={{ base: "xl", md: "xxl" }}
        >
          <TechnologyPipeline />
          {/* <Image
            src="https://drb-files-qa.s3.us-east-1.amazonaws.com/misc/technologyDiagram.png"
            alt="Flow diagram showing an Agentic AI system. A question enters the Agentic AI box, which flows through three stages: Question Processed, Information Retrieved, and Response Generated (linked to a Book Ingestion Pipeline), finally exiting as a Response."
            width="608px"
            flexShrink="0"
            backgroundColor="transparent"
          /> */}
        </Flex>
        <Box
          display="flex"
          flexDir="column"
          borderTop="1px solid"
          borderColor="section.research.primary-10"
        >
          <Box paddingY={{ base: "s", md: "l" }}>
            <Heading
              level="h3"
              marginBottom={{ base: "xs", md: "s" }}
              fontSize={{ base: "mobile.heading.heading4", md: "heading3" }}
              fontFamily="Domine"
            >
              Book ingestion pipeline
            </Heading>
            <Text
              fontSize={{ base: "mobile.subtitle.subtitle1", md: "lg" }}
              color="ui.gray.dark"
            >
              Our ingestion pipeline downloads, refines, embeds, and stores data
              from the collection. It prepares the books to be quickly and
              contextually accessed by Enhanced Search.
            </Text>
          </Box>
          <IngestionPipeline />
        </Box>
        <Box
          display="flex"
          flexDir="column"
          borderTop="1px solid"
          borderColor="section.research.primary-10"
        >
          <Box paddingY={{ base: "s", md: "l" }}>
            <Heading
              level="h3"
              marginBottom={{ base: "xs", md: "s" }}
              fontSize={{ base: "mobile.heading.heading4", md: "heading3" }}
              fontFamily="Domine"
            >
              Agentic AI workflow
            </Heading>
            <Text
              fontSize={{ base: "mobile.subtitle.subtitle1", md: "lg" }}
              color="ui.gray.dark"
            >
              Enhanced Search is enabled by an agentic AI workflow. Comprised of
              a Large Language Model (LLM) and other tools, it helps users find
              relevant resources through a natural language chat interface.
            </Text>
          </Box>
          <AiWorkflow />
        </Box>
      </Box>
    </SectionContainer>
  );
};

TechnologySection.displayName = "TechnologySection";

export default TechnologySection;
