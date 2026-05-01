import { Box, Heading, Text } from "@nypl/design-system-react-components";
import SectionContainer from "../../ResearchAssistantLanding/SectionContainer";
import AiWorkflow from "./AIWorkflow";
import IngestionPipeline from "./IngestionPipeline";

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
          size="heading2"
          fontFamily="Domine"
          fontWeight="bold"
          marginBottom="l"
        >
          The technology
        </Heading>
        <Box display="flex" flexDir="column" gap="m">
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            fontWeight="medium"
          >
            The collection is accessed via Enhanced Search, an AI-enabled tool
            that connects users with relevant content through a natural language
            chat interface.
          </Text>
          <Text
            fontSize="desktop.heading.heading5"
            color="ui.gray.dark"
            fontWeight="medium"
          >
            Developed with the library community and backed by our institutional
            values, Enhanced Search serves to democratize access to knowledge.
            It has been designed to help users quickly and deeply engage with
            trusted scholarly sources.
          </Text>
        </Box>
        <Box
          margin="0 auto"
          display="flex"
          justifyContent="center"
          marginY="xxl"
        >
          {/* change these to be stored online and use link */}
          <img
            src="/images/diagram.png"
            alt="Diagram of Enhanced Search flow"
            width="608px"
            height="auto"
          />
        </Box>
        <Box
          display="flex"
          flexDir="column"
          borderTop="1px solid"
          borderColor="section.research.primary-10"
        >
          <Box paddingY="l">
            <Heading
              level="h3"
              marginBottom="s"
              size="heading3"
              fontWeight="400"
            >
              <Box display="flex" fontFamily="Domine">
                <span>Book ingestion pipeline</span>
              </Box>
            </Heading>
            <Text fontSize="lg" color="ui.gray.dark">
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
          <Box paddingY="l">
            <Heading
              level="h3"
              marginBottom="s"
              size="heading3"
              fontWeight="400"
            >
              <Box display="flex" fontFamily="Domine">
                <span>Agentic AI workflow</span>
              </Box>
            </Heading>
            <Text fontSize="lg" color="ui.gray.dark">
              Enhanced Search is enabled by an agentic AI workflow. Comprised of
              a Large Language Model (LLM) and other tools, it helps users find
              relevant through a natural language chat interface.
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
