import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";
import ArrowSVG from "./ArrowSVG";
import ChunkIcon from "./ChunkIcon";
import ClusterIcon from "./ClusterIcon";
import EmbedIcon from "./EmbedIcon";
import SourceIcon from "./SourceIcon";
import StorageIcon from "./StorageIcon";

const IngestionPipeline = () => {
  const pipelineSteps = [
    {
      icon: <SourceIcon />,
      title: "SOURCING",
      description:
        "Digitized books are retrieved from various sources such as NYPL (through Google Books) and Harvard. Incoming data has already undergone raw-text processing and categorization for copyright clearance.",
    },
    {
      icon: <ClusterIcon />,
      title: "CLUSTERING",
      description:
        "Books coming from different sources are clustered, or related to each other, using a hierarchical structure of items, editions, and works. OCLC tools are used to enrich these clusters with more metadata.",
    },
    {
      icon: <ChunkIcon />,
      title: "CHUNKING",
      description:
        "Chunking is a process that breaks up raw text into smaller, more manageable segments that can be efficiently stored and retrieved. Each text chunk retains some metadata that allows us to trace it back to its source.",
    },
    {
      icon: <EmbedIcon />,
      title: "EMBEDDING",
      description:
        "Embedding, or vectorization, transforms text chunks into numerical arrays called vectors. Semantically similar chunks are mapped together in vector space, allowing the system to quickly identify related content.",
    },
    {
      icon: <StorageIcon />,
      title: "STORAGE",
      description:
        "Embedded data is stored in a special vector database optimized for storing, querying, and retrieving information quickly. When users ask the tool a question, this database is searched to find the best matches.",
    },
  ];
  return (
    <Box
      display="flex"
      flexDir="column"
      margin="0 auto"
      marginTop={{ base: "s", md: "xl" }}
      marginBottom={{ base: "xl", md: "xxl" }}
    >
      {pipelineSteps.map((stage, index) => (
        <Flex
          key={index}
          flexDir="row"
          gap={{ base: "s", md: "58px" }}
          alignItems="stretch"
        >
          <Flex flexDir="column" alignItems="center" flexShrink={0}>
            <Box
              background="ui.white"
              border="2px solid"
              borderColor="section.research.secondary"
              borderRadius="16px"
              padding="s"
              display="inline-flex"
            >
              {stage.icon}
            </Box>
            {index < pipelineSteps.length - 1 && (
              <Box flex="1" display="flex" justifyContent="center">
                <ArrowSVG />
              </Box>
            )}
          </Flex>
          <Box
            maxWidth="395px"
            paddingBottom={index < pipelineSteps.length - 1 ? "xl" : "0"}
          >
            <Heading
              size="heading8"
              fontWeight="bold"
              level="h4"
              marginBottom="s"
            >
              {stage.title}
            </Heading>
            <Text
              color="ui.gray.dark"
              fontSize="desktop.subtitle.subtitle1"
              lineHeight="135%;"
            >
              {stage.description}
            </Text>
          </Box>
        </Flex>
      ))}
    </Box>
  );
};

export default IngestionPipeline;
