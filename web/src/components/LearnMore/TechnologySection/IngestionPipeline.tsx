import { Box, Heading, Text } from "@nypl/design-system-react-components";
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
      flexDir="row"
      gap="58px"
      margin="0 auto"
      marginTop="xl"
      marginBottom="xxl"
    >
      <Box display="flex" flexDir="column">
        {pipelineSteps.map((stage, index) => (
          <>
            <Box
              key={index}
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
              <Box
                width="0"
                height="8rem"
                borderColor="section.research.secondary"
                borderWidth="1px"
                margin="0 auto"
              ></Box>
            )}
          </>
        ))}
      </Box>
      <Box display="flex" flexDir="column" gap="xl">
        {pipelineSteps.map((stage, index) => (
          <Box key={index} width="395px">
            <Heading size="heading8" fontWeight="700">
              {stage.title}
            </Heading>
            <Text fontSize="lg">{stage.description}</Text>
          </Box>
        ))}
      </Box>
    </Box>
  );
};

export default IngestionPipeline;
