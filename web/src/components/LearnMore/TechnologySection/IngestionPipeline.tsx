import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";
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
      <Flex flexDir="column">
        {pipelineSteps.map((stage, index) => (
          <Box key={index}>
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
              <Flex justifyContent="center">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="15"
                  height="129"
                  viewBox="0 0 15 129"
                  fill="none"
                >
                  <path
                    d="M6.65667 128.707C7.04719 129.098 7.68036 129.098 8.07088 128.707L14.4348 122.343C14.8254 121.953 14.8254 121.319 14.4348 120.929C14.0443 120.538 13.4112 120.538 13.0206 120.929L7.36378 126.586L1.70692 120.929C1.3164 120.538 0.683231 120.538 0.292707 120.929C-0.0978174 121.319 -0.0978173 121.953 0.292707 122.343L6.65667 128.707ZM7.36377 0L6.36377 4.37114e-08L6.36378 128L7.36378 128L8.36378 128L8.36377 -4.37114e-08L7.36377 0Z"
                    fill="#006166"
                  />
                </svg>
              </Flex>
            )}
          </Box>
        ))}
      </Flex>
      <Flex flexDir="column" gap="xl">
        {pipelineSteps.map((stage, index) => (
          <Box key={index} width="395px">
            <Heading size="heading8" fontWeight="700">
              {stage.title}
            </Heading>
            <Text fontSize="lg">{stage.description}</Text>
          </Box>
        ))}
      </Flex>
    </Box>
  );
};

export default IngestionPipeline;
