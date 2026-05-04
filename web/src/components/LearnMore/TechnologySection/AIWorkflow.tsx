import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";
import ChatIcon from "./ChatIcon";
import InputIcon from "./InputIcon";
import LLMIcon from "./LLMIcon";
import LookupIcon from "./LookupIcon";
import RAGIcon from "./RAGIcon";

const AiWorkflow = () => {
  const pipelineSteps = [
    {
      icon: <InputIcon />,
      title: "USER INPUT",
      description:
        "The user's question is sent to the back end to be embedded (or converted into vectors). Embeddings are mathematical representations of natural language queries that can be read by machines to understand the user's intent. ",
    },
    {
      icon: <LookupIcon />,
      title: "DATABASE LOOKUP",
      description:
        "The embedded question is searched against either a vector or a relational database (or both). Vector databases retrieve semantically relevant text chunks, while relational databases retrieve keyword matches. ",
    },
    {
      icon: <LLMIcon />,
      title: "LARGE LANGUAGE MODEL (LLM)",
      description:
        "The checking of embedded questions against vector and relational databases is done by the LLM. We currently use Google's Gemini Flash and Embeddings models to implement our agentic workflows.",
    },
    {
      icon: <RAGIcon />,
      title: "RETRIEVAL-AUGMENTED GENERATION (RAG)",
      description:
        "As information is retrieved from the collection, it gets added to the user's original question to create a richer context for the LLM. The LLM then uses this to generate a contextually relevant answer that includes source citations.",
    },
    {
      icon: <ChatIcon />,
      title: "CHAT OUTPUT",
      description:
        "The answer provided by the LLM is converted back into a natural language response by the agent and conveyed to the user through the chat. The tool's rationale is shown to the user alongside the most relevant search results.",
    },
  ];
  return (
    <Box display="flex" flexDir="row" gap="58px" margin="0 auto" marginTop="xl">
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

export default AiWorkflow;
