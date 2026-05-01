import { Box, Heading, Text } from "@nypl/design-system-react-components";
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
      title: "RETRIEVAL-AUGMENTED GENERATION (RAG)",
      description:
        "The checking of embedded questions against vector and relational databases is done by the LLM. We currently use Google's Gemini Flash and Embeddings models to implement our agentic workflows. ",
    },
    {
      icon: <RAGIcon />,
      title: "EMBEDDING",
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

export default AiWorkflow;
