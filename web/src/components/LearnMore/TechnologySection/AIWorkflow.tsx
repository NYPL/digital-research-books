import { Box, Flex, Heading, Text } from "@nypl/design-system-react-components";
import ArrowSVG from "./ArrowSVG";
import ChatIcon from "./ChatIcon";
import InputIcon from "./InputIcon";
import LLMIcon from "./LLMIcon";
import LookupIcon from "./LookupIcon";
import RAGIcon from "./RAGIcon";

const AiWorkflow = () => {
  const workflowSteps = [
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
    <Box
      display="flex"
      flexDir="column"
      margin="0 auto"
      marginTop={{ base: "s", md: "xl" }}
    >
      {workflowSteps.map((stage, index) => (
        <Flex
          key={index}
          flexDir="row"
          gap={{ base: "s", sm: "xl", md: "58px" }}
          alignItems="stretch"
        >
          {/* Left: icon + stretchy arrow below */}
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
            {index < workflowSteps.length - 1 && (
              <Box flex="1" display="flex" justifyContent="center">
                <ArrowSVG />
              </Box>
            )}
          </Flex>
          {/* Right: text */}
          <Box
            maxWidth="395px"
            paddingBottom={index < workflowSteps.length - 1 ? "xl" : "0"}
          >
            <Heading
              size="heading8"
              fontWeight="700"
              level="h4"
              marginBottom="s"
            >
              {stage.title}
            </Heading>
            <Text
              color="ui.gray.dark"
              fontSize={{
                base: "mobile.subtitle.subtitle1",
                md: "desktop.subtitle.subtitle1",
              }}
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

export default AiWorkflow;
