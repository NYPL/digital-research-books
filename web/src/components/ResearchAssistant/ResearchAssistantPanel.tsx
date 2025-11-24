import React from "react";
import {
  Box,
  Button,
  Flex,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";
import RewindIcon from "./RewindIcon";

type ResearchAssistantPanelProps = {
  messages: any[];
  isLoading: boolean;
  error?: string;
  onSendMessage: (msg: string) => void;
  clearHistory: () => void;
};

const ResearchAssistantPanel: React.FC<ResearchAssistantPanelProps> = ({
  messages,
  isLoading,
  error,
  onSendMessage,
  clearHistory,
}) => (
  <Box
    flex="1"
    display="flex"
    flexDirection="column"
    bgColor="section.research.primary"
    maxHeight="100vh"
    position="sticky"
    top="0"
  >
    <Box
      bgColor="section.research.primary"
      display="flex"
      justifyContent="space-between"
      alignItems="center"
      paddingX="l"
      paddingY="s"
      borderBottom="1px white solid"
      position="sticky"
      top="0"
      zIndex="999"
    >
      <Heading
        level="h2"
        size="heading7"
        color="ui.white"
        display="flex"
        alignItems="center"
        gap="xs"
      >
        <ResearchAssistantIcon inCircle />
        <span>Virtual Research Assistant</span>
      </Heading>
      <Button
        onClick={clearHistory}
        variant="text"
        color="ui.white"
        fontSize="0"
        id="clear-history-button"
        sx={{
          "&:hover": {
            color: "ui.link.secondary",
            path: {
              stroke: "ui.link.secondary",
            },
          },
        }}
      >
        <Flex gap="xxs" alignItems="center">
          <RewindIcon /> <Text>Start over</Text>
        </Flex>
      </Button>
    </Box>

    <ResearchAssistantWindow messages={messages} isLoading={isLoading} />

    {error && <Text fontWeight="bold">{error}</Text>}

    <ResearchAssistantInput
      onSendMessage={onSendMessage}
      isDisabled={isLoading}
      messages={messages}
    />
  </Box>
);

export default ResearchAssistantPanel;
