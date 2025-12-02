import React from "react";
import {
  Box,
  Button,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import ResearchAssistantIcon from "./ResearchAssistantIcon";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import ResearchAssistantInput from "./ResearchAssistantInput";

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
      <Button onClick={clearHistory} id="clear-history-button">
        Clear chat
      </Button>
    </Box>

    <Box flex="1" overflowY="auto">
      <ResearchAssistantWindow messages={messages} isLoading={isLoading} />
      {error && <Text fontWeight="bold">{error}</Text>}
    </Box>

    <Box
      position="sticky"
      bottom="0"
      zIndex="1000"
    >
      <ResearchAssistantInput
        onSendMessage={onSendMessage}
        isDisabled={isLoading}
        messages={messages}
      />
    </Box>
  </Box>
);

export default ResearchAssistantPanel;
