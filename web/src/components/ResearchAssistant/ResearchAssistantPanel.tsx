import {
  Box,
  Button,
  Flex,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import React from "react";
import { getPanelLayout } from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import ArrowIcon from "./icons/ArrowIcon";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";
import RewindIcon from "./icons/RewindIcon";
import ResearchAssistantHeader from "./ResearchAssistantHeader";
import ResearchAssistantInput from "./ResearchAssistantInput";
import ResearchAssistantWindow from "./ResearchAssistantWindow";

const ResearchAssistantPanel: React.FC = () => {
  const { clearHistory, showChat, toggleChat } = useResearchAssistant();

  const { paddingX } = getPanelLayout();

  const { page } = useResultPageContext();

  return (
    <Box
      flex="1"
      display="flex"
      flexDirection="column"
      bgColor="section.research.primary"
      maxHeight="100vh"
      position="sticky"
      top="0"
      width="100%"
      paddingLeft={paddingX}
      paddingRight={page === "item" ? "l" : undefined}
    >
      {showChat ? (
        <>
          <ResearchAssistantHeader>
            <Heading
              level="h2"
              size="heading7"
              color="ui.white"
              display="flex"
              alignItems="center"
              gap="xs"
              height="40px"
            >
              <ResearchAssistantIcon inCircle />
              <span>Virtual Research Assistant</span>
            </Heading>
            <Flex gap="xxs">
              <Button
                onClick={() => clearHistory(page, true)}
                variant="text"
                size="small"
                color="ui.white"
                fontSize="desktop.body.body2"
                id="clear-history-button"
                sx={{
                  "&:hover": {
                    color: "ui.white",
                    opacity: 0.8,
                    path: {
                      stroke: "ui.white",
                    },
                  },
                }}
              >
                <Flex gap="xxs" alignItems="center">
                  <RewindIcon /> <Text>Start over</Text>
                </Flex>
              </Button>
              <Button
                onClick={toggleChat}
                variant="text"
                size="small"
                color="ui.white"
                fontSize="desktop.body.body2"
                id="hide-chat-button"
                sx={{
                  "&:hover": {
                    color: "ui.white",
                    opacity: 0.8,
                    path: {
                      stroke: "ui.white",
                    },
                  },
                }}
              >
                <Flex gap="xxs" alignItems="center">
                  <Text>Hide chat</Text>
                  <ArrowIcon color="#FFF" />
                </Flex>
              </Button>
            </Flex>
          </ResearchAssistantHeader>

          <ResearchAssistantWindow />
          <Box
            backgroundColor="section.research.primary"
            position="sticky"
            bottom="0"
            zIndex="1000"
          >
            <ResearchAssistantInput />
          </Box>
        </>
      ) : (
        <ResearchAssistantHeader>
          <Button
            onClick={toggleChat}
            variant="text"
            size="small"
            color="ui.white"
            fontSize="0"
            id="hide-chat-button"
            sx={{
              "&:hover": {
                color: "ui.white",
                opacity: 0.8,
                path: {
                  stroke: "ui.white",
                },
              },
            }}
          >
            <Flex gap="xxs" alignItems="center">
              <ArrowIcon direction="left" color="#FFF" />
              <Text fontSize="desktop.body.body2">Show chat</Text>
            </Flex>
          </Button>
        </ResearchAssistantHeader>
      )}
    </Box>
  );
};

export default ResearchAssistantPanel;
