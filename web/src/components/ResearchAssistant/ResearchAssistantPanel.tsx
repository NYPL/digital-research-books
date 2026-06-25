import {
  Box,
  Button,
  Flex,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import React, { useEffect, useRef } from "react";
import { getPanelLayout } from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { chatAnnouncer } from "~/src/lib/chatAnnouncer/ChatAnnouncer";
import ArrowIcon from "./icons/ArrowIcon";
import ResearchAssistantIcon from "./icons/ResearchAssistantIcon";
import RewindIcon from "./icons/RewindIcon";
import ResearchAssistantHeader from "./ResearchAssistantHeader";
import ResearchAssistantInput from "./ResearchAssistantInput";
import ResearchAssistantWindow from "./ResearchAssistantWindow";

type ResearchAssistantPanelProps = {
  onResizeStart?: (event: React.PointerEvent<HTMLDivElement>) => void;
  panelHeight?: number;
  minPanelHeight?: number;
  maxPanelHeight?: number;
};

const ResearchAssistantPanel: React.FC<ResearchAssistantPanelProps> = ({
  onResizeStart,
  panelHeight,
  minPanelHeight,
  maxPanelHeight,
}) => {
  const {
    clearHistory,
    messages,
    showChat,
    toggleChat,
  } = useResearchAssistant();

  const { paddingX } = getPanelLayout();

  const { page } = useResultPageContext();

  const announce = chatAnnouncer.announce;

  const showChatButtonRef = useRef<HTMLButtonElement>(null);
  const hideChatButtonRef = useRef<HTMLButtonElement>(null);
  const isFirstRender = useRef(true);

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false;
      return;
    }
    if (showChat) {
      hideChatButtonRef.current?.focus();
    } else {
      showChatButtonRef.current?.focus();
    }
  }, [showChat]);

  const handleStartOver = () => {
    if (messages.length === 0) return;

    clearHistory(page, true);
    announce("Chat cleared.");
  };

  return (
    <Box
      flex="1"
      display="flex"
      flexDirection="column"
      maxHeight={{ base: "100%", md: "100vh" }}
      minHeight="0"
      position={{ base: "relative", md: "sticky" }}
      top={{ base: "auto", md: "0" }}
      width="100%"
      paddingLeft={paddingX}
      paddingRight={page === "item" ? "l" : undefined}
      overflow="hidden"
      sx={{ overscrollBehavior: "contain" }}
      role="region"
      aria-labelledby="vra-panel-heading"
    >
      {showChat ? (
        <>
          <Box
            display={{ base: "flex", md: "none" }}
            alignItems="center"
            justifyContent="center"
            width="100%"
            paddingTop="xs"
            onPointerDown={onResizeStart}
            role="separator"
            aria-orientation="horizontal"
            aria-label="Resize enhanced search panel"
            aria-valuenow={panelHeight}
            aria-valuemin={minPanelHeight}
            aria-valuemax={maxPanelHeight}
            sx={{
              cursor: "ns-resize",
              touchAction: "none",
            }}
          >
            <Box
              width="44px"
              height="4px"
              borderRadius="999px"
              bgColor="ui.white"
              sx={{ opacity: 0.65 }}
            />
          </Box>
          <ResearchAssistantHeader>
            <Heading
              level="h2"
              size="heading7"
              color="ui.white"
              display="flex"
              alignItems="center"
              gap="xs"
              height="40px"
              id="vra-panel-heading"
            >
              <ResearchAssistantIcon color="#ECFAFB" size="large" />
              <span>Enhanced Search</span>
            </Heading>
            <Flex gap="xxs">
              <Button
                onClick={handleStartOver}
                variant="text"
                size="small"
                color="ui.white"
                fontSize="desktop.body.body2"
                outlineColor="ui.white"
                id="clear-history-button"
                sx={{
                  "&:hover": {
                    color: "ui.white",
                    opacity: 0.8,
                    path: {
                      stroke: "ui.white",
                    },
                  },
                  "&:not([disabled]):focus": {
                    outlineColor: "ui.white",
                  },
                }}
              >
                <Flex gap="xxs" alignItems="center">
                  <RewindIcon />{" "}
                  <Text display={{ base: "none", sm: "block" }}>
                    Start over
                  </Text>
                </Flex>
              </Button>
              <Button
                ref={hideChatButtonRef}
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
                  "&:not([disabled]):focus": {
                    outlineColor: "ui.white",
                  },
                }}
              >
                <Flex gap="xxs" alignItems="center">
                  <Text display={{ base: "none", sm: "block" }}>Hide chat</Text>
                  <ArrowIcon color="#FFF" />
                </Flex>
              </Button>
            </Flex>
          </ResearchAssistantHeader>

          <ResearchAssistantWindow />
          <Box
            backgroundColor="section.research.primary"
            bottom="0"
            zIndex="10001"
            position="sticky"
          >
            <ResearchAssistantInput />
          </Box>
        </>
      ) : (
        <ResearchAssistantHeader>
          <Box display={{ base: "block", md: "none" }}>
            <Heading
              level="h2"
              size="heading7"
              color="ui.white"
              display="flex"
              alignItems="center"
              gap="xs"
              id="vra-panel-heading"
            >
              <ResearchAssistantIcon color="#ECFAFB" size="large" />
              <span>Enhanced Search</span>
            </Heading>
          </Box>
          <Button
            ref={showChatButtonRef}
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
              "&:not([disabled]):focus": {
                outlineColor: "ui.white",
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
