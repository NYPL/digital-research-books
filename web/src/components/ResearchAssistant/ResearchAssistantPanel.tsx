import {
  Box,
  Button,
  Flex,
  Heading,
  Menu,
  Text,
  useNYPLBreakpoints,
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
  onResizeKeyDown?: (event: React.KeyboardEvent<HTMLDivElement>) => void;
  onExpandToFull?: () => void;
  onDecreaseToMin?: () => void;
  panelHeight?: number;
  minPanelHeight?: number;
  maxPanelHeight?: number;
};

const ResearchAssistantPanel: React.FC<ResearchAssistantPanelProps> = ({
  onResizeStart,
  onResizeKeyDown,
  onExpandToFull,
  onDecreaseToMin,
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
  const { isLargerThanMedium } = useNYPLBreakpoints();

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
      if (!showChat) {
        showChatButtonRef.current?.focus();
      }
    }
  }, [showChat]);

  const handleStartOver = () => {
    if (messages.length === 0) return;

    clearHistory(page, true);
    if (!isLargerThanMedium) {
      const resultsTop = document.getElementById("mainContent");
      if (resultsTop) {
        resultsTop.scrollIntoView({ behavior: "smooth", block: "start" });
      } else {
        window.scrollTo({ top: 0, behavior: "smooth" });
      }
    }
    announce("Chat cleared.");
  };

  return (
    <Box
      flex="1"
      display="flex"
      flexDirection="column"
      maxHeight={{ base: "100%", md: "100vh" }}
      minHeight="0px"
      width="100%"
      paddingLeft={{ base: "s", md: paddingX.md }}
      paddingRight={page === "item" ? "l" : undefined}
      role="region"
      aria-labelledby="vra-panel-heading"
      sx={{ position: "relative" }}
      backgroundColor="section.research.primary"
    >
      {showChat ? (
        <>
          <Box
            display={{ base: "flex", md: "none" }}
            alignItems="center"
            justifyContent="center"
            width="100%"
            position="absolute"
            top="0"
            left="0"
            right="0"
            height="18px"
            zIndex="10000"
            onPointerDown={onResizeStart}
            onKeyDown={onResizeKeyDown}
            tabIndex={0}
            role="separator"
            aria-orientation="horizontal"
            aria-label="Resize enhanced search panel"
            aria-controls="research-assistant-chat-panel"
            aria-valuemin={minPanelHeight}
            aria-valuemax={maxPanelHeight}
            aria-valuenow={panelHeight}
            sx={{ touchAction: "none", cursor: "ns-resize" }}
          >
            <Box
              width="44px"
              height="4px"
              borderRadius="999px"
              bgColor="ui.white"
              sx={{ opacity: 0.65 }}
            />
          </Box>
          <ResearchAssistantHeader showChat={showChat}>
            <Heading
              level="h2"
              size="heading7"
              color="ui.white"
              display="flex"
              alignItems="center"
              gap="xs"
              height="auto"
              id="vra-panel-heading"
            >
              <ResearchAssistantIcon color="#ECFAFB" size="large" />
              <span>Enhanced Search</span>
            </Heading>
            <Flex gap="xxs" alignItems="center">
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
                  <ArrowIcon
                    direction={{ base: "down", md: "right" }}
                    color="#FFF"
                  />
                </Flex>
              </Button>
              <Menu
                display={{ base: "block", md: "none" }}
                showLabel={false}
                labelText="Panel size options"
                sx={{
                  ".chakra-menu__menu-button": {
                    color: "ui.white",
                    border: "none !important",
                    boxShadow: "none",
                  },
                  ".chakra-menu__menu-button:hover, .chakra-menu__menu-button:focus, .chakra-menu__menu-button:active": {
                    border: "none !important",
                    boxShadow: "none",
                  },
                  ".chakra-menu__menu-button > span > svg, .chakra-menu__menu-button > span > svg path": {
                    fill: "ui.white",
                  },
                }}
                listItemsData={[
                  {
                    type: "action",
                    id: "increase-panel-size",
                    label: "Increase panel size",
                    media: {
                      type: "svg",
                      src: `data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18" fill="none"><path d="M13.5 11.25L9 6.75L4.5 11.25" stroke="black" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
                      alt: "arrow up icon",
                    },
                    onClick: () => onExpandToFull?.(),
                  },
                  {
                    type: "action",
                    id: "decrease-panel-size",
                    label: "Decrease panel size",
                    media: {
                      type: "svg",
                      src: `data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18" fill="none"><path d="M4.5 6.75L9 11.25L13.5 6.75" stroke="black" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
                      alt: "arrow down icon",
                    },
                    onClick: () => onDecreaseToMin?.(),
                  },
                ]}
              />
            </Flex>
          </ResearchAssistantHeader>

          <ResearchAssistantWindow />
          <Box
            backgroundColor="section.research.primary"
            bottom="0"
            zIndex="10001"
            position="sticky"
          >
            <ResearchAssistantInput onExpandToFull={onExpandToFull} />
          </Box>
        </>
      ) : (
        <ResearchAssistantHeader showChat={showChat}>
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
              <ArrowIcon direction={{ base: "up", md: "left" }} color="#FFF" />
              <Text fontSize="desktop.body.body2">Show chat</Text>
            </Flex>
          </Button>
        </ResearchAssistantHeader>
      )}
    </Box>
  );
};

export default ResearchAssistantPanel;
