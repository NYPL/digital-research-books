import { Box, Flex } from "@nypl/design-system-react-components";
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  HEADER_HEIGHT,
  MARGIN_BLEED,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { isCatalogResults } from "~/src/util/ResearchAssistantUtils";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import EmptySearchPrompt from "../EmptySearchPrompt/EmptySearchPrompt";
import CatalogResults from "./CatalogResults/CatalogResults";
import CatalogResultsSkeleton from "./CatalogResults/CatalogResultsSkeleton";
import ResearchAssistantPanel from "./ResearchAssistantPanel";
import ResultsBanner from "./ResultsBanner";

const COLLAPSED_PANEL_HEIGHT = 68;
const MIN_RESIZABLE_PANEL_HEIGHT = 280;

const ResearchAssistant: React.FC = () => {
  const {
    messages,
    sendMessage,
    results,
    historyStack,
    goToPreviousState,
    showChat,
    isLoading,
  } = useResearchAssistant();
  const [mobilePanelHeight, setMobilePanelHeight] = useState(640);
  const resizeStateRef = useRef({
    isResizing: false,
    startY: 0,
    startHeight: 640,
  });

  const getMaxPanelHeight = useCallback(() => {
    if (typeof window === "undefined") return 900;
    return window.innerHeight;
  }, []);

  const clampPanelHeight = useCallback(
    (height: number) => {
      const maxPanelHeight = getMaxPanelHeight();
      return Math.min(
        Math.max(height, MIN_RESIZABLE_PANEL_HEIGHT),
        maxPanelHeight
      );
    },
    [getMaxPanelHeight]
  );

  useEffect(() => {
    if (!messages || messages.length === 0) {
      const initialMessage = sessionStorage.getItem(
        "researchAssistantInitialMessage"
      );
      if (initialMessage) {
        sendMessage(initialMessage);
        sessionStorage.removeItem("researchAssistantInitialMessage");
      }
    }
  }, [messages, sendMessage]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const initializeHeight = () => {
      const viewportHeight = window.innerHeight;
      const defaultHeight = Math.round(viewportHeight * 0.8);
      setMobilePanelHeight((previousHeight) => {
        if (previousHeight === 640) {
          return clampPanelHeight(defaultHeight);
        }
        return clampPanelHeight(previousHeight);
      });
    };

    initializeHeight();
    window.addEventListener("resize", initializeHeight);

    return () => {
      window.removeEventListener("resize", initializeHeight);
    };
  }, [clampPanelHeight]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const handlePointerMove = (event: PointerEvent) => {
      if (!resizeStateRef.current.isResizing) return;
      event.preventDefault();

      const deltaY = resizeStateRef.current.startY - event.clientY;
      const nextHeight = clampPanelHeight(
        resizeStateRef.current.startHeight + deltaY
      );
      setMobilePanelHeight(nextHeight);
    };

    const stopResizing = () => {
      resizeStateRef.current.isResizing = false;
    };

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", stopResizing);
    window.addEventListener("pointercancel", stopResizing);

    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", stopResizing);
      window.removeEventListener("pointercancel", stopResizing);
    };
  }, [clampPanelHeight]);

  useEffect(() => {
    if (!showChat) {
      resizeStateRef.current.isResizing = false;
      return;
    }

    setMobilePanelHeight(getMaxPanelHeight());
  }, [getMaxPanelHeight, showChat]);

  const handleResizeStart = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      if (!showChat) return;

      event.preventDefault();
      resizeStateRef.current = {
        isResizing: true,
        startY: event.clientY,
        startHeight: mobilePanelHeight,
      };
    },
    [mobilePanelHeight, showChat]
  );

  const gridTemplateColumns = showChat
    ? { base: "1fr", md: "1fr minmax(0, 640px) minmax(0, 640px) 1fr" }
    : { base: "1fr", md: "1fr minmax(0, 1152px) minmax(0, 128px) 1fr" };

  const latestResults = useMemo(() => {
    if (!results) return null;

    const exactMatch = results[messages.length];
    if (exactMatch) return exactMatch;

    const sortedResults = Object.entries(results)
      .filter(([, value]) => value && Object.keys(value).length > 0)
      .sort(([a], [b]) => Number(b) - Number(a));

    if (sortedResults.length > 0) {
      return sortedResults[0][1];
    }

    return null;
  }, [messages.length, results]);

  return (
    <>
      <Box
        display="grid"
        gridTemplateColumns={gridTemplateColumns}
        width="100%"
        minHeight="100vh"
        id="mainContent"
        role="main"
      >
        <Flex
          gridColumn={{ base: "1 / -1", md: "1 / span 2" }}
          flexDirection="column"
          minWidth="0"
          justifyContent="flex-end"
          alignItems="flex-end"
          bgColor="ui.bg.default"
          paddingBottom={{
            base: showChat
              ? `${mobilePanelHeight}px`
              : `${COLLAPSED_PANEL_HEIGHT}px`,
            md: "0",
          }}
        >
          <Flex
            width={{ base: "100%", md: "100%" }}
            maxWidth={{ md: showChat ? "640px" : "1152px" }}
            flexDirection="column"
            height="100%"
            justifyContent="flex-end"
            alignItems="flex-end"
          >
            <Flex flexDirection="column" flex="1" width="100%">
              {latestResults && historyStack.length > 1 && (
                <Box
                  padding="s"
                  borderBottom="1px solid"
                  borderColor="ui.border.default"
                  height={HEADER_HEIGHT}
                  marginLeft={MARGIN_BLEED}
                  paddingLeft={PADDING_COUNTER}
                >
                  <BackToResultsButton
                    handleBackToResults={() => goToPreviousState()}
                  />
                </Box>
              )}
              {!latestResults && !isLoading && (
                <Box
                  padding="s"
                  borderBottom="1px solid"
                  borderColor="ui.border.default"
                  height={HEADER_HEIGHT}
                  marginLeft={MARGIN_BLEED}
                  paddingLeft={PADDING_COUNTER}
                />
              )}
              <Box paddingBottom="l" flex="1">
                {isLoading ? (
                  <CatalogResultsSkeleton />
                ) : latestResults && Object.keys(latestResults).length > 0 ? (
                  <>
                    {isCatalogResults(latestResults) && (
                      <CatalogResults results={latestResults} />
                    )}
                  </>
                ) : (
                  <Box width="100%" marginTop="s">
                    <ResultsBanner />
                    <EmptySearchPrompt
                      message={
                        messages.length > 1
                          ? "No results found. Try a different topic."
                          : undefined
                      }
                    />
                  </Box>
                )}
              </Box>
            </Flex>
          </Flex>
        </Flex>
        <Flex
          gridColumn={{ base: "1 / -1", md: "3 / span 2" }}
          flexDirection="column"
          bgColor="section.research.primary"
          height={{
            base: showChat
              ? `${mobilePanelHeight}px`
              : `${COLLAPSED_PANEL_HEIGHT}px`,
            md: "100vh",
          }}
          maxHeight={{
            base: showChat
              ? `${mobilePanelHeight}px`
              : `${COLLAPSED_PANEL_HEIGHT}px`,
            md: "100vh",
          }}
          position={{ base: "fixed", md: "sticky" }}
          top={{ base: "auto", md: "0" }}
          bottom={{ base: "0", md: "auto" }}
          left={{ base: "0", md: "auto" }}
          right={{ base: "0", md: "auto" }}
          zIndex="1000"
          minWidth="0"
          minHeight="0"
          width={{ base: "100%", md: "auto" }}
          justifyContent="flex-start"
          alignItems="flex-start"
          borderRadius={{ base: "8px 8px 0 0", md: "0" }}
          overflow="hidden"
        >
          <Flex
            width={{ base: "100%", md: "100%" }}
            maxWidth={{ md: "640px" }}
            flexDirection="column"
            height="100%"
            minHeight="0"
            justifyContent="flex-start"
            alignItems="flex-start"
          >
            <ResearchAssistantPanel
              onResizeStart={handleResizeStart}
              panelHeight={mobilePanelHeight}
              minPanelHeight={MIN_RESIZABLE_PANEL_HEIGHT}
              maxPanelHeight={getMaxPanelHeight()}
            />
          </Flex>
        </Flex>
      </Box>
    </>
  );
};

export default ResearchAssistant;
