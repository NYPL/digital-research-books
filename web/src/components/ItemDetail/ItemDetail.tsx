import {
  Box,
  Flex,
  Grid,
  GridItem,
  Toggle,
  Tooltip,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  getGridColumns,
  getGridRows,
  getHeaderPaddingRight,
  HEADER_HEIGHT,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { HistoryItem } from "~/src/types/ResearchAssistant";
import { ApiWork, WorkResult } from "~/src/types/WorkQuery";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import ResearchAssistantPanel from "../ResearchAssistant/ResearchAssistantPanel";
import ResearchAssistantViewer from "../ResearchAssistant/ResearchAssistantViewer";
import ItemDetailSidebar from "./ItemDetailSidebar";

interface ItemDetailProps {
  workResult: WorkResult;
  backUrl?: string;
}

const ItemDetail: React.FC<ItemDetailProps> = ({ workResult, backUrl }) => {
  const [vraEnabled, setVraEnabled] = useState(true);
  const [hasPreviewLoaded, setHasPreviewLoaded] = useState(false);

  const {
    clearHistory,
    messages,
    setMessages,
    itemId,
    pageId,
    handlePreview,
    goToPreviousState,
    setViewState,
    historyStack,
    setHistoryStack,
    showChat,
  } = useResearchAssistant();

  const { page } = useResultPageContext();

  const router = useRouter();
  const { previewItemId, previewPage } = router.query;

  const work: ApiWork = workResult.data;

  const { previewEdition, previewItem } = useMemo(() => {
    if (!work.editions || work.editions.length === 0)
      return { previewEdition: undefined, previewItem: undefined };

    for (const edition of work.editions) {
      if (!edition.items) continue;
      for (const item of edition.items) {
        if (item.links?.some((link) => link.mediaType === "application/ocr")) {
          return { previewEdition: edition, previewItem: item };
        }
      }
    }

    const firstEdition = work.editions[0];
    return {
      previewEdition: firstEdition,
      previewItem: EditionCardUtils.getPreviewItem(firstEdition.items),
    };
  }, [work.editions]);

  const previewLink = useMemo(() => {
    if (previewPage) {
      return {
        url: `/item/${previewItemId}/page/${previewPage}`,
      };
    }
    return EditionCardUtils.getReadOnlineLink(previewItem);
  }, [previewPage, previewItemId, previewItem]);

  useEffect(() => {
    if (previewLink && !hasPreviewLoaded) {
      handlePreview(previewLink.url);
      setHasPreviewLoaded(true);
    }
  }, [previewLink, handlePreview, hasPreviewLoaded]);

  const effectPayloadRef = useRef({
    historyStack,
    messages,
    clearHistory,
    setMessages,
    setViewState,
    page,
    previewEditionId: previewEdition?.edition_id,
  });

  effectPayloadRef.current = {
    historyStack,
    messages,
    clearHistory,
    setMessages,
    setViewState,
    page,
    previewEditionId: previewEdition?.edition_id,
  };

  useEffect(() => {
    if (previewLink?.url && router.pathname.startsWith("/item")) {
      const {
        historyStack,
        messages,
        clearHistory,
        setMessages,
        setViewState,
        page,
        previewEditionId,
      } = effectPayloadRef.current;

      const urlParts = previewLink.url.split("/");
      const [itemId, pageId] = [urlParts.at(-3), urlParts.at(-1)];

      sessionStorage.setItem("vraHistoryStack", JSON.stringify(historyStack));
      sessionStorage.setItem("vraMessages", JSON.stringify(messages));

      clearHistory(page);
      setViewState((prev) => ({
        ...prev,
        itemId,
        pageId,
        editionId: previewEditionId,
        results: null,
        linkResults: null,
      }));

      setMessages([]);
    }
  }, [previewLink.url, router.pathname]);

  const handleBackToResults = () => {
    const storedHistory = sessionStorage.getItem("vraHistoryStack");
    const storedMessages = sessionStorage.getItem("vraMessages");
    let parsedHistory: HistoryItem[] = [];
    if (storedHistory) {
      parsedHistory = JSON.parse(storedHistory);
      setHistoryStack(parsedHistory);
    }
    if (storedMessages) setMessages(JSON.parse(storedMessages));
    sessionStorage.removeItem("vraHistoryStack");
    sessionStorage.removeItem("vraMessages");
    goToPreviousState(parsedHistory);
    router.push("/research-assistant");
  };

  return (
    <Box fontSize="desktop.body.body2" bgColor="ui.bg.default" width="100%">
      <Grid
        templateColumns={getGridColumns(vraEnabled, showChat)}
        templateRows={getGridRows(backUrl)}
        gap="l"
        marginBottom="xs"
        margin="0 auto"
        width="100%"
      >
        <GridItem colSpan={1} gridRow={backUrl ? "2" : "1"} />
        {backUrl && (
          <Flex
            alignItems="center"
            justifyContent="space-between"
            padding="s"
            bgColor="ui.white"
            gridColumn={vraEnabled ? "1 / span 3" : "1 / span 4"}
            gridRow="1"
            borderBottom="1px solid"
            borderColor="ui.border.default"
            paddingLeft="calc((100vw - 1280px) / 2)"
            paddingRight={getHeaderPaddingRight(vraEnabled)}
            paddingY="s"
            marginRight={vraEnabled ? "0" : "-2rem"}
            height={HEADER_HEIGHT}
          >
            <BackToResultsButton handleBackToResults={handleBackToResults} />
            <Tooltip
              content="Toggle off if you would like to opt out of using the AI tool. When toggled off, chat window will close and chat history will be lost."
              shouldWrapChildren
            >
              <Toggle
                isChecked={vraEnabled}
                labelText="Use Virtual Research Assistant"
                onChange={() => setVraEnabled((prev) => !prev)}
                size="small"
                sx={{
                  ".chakra-switch__track[data-checked]": {
                    backgroundColor: "section.research.secondary",
                  },
                }}
              />
            </Tooltip>
          </Flex>
        )}
        <ItemDetailSidebar
          work={work}
          previewEdition={previewEdition}
          previewItem={previewItem}
          backUrl={backUrl}
        />
        <Box
          gridColumn="3"
          gridRow={backUrl ? "2" : "1"}
          marginRight={vraEnabled ? "l" : "0"}
          marginTop={backUrl ? "0" : "l"}
          paddingBottom="l"
        >
          <ResearchAssistantViewer itemId={itemId} pageId={pageId} />
        </Box>
        <Box
          gridColumn={vraEnabled ? "4 / span 2" : "4"}
          gridRow={backUrl ? "1 / span 2" : "1"}
          height="100%"
          marginLeft="-2rem"
          display="flex"
          flexDirection="column"
        >
          {vraEnabled && <ResearchAssistantPanel />}
        </Box>
      </Grid>
    </Box>
  );
};

export default ItemDetail;
