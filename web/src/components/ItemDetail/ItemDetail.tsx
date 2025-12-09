import React, { useEffect, useMemo, useState } from "react";
import { useCookies } from "react-cookie";
import { useRouter } from "next/router";
import {
  Accordion,
  Box,
  Flex,
  Grid,
  GridItem,
  Heading,
  Text,
  Toggle,
  VStack,
} from "@nypl/design-system-react-components";
import AuthorsList from "../AuthorsList/AuthorsList";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import ResearchAssistantIcon from "../ResearchAssistant/icons/ResearchAssistantIcon";
import ResearchAssistantPanel from "../ResearchAssistant/ResearchAssistantPanel";
import ResearchAssistantViewer from "../ResearchAssistant/ResearchAssistantViewer";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { ApiWork, WorkResult } from "~/src/types/WorkQuery";
import {
  HistoryItem,
  MessageStatus,
  MessageType,
} from "~/src/types/ResearchAssistant";
import AboutItemPanel from "./AboutItemPanel";
import SummaryPanel from "./SummaryPanel";
import SearchPanel from "./SearchPanel";
import DownloadOptionsPanel from "./DownloadOptionsPanel";
import DetailsPanel from "./DetailsPanel";
import OtherEditionsPanel from "./OtherEditionsPanel";
import Link from "../Link/Link";
import {
  getGridColumns,
  getHeaderPaddingRight,
  getGridRows,
  GRID_PADDING_X,
} from "~/src/constants/researchAssistant";

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
  } = useResearchAssistant();

  const { page } = useResultPageContext();

  const router = useRouter();

  const work: ApiWork = workResult.data;

  const previewEdition = work.editions && work.editions[0];
  const previewItem = EditionCardUtils.getPreviewItem(previewEdition.items);
  const previewLink = useMemo(
    () => EditionCardUtils.getReadOnlineLink(previewItem),
    [previewItem]
  );

  useEffect(() => {
    if (previewLink && !hasPreviewLoaded) {
      handlePreview(previewLink.url);
      setHasPreviewLoaded(true);
    }
  }, [previewLink, handlePreview]);

  useEffect(() => {
    if (previewLink?.url && router.pathname.startsWith("/item")) {
      const urlParts = previewLink.url.split("/");
      const [itemId, pageId] = [urlParts.at(-3), urlParts.at(-1)];

      sessionStorage.setItem("vraHistoryStack", JSON.stringify(historyStack));
      sessionStorage.setItem("vraMessages", JSON.stringify(messages));

      clearHistory(page);
      setViewState((prev) => ({
        ...prev,
        itemId,
        pageId,
        showWebReader: true,
        results: null,
        linkResults: null,
      }));

      setMessages([
        {
          id: "assistant-initial",
          data: {
            content:
              "I can help you find relevant content in this book. Ask me a question, or try the suggestions below.",
          },
          status: MessageStatus.Sent,
          type: MessageType.Ai,
        },
      ]);
    }
  }, [previewLink?.url, router.pathname]);

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

  const publisherNames = previewEdition.publishers.map(
    (pubAgent) => pubAgent && pubAgent.name
  );

  const downloadLink = EditionCardUtils.selectDownloadLink(previewEdition);
  const authorNames = work.authors
    ? work.authors.map((author) => author.name)
    : [];
  const [cookies] = useCookies([NYPL_SESSION_ID]);
  const loginCookie = cookies[NYPL_SESSION_ID];
  const isLoggedIn = !!loginCookie;

  return (
    <Box fontSize="desktop.body.body2" bgColor="ui.bg.default" width="100%">
      <Grid
        templateColumns={getGridColumns(vraEnabled)}
        templateRows={getGridRows(backUrl)}
        gap="l"
        marginBottom="xs"
        margin="0 auto"
        width="100%"
      >
        <GridItem colSpan={1} gridRow={2} />
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
          >
            <BackToResultsButton handleBackToResults={handleBackToResults} />
            <Toggle
              isChecked={vraEnabled}
              labelText="Use Virtual Research Assistant"
              onChange={() => setVraEnabled((prev) => !prev)}
            />
          </Flex>
        )}
        <VStack
          alignContent="left"
          alignItems="left"
          bgColor="ui.bg.default"
          gridColumn="2"
          gridRow={backUrl ? "2" : "1"}
          paddingBottom="l"
          paddingLeft={GRID_PADDING_X}
          marginTop={backUrl ? "0" : "l"}
        >
          <Text size="caption" marginBottom="xxs">
            E-BOOK
          </Text>
          <Heading level="h1" size="heading6" marginBottom="xs">
            {work.title}
          </Heading>
          <VStack alignContent="left" alignItems="left" gap="l">
            {work.authors && work.authors.length > 0 && (
              <AuthorsList authors={work.authors} />
            )}
            {/* TODO: Re-add after download is implemented on the backend
            <DownloadLink
              authors={authorNames}
              downloadLink={downloadLink}
              title={work.title}
              isLoggedIn={isLoggedIn}
            /> 
            Placeholder for Download Link
            */}
            <Link
              to="#"
              variant="buttonSecondary"
              backgroundColor="ui.white"
              borderColor="section.research.secondary"
              color="section.research.secondary"
              width="fit-content"
            >
              Download PDF
            </Link>
            <Accordion
              accordionData={[
                {
                  ariaLabel: "About this item",
                  label: "About this item",
                  panel: (
                    <AboutItemPanel
                      previewItem={previewItem}
                      previewEdition={previewEdition}
                      publisherNames={publisherNames}
                    />
                  ),
                },
                {
                  ariaLabel: "Read summary",
                  label: (
                    <Box
                      display="flex"
                      alignItems="center"
                      gap="xxs"
                      __css={{ svg: { marginInlineStart: "0 !important" } }}
                    >
                      <ResearchAssistantIcon inCircle />
                      <span>Read summary</span>
                    </Box>
                  ),
                  panel: <SummaryPanel previewEdition={previewEdition} />,
                },
                {
                  ariaLabel: "Download options",
                  label: "Download options",
                  panel: (
                    <DownloadOptionsPanel
                      authorNames={authorNames}
                      downloadLink={downloadLink}
                      title={work.title}
                      isLoggedIn={isLoggedIn}
                    />
                  ),
                },
                {
                  ariaLabel: "Search inside this item",
                  label: "Search inside this item",
                  panel: <SearchPanel />,
                },
                {
                  ariaLabel: "Other editions",
                  label: "Other editions",
                  panel: <OtherEditionsPanel work={work} />,
                },
                {
                  ariaLabel: "Details",
                  label: "Details",
                  panel: <DetailsPanel work={work} />,
                },
              ]}
              isDefaultOpen
              bgColor="ui.white"
              id="item-detail-accordion"
              sx={{
                "button[aria-expanded=true]": {
                  bgColor: "ui.link.primary-05",
                },
                ".chakra-collapse": {
                  bgColor: "ui.white",
                },
              }}
            />
          </VStack>
        </VStack>
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
