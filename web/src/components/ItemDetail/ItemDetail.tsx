import {
  Accordion,
  Box,
  Flex,
  Grid,
  GridItem,
  Heading,
  Text,
  Toggle,
  Tooltip,
  VStack,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useEffect, useMemo, useState } from "react";
import { useCookies } from "react-cookie";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { ACCORDION_EXPANDED_BG } from "~/src/constants/colors";
import {
  getGridColumns,
  getGridRows,
  getHeaderPaddingRight,
  GRID_PADDING_X,
  HEADER_HEIGHT,
} from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { HistoryItem } from "~/src/types/ResearchAssistant";
import { ApiWork, WorkResult } from "~/src/types/WorkQuery";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import AuthorsList from "../AuthorsList/AuthorsList";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import Link from "../Link/Link";
import ResearchAssistantPanel from "../ResearchAssistant/ResearchAssistantPanel";
import ResearchAssistantViewer from "../ResearchAssistant/ResearchAssistantViewer";
import AccordionLabelWithIcon from "./AccordionLabelWithIcon";
import AboutItemPanel from "./panels/AboutItemPanel";
import DetailsPanel from "./panels/DetailsPanel";
import DownloadOptionsPanel from "./panels/DownloadOptionsPanel";
import OtherEditionsPanel from "./panels/OtherEditionsPanel";
import RelatedBooksPanel from "./panels/RelatedBooksPanel";
import SearchPanel from "./panels/SearchPanel";
import SummaryPanel from "./panels/SummaryPanel";

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

  const previewLink = useMemo(
    () => EditionCardUtils.getReadOnlineLink(previewItem),
    [previewItem]
  );

  useEffect(() => {
    if (previewLink && !hasPreviewLoaded) {
      handlePreview(previewLink.url);
      setHasPreviewLoaded(true);
    }
  }, [previewLink, handlePreview, hasPreviewLoaded]);

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
        editionId: previewEdition?.edition_id,
        showWebReader: true,
        results: null,
        linkResults: null,
      }));

      setMessages([]);
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

  const publisherNames = (previewEdition?.publishers ?? []).map(
    (pubAgent) => pubAgent && pubAgent.name
  );

  const downloadLink = previewEdition
    ? EditionCardUtils.selectDownloadLink(previewEdition)
    : undefined;
  const authorNames = work.authors
    ? work.authors.map((author) => author.name)
    : [];
  const [cookies] = useCookies([NYPL_SESSION_ID]);
  const loginCookie = cookies[NYPL_SESSION_ID];
  const isLoggedIn = !!loginCookie;

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
          <Flex flexDir="column" gap="xxs">
            <Text size="caption">E-BOOK</Text>
            <Heading level="h1" size="heading6">
              {work.title}
            </Heading>
          </Flex>
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
                  ariaLabel: "About this book",
                  label: "About this book",
                  panel: (
                    <AboutItemPanel
                      previewItem={previewItem}
                      previewEdition={previewEdition}
                      publisherNames={publisherNames}
                    />
                  ),
                },
                {
                  ariaLabel: "What is this book about?",
                  label: (
                    <AccordionLabelWithIcon text="What is this book about?" />
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
                  ariaLabel: "Search inside this book",
                  label: "Search inside this book",
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
                {
                  ariaLabel: "Related books",
                  label: <AccordionLabelWithIcon text="Related books" />,
                  panel: <RelatedBooksPanel />,
                },
              ]}
              isDefaultOpen
              bgColor="ui.white"
              id="item-detail-accordion"
              sx={{
                "button[aria-expanded=true]": {
                  bgColor: ACCORDION_EXPANDED_BG,
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
