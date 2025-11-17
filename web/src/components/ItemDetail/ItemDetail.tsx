import React, { useEffect, useMemo, useState } from "react";
import { useCookies } from "react-cookie";
import { useRouter } from "next/router";
import {
  Accordion,
  Box,
  Flex,
  Grid,
  Heading,
  Text,
  Toggle,
  VStack,
} from "@nypl/design-system-react-components";
import AuthorsList from "../AuthorsList/AuthorsList";
import BackToResultsButton from "../BackToResultsButton/BackToResultsButton";
import DownloadLink from "../ResultCard/DownloadLink";
import ResearchAssistantIcon from "../ResearchAssistant/ResearchAssistantIcon";
import ResearchAssistantPanel from "../ResearchAssistant/ResearchAssistantPanel";
import ResearchAssistantViewer from "../ResearchAssistant/ResearchAssistantViewer";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { ApiWork, WorkResult } from "~/src/types/WorkQuery";
import { MessageStatus, MessageType } from "~/src/types/ResearchAssistant";
import AboutItemPanel from "./AboutItemPanel";
import SummaryPanel from "./SummaryPanel";
import SearchPanel from "./SearchPanel";
import DownloadOptionsPanel from "./DownloadOptionsPanel";
import DetailsPanel from "./DetailsPanel";
import OtherEditionsPanel from "./OtherEditionsPanel";

interface ItemDetailProps {
  workResult: WorkResult;
  backUrl?: string;
}

const ItemDetail: React.FC<ItemDetailProps> = ({ workResult, backUrl }) => {
  const [vraEnabled, setVraEnabled] = useState(true);
  const [hasPreviewLoaded, setHasPreviewLoaded] = useState(false);
  const [shouldNavigate, setShouldNavigate] = useState(false);

  const {
    clearHistory,
    isLoading,
    messages,
    sendMessage,
    setMessages,
    itemId,
    pageId,
    handlePreview,
    goToPreviousState,
    setViewState,
    historyStack,
    setHistoryStack,
    error,
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

      clearHistory();
      setViewState((prev) => ({
        ...prev,
        itemId,
        pageId,
        showWebReader: false,
        results: null,
        linkResults: null,
        pdfData: null,
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

  const gridColumns = vraEnabled
    ? { base: "1fr", md: "25% 50% 25%" }
    : { base: "1fr", md: "33.33% 66.67%" };
  const gridRows = backUrl ? "auto 1fr" : "1fr";
  const gridPaddingX = { base: "1rem", md: "1.5rem", xl: "1rem" };

  const outerMarginCalc = "calc((100vw - 1280px) / 2)";
  const headerMarginLeft = {
    base: `calc(${outerMarginCalc} * -1 + ${gridPaddingX.base})`,
    md: `calc(${outerMarginCalc} * -1 + ${gridPaddingX.md})`,
    xl: `calc(${outerMarginCalc} * -1 + ${gridPaddingX.xl})`,
  };
  const headerMarginRight = vraEnabled ? "0" : headerMarginLeft;

  const handleBackToResults = () => {
    const storedHistory = sessionStorage.getItem("vraHistoryStack");
    const storedMessages = sessionStorage.getItem("vraMessages");
    if (storedHistory) setHistoryStack(JSON.parse(storedHistory));
    if (storedMessages) setMessages(JSON.parse(storedMessages));
    sessionStorage.removeItem("vraHistoryStack");
    sessionStorage.removeItem("vraMessages");
    setShouldNavigate(true);
  };

  useEffect(() => {
    if (shouldNavigate) {
      goToPreviousState();
      router.push("/research-assistant");
      setShouldNavigate(false);
    }
  }, [shouldNavigate, router]);

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
        templateColumns={gridColumns}
        templateRows={gridRows}
        gap="l"
        marginBottom="xs"
        maxWidth="1280px"
        margin="0 auto"
        width="100%"
      >
        {backUrl && (
          <Flex
            alignItems="center"
            justifyContent="space-between"
            padding="s"
            bgColor="ui.white"
            gridColumn="1 / span 2"
            gridRow="1"
            borderBottom="1px solid"
            borderColor="ui.border.default"
            marginLeft={headerMarginLeft}
            marginRight={headerMarginRight}
            paddingLeft={outerMarginCalc}
            paddingRight={{
              base: vraEnabled
                ? gridPaddingX.base
                : `calc(${outerMarginCalc} + ${gridPaddingX.base} * 2)`,
              md: vraEnabled
                ? gridPaddingX.md
                : `calc(${outerMarginCalc} + ${gridPaddingX.md} * 2)`,
              xl: vraEnabled
                ? gridPaddingX.xl
                : `calc(${outerMarginCalc} + ${gridPaddingX.xl} * 2)`,
            }}
            paddingY="s"
          >
            <BackToResultsButton handleBackToResults={handleBackToResults} />
            {page === "vra" && (
              <Toggle
                isChecked={vraEnabled}
                labelText="Use Virtual Research Assistant"
                onChange={() => setVraEnabled((prev) => !prev)}
              />
            )}
          </Flex>
        )}
        <VStack
          alignContent="left"
          alignItems="left"
          bgColor="ui.bg.default"
          gridColumn="1"
          gridRow={backUrl ? "2" : "1"}
          marginTop="2rem"
          paddingBottom="l"
          paddingLeft={gridPaddingX}
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
            <DownloadLink
              authors={authorNames}
              downloadLink={downloadLink}
              title={work.title}
              isLoggedIn={isLoggedIn}
            />
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
                      gap="xxs"
                      __css={{ svg: { marginInlineStart: "0 !important" } }}
                    >
                      <ResearchAssistantIcon />
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
            />
          </VStack>
        </VStack>
        <Box
          gridColumn="2"
          gridRow={backUrl ? "2" : "1"}
          marginRight="2rem"
          marginTop="2rem"
          paddingBottom="l"
          paddingRight={vraEnabled ? "0" : gridPaddingX}
        >
          <ResearchAssistantViewer itemId={itemId} pageId={pageId} />
        </Box>
        <Box
          gridColumn="3"
          gridRow={backUrl ? "1 / span 2" : "1"}
          height="100%"
          marginLeft="-2rem"
          marginRight={{
            base: `calc(-1 * (100vw - 1280px)/2 + ${gridPaddingX.base} * 2)`,
            md: `calc(-1 * (100vw - 1280px)/2 + ${gridPaddingX.md} * 2)`,
            xl: `calc(-1 * (100vw - 1280px)/2 + ${gridPaddingX.xl} * 2)`,
          }}
          paddingRight={gridPaddingX}
          display="flex"
          flexDirection="column"
        >
          {vraEnabled && (
            <ResearchAssistantPanel
              messages={messages}
              isLoading={isLoading}
              error={error}
              onSendMessage={sendMessage}
              clearHistory={clearHistory}
            />
          )}
        </Box>
      </Grid>
    </Box>
  );
};

export default ItemDetail;
