import React, { useEffect, useMemo, useState } from "react";
import { useCookies } from "react-cookie";
import {
  Accordion,
  Box,
  Flex,
  Grid,
  Heading,
  Radio,
  RadioGroup,
  SearchBar,
  Text,
  Toggle,
  VStack,
} from "@nypl/design-system-react-components";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import AuthorsList from "../AuthorsList/AuthorsList";
import DownloadLink from "../ResultCard/DownloadLink";
import EditionLinks from "../ResultCard/EditionLinks";
import Link from "../Link/Link";
import ResearchAssistantPanel from "../ResearchAssistant/ResearchAssistantPanel";
import ResearchAssistantViewer from "../ResearchAssistant/ResearchAssistantViewer";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { ApiWork, WorkResult } from "~/src/types/WorkQuery";
import ResearchAssistantIcon from "../ResearchAssistant/ResearchAssistantIcon";

interface ItemDetailProps {
  workResult: WorkResult;
  backUrl?: string;
}

const ItemDetail: React.FC<ItemDetailProps> = ({ workResult, backUrl }) => {
  const [vraEnabled, setVraEnabled] = useState(true);
  const [hasPreviewLoaded, setHasPreviewLoaded] = useState(false);

  const {
    clearHistory,
    isLoading,
    messages,
    sendMessage,
    itemId,
    pageId,
    handlePreview,
    error,
  } = useResearchAssistant();

  const { page } = useResultPageContext();

  const work: ApiWork = workResult.data;

  const previewEdition = work.editions && work.editions[0];
  const previewItem = EditionCardUtils.getPreviewItem(previewEdition.items);
  const previewLink = useMemo(
    () => EditionCardUtils.getReadOnlineLink(previewItem),
    [previewItem]
  );

  useEffect(() => {
    setHasPreviewLoaded(false);
  }, [previewLink?.url]);

  useEffect(() => {
    if (previewLink && !hasPreviewLoaded) {
      handlePreview(previewLink.url);
      setHasPreviewLoaded(true);
    }
  }, [previewLink, handlePreview, hasPreviewLoaded]);

  const gridColumns = vraEnabled
    ? { base: "1fr", md: "25% 50% 25%" }
    : { base: "1fr", md: "33.33% 66.67%" };
  const gridRows = backUrl ? "auto 1fr" : "1fr";
  const gridPaddingX = { base: "1rem", md: "1.5rem", xl: "1rem" };

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

  const onDownloadOptionChange = (): void => {
    throw new Error("Function not implemented.");
  };

  const aboutThisItemPanel = (
    <VStack alignItems="left" gap="xs">
      <Box>
        <Text fontWeight="bold">Copyright</Text>
        <Link to="/copyright" isUnderlined={false}>
          {previewItem && previewItem.rights && previewItem.rights.length > 0
            ? `${previewItem.rights[0].rightsStatement}`
            : "Unknown"}
        </Link>
      </Box>
      <Box>
        <Text fontWeight="bold">Edition</Text>
        <Text>{previewEdition.publication_date || "Unknown date"}</Text>
      </Box>
      <Box>
        <Text fontWeight="bold">Publisher</Text>
        <Text>{publisherNames.join(", ") || "Publisher unknown"}</Text>
      </Box>
      <Box>
        <Text fontWeight="bold">Place of publication</Text>
        <Text>
          {previewEdition.publication_place || "Place of publication unknown"}
        </Text>
      </Box>
    </VStack>
  );

  const summaryPanel = (
    <VStack alignItems="left" gap="xs">
      {previewEdition.summary || "No summary available."}
      <AiGeneratedText />
    </VStack>
  );

  const downloadOptionsPanel = (
    <Box>
      <RadioGroup
        defaultValue="full"
        labelText="Range"
        onChange={onDownloadOptionChange}
        name="downloadOptionRange"
      >
        <Radio labelText="Entire e-book" value="full" />
        <Radio labelText="Current page" value="page" />
      </RadioGroup>
      <DownloadLink
        authors={authorNames}
        downloadLink={downloadLink}
        title={work.title}
        isLoggedIn={isLoggedIn}
      />
    </Box>
  );

  const searchPanel = (
    <Box>
      {/* TODO: Implement search functionality */}
      <SearchBar
        labelText="Search inside this item"
        textInputProps={{
          isClearable: true,
          labelText: "Item Search",
          name: "textInputName",
          placeholder: "Enter keywords",
        }}
        sx={{
          "button[type='submit']": {
            bgColor: "section.research.secondary", // TODO: update hover state colors
          },
        }}
      />
    </Box>
  );

  const otherEditionsPanel = (
    <Box>
      {work.editions.length > 1 ? (
        <EditionLinks work={work} />
      ) : (
        <Text>No other editions available.</Text>
      )}
    </Box>
  );

  const detailsPanel = (
    <VStack alignItems="left" gap="xs">
      <Box>
        <Text fontWeight="bold">Authors</Text>
        <AuthorsList authors={work.authors} />
      </Box>
      <Box>
        <Text fontWeight="bold">Subjects</Text>
        {work.subjects && work.subjects.length > 0 ? (
          <VStack alignItems="left" gap="xxs">
            {work.subjects
              .filter((subject) => subject.heading)
              .map((subject, i) => (
                <Link
                  key={`subject-link-${i}`}
                  to={{
                    pathname: "/keyword-search",
                    query: { query: `subject:${subject.heading}` },
                  }}
                  isUnderlined={false}
                >
                  {subject.heading}
                </Link>
              ))}
          </VStack>
        ) : (
          <Text>Unknown subjects</Text>
        )}
      </Box>
      <Box>
        <Text fontWeight="bold">Languages</Text>
        <Text>{work.languages.join(", ") || "Unknown languages"}</Text>
      </Box>
    </VStack>
  );

  return (
    <Box fontSize="desktop.body.body2" bgColor="ui.bg.default" width="100%">
      <Grid
        templateColumns={gridColumns}
        templateRows={gridRows}
        gap="l"
        marginBottom="xs"
        maxWidth="1280px"
        margin="0 auto"
        paddingX={gridPaddingX}
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
          >
            <Link
              to={backUrl}
              variant="backwards"
              color="section.research.secondary"
              hasVisitedState={false}
            >
              Back to results
            </Link>
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
                  panel: aboutThisItemPanel,
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
                  panel: summaryPanel,
                },
                {
                  ariaLabel: "Download options",
                  label: "Download options",
                  panel: downloadOptionsPanel,
                },
                {
                  ariaLabel: "Search inside this item",
                  label: "Search inside this item",
                  panel: searchPanel,
                },
                {
                  ariaLabel: "Other editions",
                  label: "Other editions",
                  panel: otherEditionsPanel,
                },
                {
                  ariaLabel: "Details",
                  label: "Details",
                  panel: detailsPanel,
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
        >
          <ResearchAssistantViewer itemId={itemId} pageId={pageId} />
        </Box>
        <Box
          gridColumn="3"
          gridRow={backUrl ? "1 / span 2" : "1"}
          height="100%"
          marginLeft="-2rem"
          marginRight={{
            base: `calc(-1 * (100vw - 1280px)/2 + ${gridPaddingX.base})`,
            md: `calc(-1 * (100vw - 1280px)/2 + ${gridPaddingX.md})`,
            xl: `calc(-1 * (100vw - 1280px)/2 + ${gridPaddingX.xl})`,
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
