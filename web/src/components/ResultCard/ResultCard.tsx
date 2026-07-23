import {
  Accordion,
  AccordionDataProps,
  Banner,
  Box,
  Button,
  Flex,
  Heading,
  SkeletonLoader,
  Text,
} from "@nypl/design-system-react-components";
import React from "react";
import { MAX_TITLE_LENGTH } from "~/src/constants/editioncard";
import {
  RESEARCH_CATALOG_LINK,
  SCAN_AND_DELIVER_LINK,
} from "~/src/constants/links";
import {
  RESULT_AUTHOR_TEST_ID,
  RESULT_EDITION_TEST_ID,
  RESULT_TITLE_TEST_ID,
} from "~/src/constants/testIds";
import { FeedbackContext } from "~/src/context/FeedbackContext";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { Agent, WorkEdition } from "~/src/types/DataModel";
import { CatalogEdition } from "~/src/types/ResearchAssistant";
import { ApiWork } from "~/src/types/WorkQuery";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { renderMarkdownContent } from "~/src/util/MarkdownParser";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import AuthorsList from "../AuthorsList/AuthorsList";
import HiddenAria from "../HiddenAria/HiddenAria";
import Link from "../Link/Link";
import FeedbackButtons from "../ResearchAssistant/FeedbackButtons";
import PublicDomainBadge from "../ResearchAssistant/PublicDomainBadge";
import ResearchAssistantIcon from "../ResearchAssistant/icons/ResearchAssistantIcon";
import CardRequiredBadge from "./CardRequiredBadge";
import Ctas from "./Ctas/Ctas";
import EditionLinks from "./EditionLinks";
import FeaturedEditionBadge from "./FeaturedEditionBadge";
import PhysicalEditionBadge from "./PhysicalEditionBadge";
import RelevantSections from "./RelevantSections";
import ResultPublisherAndLocation from "./ResultPublisherAndLocation";

interface ResultCardProps {
  authors: Agent[];
  edition: WorkEdition | CatalogEdition;
  work:
    | ApiWork
    | {
        uuid?: string;
        title?: string;
        editions?: CatalogEdition[];
        edition_count?: number;
      };
  isFeaturedEdition?: boolean;
}

export const ResultCard: React.FC<ResultCardProps> = ({
  authors,
  edition,
  work,
  isFeaturedEdition,
}) => {
  const feedbackContext = React.useContext(FeedbackContext);
  const { page } = useResultPageContext();
  const isEnhancedSearch = page !== "drb" && page !== "keyword";
  const snippetsLength = (edition as any)?.snippets?.length ?? 0;
  const hasResultReasonAccordion = page === "vra" && snippetsLength > 0;
  const editionId = (edition as any)?.edition_id ?? (edition as any)?.id;
  const resultReasonCallId = (edition as any)?.call_id;

  const accordionRef = React.useRef<HTMLButtonElement | null>(null);
  const [resultReasonText, setResultReasonText] = React.useState<string | null>(
    null
  );
  const [isResultReasonLoading, setIsResultReasonLoading] = React.useState(
    false
  );
  const [hasResultReasonError, setHasResultReasonError] = React.useState(false);
  const isResultReasonLoadingRef = React.useRef(isResultReasonLoading);
  const resultReasonTextRef = React.useRef(resultReasonText);

  React.useEffect(() => {
    isResultReasonLoadingRef.current = isResultReasonLoading;
  }, [isResultReasonLoading]);

  React.useEffect(() => {
    resultReasonTextRef.current = resultReasonText;
  }, [resultReasonText]);

  const openFeedbackBox = (e: React.SyntheticEvent) => {
    e.preventDefault();
    e.stopPropagation();
    feedbackContext?.onOpen();
  };

  const renderResultReasonError = () => {
    if (!hasResultReasonError) return null;

    return (
      <Text>
        Content could not be generated at this time. Try again later or{" "}
        <Button
          variant="text"
          onClick={openFeedbackBox}
          sx={{
            display: "inline !important",
            padding: "0 !important",
            height: "auto !important",
            minHeight: "auto !important",
            maxHeight: "none !important",
            lineHeight: "inherit !important",
            borderRadius: "0 !important",
            "&:hover": {
              backgroundColor: "transparent !important",
            },
            "&:active": {
              backgroundColor: "transparent !important",
            },
          }}
        >
          contact us
        </Button>{" "}
        for assistance
      </Text>
    );
  };
  const previewItem = EditionCardUtils.getPreviewItem(
    (edition as any)?.items,
    isEnhancedSearch
  );

  const editionYearElem = () => {
    const editionDisplay =
      edition && edition.publication_date
        ? `${edition.publication_date} edition`
        : "Unknown edition";

    return editionDisplay;
  };

  const fetchResultsReason = async (callId: string, editionId: number) => {
    const response = await fetch("/api/result-reason", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ call_id: callId, edition_id: editionId }),
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || "Result reasons request failed.");
    }

    return response.json();
  };

  React.useEffect(() => {
    if (!hasResultReasonAccordion) return;

    const accordionNode = accordionRef.current;
    if (!accordionNode) return;

    const onAccordionClick = async () => {
      const isOpening = accordionNode.getAttribute("aria-expanded") !== "true";
      const parsedEditionId = Number(editionId);
      const canFetchReason =
        isOpening &&
        !isResultReasonLoadingRef.current &&
        !resultReasonTextRef.current &&
        Boolean(resultReasonCallId) &&
        editionId !== undefined &&
        !Number.isNaN(parsedEditionId);

      if (!canFetchReason) return;

      try {
        setIsResultReasonLoading(true);
        setHasResultReasonError(false);

        const result = await fetchResultsReason(
          resultReasonCallId,
          parsedEditionId
        );
        setResultReasonText(result?.explanation ?? null);
      } catch (error) {
        console.error("Result reason fetch failed: ", error);
        setHasResultReasonError(true);
      } finally {
        setIsResultReasonLoading(false);
      }
    };

    accordionNode.addEventListener("click", onAccordionClick);

    return () => {
      accordionNode.removeEventListener("click", onAccordionClick);
    };
  }, [editionId, hasResultReasonAccordion, resultReasonCallId]);

  const isPhysicalEdition = EditionCardUtils.isPhysicalEdition(previewItem);
  const isUniversityPress = EditionCardUtils.isUniversityPress(previewItem);
  const isPublicDomain = EditionCardUtils.isPublicDomain(previewItem);
  const isLoginRequired = isPhysicalEdition || isUniversityPress;

  const accordionSummaryData = () => {
    const accordionData: AccordionDataProps[] = [];
    if (hasResultReasonAccordion) {
      accordionData.push({
        buttonInteractionRef: accordionRef,
        label: (
          <Box
            display="flex"
            gap="xxs"
            alignItems="center"
            margin="0"
            __css={{ svg: { marginInlineStart: "0 !important" } }}
          >
            <ResearchAssistantIcon inCircle />
            <HiddenAria ariaLive="off" ariaAtomic={false}>
              AI Generated:
            </HiddenAria>
            <Text>Why am I seeing this result?</Text>
          </Box>
        ),
        panel: (
          <>
            <Flex flexDir="column" gap="s">
              {resultReasonText && renderMarkdownContent(resultReasonText)}
              {isResultReasonLoading && (
                <SkeletonLoader
                  showImage={false}
                  showHeading={false}
                  contentSize={5}
                  sx={{
                    div: { marginTop: 0 },
                  }}
                />
              )}
              {renderResultReasonError()}
              <Box
                display="flex"
                alignItems="center"
                justifyContent="space-between"
                height="1.125rem"
                __css={{ button: { padding: "xs" } }}
              >
                <AiGeneratedText />
                <FeedbackButtons label="relevance feedback" />
              </Box>
            </Flex>
            <RelevantSections
              snippets={(edition as any)?.snippets}
              workId={work.uuid}
            />
          </>
        ),
      });
    }
    // accordionData.push({
    //   label: (
    //     <Box
    //       display="flex"
    //       gap="xxs"
    //       alignItems="center"
    //       margin="0"
    //       __css={{ svg: { marginInlineStart: "0 !important" } }}
    //     >
    //       <ResearchAssistantIcon inCircle />
    //       <HiddenAria ariaLive="off" ariaAtomic={false}>
    //         AI Generated:
    //       </HiddenAria>
    //       <Text>What is this book about?</Text>
    //     </Box>
    //   ),
    //   panel: (
    //     <Box display="flex" flexDir="column" gap="xs">
    //       {edition.summary ||
    //         "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."}
    //       <Box
    //         display="flex"
    //         alignItems="center"
    //         justifyContent="space-between"
    //         height="1.125rem"
    //         __css={{ button: { padding: "xs" } }}
    //       >
    //         <AiGeneratedText />
    //         <FeedbackButtons label="summary feedback" />
    //       </Box>
    //     </Box>
    //   ),
    // });
    if (work.editions.length > 1) {
      accordionData.push({
        label: (
          <Text>
            {work.editions.length - 1} other edition
            {work.editions.length - 1 !== 1 ? "s" : ""}
          </Text>
        ),
        panel: <EditionLinks work={work as ApiWork} />,
      });
    }
    return accordionData;
  };

  return (
    <Box
      id={`edition-${editionId}`}
      border="1px solid"
      borderColor="ui.border.default"
      padding="s"
      backgroundColor="ui.white"
      borderTop="2px solid"
      borderTopColor="section.research.primary"
      fontSize="desktop.body.body2"
    >
      <Flex gap="s" flexDirection="column">
        <Flex gap="xs" flexDirection="row" alignItems="center">
          {isPublicDomain && <PublicDomainBadge />}
          {isFeaturedEdition && <FeaturedEditionBadge />}
          {isPhysicalEdition && <PhysicalEditionBadge />}
          {isLoginRequired && <CardRequiredBadge />}
        </Flex>
        <Box>
          <Heading
            size="heading7"
            marginBottom="xxs"
            data-testid={RESULT_TITLE_TEST_ID}
          >
            <Link
              to={{
                pathname: `/${page === "vra" ? "item" : "work"}/${work.uuid}`,
                ...(previewItem && {
                  query: { featured: editionId },
                }),
              }}
              isUnderlined={false}
            >
              {truncateStringOnWhitespace(work.title, MAX_TITLE_LENGTH)}
            </Link>
          </Heading>
          {authors.length > 0 && (
            <span data-testid={RESULT_AUTHOR_TEST_ID}>
              By <AuthorsList authors={authors} />
            </span>
          )}
          <Flex marginTop="xs" flexWrap="wrap" alignItems="center">
            <ResultPublisherAndLocation
              pubPlace={edition.publication_place}
              publishers={edition.publishers}
            />
            <Box whiteSpace="normal" data-testid={RESULT_EDITION_TEST_ID}>
              {editionYearElem()}
            </Box>
          </Flex>
        </Box>
        {!isPhysicalEdition && (
          <Accordion
            width="100%"
            id={`accordion-summary-${editionId}`}
            accordionData={accordionSummaryData()}
            sx={{
              span: {
                fontSize: "desktop.body.body2",
              },
              button: {
                paddingX: "s",
                paddingY: "xs",
              },
              "button[aria-expanded='true'], button[aria-expanded='true']:hover": {
                backgroundColor: "section.research.primary-05",
              },
              "button[aria-expanded='true'] div": {
                backgroundColor: "transparent",
              },
            }}
          />
        )}
        {isPhysicalEdition && (
          <Banner
            content={
              <Text>
                This is a physical edition from our{" "}
                <Link to={RESEARCH_CATALOG_LINK} hasVisitedState={false}>
                  Research Catalog
                </Link>
                . A partial scan can be requested via NYPL&apos;s{" "}
                <Link to={SCAN_AND_DELIVER_LINK} hasVisitedState={false}>
                  Scan & Deliver service
                </Link>
                .
              </Text>
            }
            sx={{
              a: {
                color: "ui.link.primary",
              },
            }}
          />
        )}
        <Flex flexDir="row" gap="xs">
          <Ctas
            authors={authors}
            item={previewItem}
            title={work.title}
            workId={work.uuid}
            editionId={editionId}
          />
        </Flex>
      </Flex>
    </Box>
  );
};

export default ResultCard;
