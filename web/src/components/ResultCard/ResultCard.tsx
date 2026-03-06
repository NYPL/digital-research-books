import {
  Accordion,
  AccordionDataProps,
  Banner,
  Box,
  Flex,
  Heading,
  Text,
} from "@nypl/design-system-react-components";
import React from "react";
import { MAX_TITLE_LENGTH } from "~/src/constants/editioncard";
import {
  RESEARCH_CATALOG_LINK,
  SCAN_AND_DELIVER_LINK,
} from "~/src/constants/links";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { Agent, WorkEdition } from "~/src/types/DataModel";
import { CatalogEdition } from "~/src/types/ResearchAssistant";
import { ApiWork } from "~/src/types/WorkQuery";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import AuthorsList from "../AuthorsList/AuthorsList";
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
  const { page } = useResultPageContext();
  const previewItem = EditionCardUtils.getPreviewItem((edition as any)?.items);

  const editionYearElem = () => {
    const editionDisplay =
      edition && edition.publication_date
        ? `${edition.publication_date} edition`
        : "Unknown edition";
    const additionalEditions =
      isFeaturedEdition && page === "vra"
        ? ` + ${work.edition_count - 1} more`
        : "";

    return (
      <>
        {editionDisplay} <Link to="/">{additionalEditions}</Link>
      </>
    );
  };

  const isPhysicalEdition = EditionCardUtils.isPhysicalEdition(previewItem);
  const isUniversityPress = EditionCardUtils.isUniversityPress(previewItem);
  const isPublicDomain = EditionCardUtils.isPublicDomain(previewItem);
  const isLoginRequired = isPhysicalEdition || isUniversityPress;

  const accordionSummaryData = () => {
    const accordionData: AccordionDataProps[] = [];
    if (
      page === "vra" &&
      (edition as any)?.snippets &&
      (edition as any)?.snippets.length > 0
    ) {
      accordionData.push({
        label: (
          <Box
            display="flex"
            gap="xxs"
            alignItems="center"
            margin="0"
            __css={{ svg: { marginInlineStart: "0 !important" } }}
          >
            <ResearchAssistantIcon inCircle />
            <Text>Why am I seeing this result?</Text>
          </Box>
        ),
        panel: (
          <>
            <Flex flexDir="column" gap="s">
              <Text>
                You&apos;re seeing this result because this book covers topics
                relevant to your request. The following sections were identified
                as matching your query.
              </Text>
              <Box
                display="flex"
                alignItems="center"
                justifyContent="space-between"
                height="1.125rem"
                __css={{ button: { padding: "xs" } }}
              >
                <AiGeneratedText />
                <FeedbackButtons />
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
    accordionData.push({
      label: (
        <Box
          display="flex"
          gap="xxs"
          alignItems="center"
          margin="0"
          __css={{ svg: { marginInlineStart: "0 !important" } }}
        >
          <ResearchAssistantIcon inCircle />
          <Text>What is this book about?</Text>
        </Box>
      ),
      panel: (
        <Box display="flex" flexDir="column" gap="xs">
          {edition.summary ||
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."}
          <AiGeneratedText />
        </Box>
      ),
    });
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

  const editionId = (edition as any)?.edition_id ?? (edition as any)?.id;

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
          <Heading size="heading7" marginBottom="xxs">
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
            <>
              By <AuthorsList authors={authors} />
            </>
          )}
          <Flex marginTop="xs" flexWrap="wrap" alignItems="center">
            <ResultPublisherAndLocation
              pubPlace={edition.publication_place}
              publishers={edition.publishers}
            />
            <Box whiteSpace="normal">{editionYearElem()}</Box>
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
