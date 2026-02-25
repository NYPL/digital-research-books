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
import { ApiWork } from "~/src/types/WorkQuery";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import AuthorsList from "../AuthorsList/AuthorsList";
import Link from "../Link/Link";
import PublicDomainBadge from "../ResearchAssistant/PublicDomainBadge";
import ResearchAssistantIcon from "../ResearchAssistant/icons/ResearchAssistantIcon";
import CardRequiredBadge from "./CardRequiredBadge";
import Ctas from "./Ctas/Ctas";
import EditionLinks from "./EditionLinks";
import FeaturedEditionBadge from "./FeaturedEditionBadge";
import PhysicalEditionBadge from "./PhysicalEditionBadge";
import ResultPublisherAndLocation from "./ResultPublisherAndLocation";

interface ResultCardProps {
  authors: Agent[];
  edition: WorkEdition;
  work: ApiWork;
  isFeaturedEdition?: boolean;
}

export const ResultCard: React.FC<ResultCardProps> = ({
  authors,
  edition,
  work,
  isFeaturedEdition,
}) => {
  const { page } = useResultPageContext();
  const previewItem = EditionCardUtils.getPreviewItem(edition.items);

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
    accordionData.push({
      label: (
        <Box
          display="flex"
          gap="xxs"
          alignItems="center"
          margin="0"
          __css={{ svg: { marginInlineStart: "0 !important" } }}
        >
          <ResearchAssistantIcon />
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
    if (page === "vra") {
      accordionData.push({
        label: (
          <Box
            display="flex"
            gap="xxs"
            alignItems="center"
            margin="0"
            __css={{ svg: { marginInlineStart: "0 !important" } }}
          >
            <ResearchAssistantIcon />
            <Text>Why is this result relevant?</Text>
          </Box>
        ),
        panel: (
          <Box>
            Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
            eiusmod tempor incididunt ut labore et dolore magna aliqua.
            <AiGeneratedText />
          </Box>
        ),
      });
    } else if (work.editions.length > 1) {
      accordionData.push({
        label: (
          <Text>
            {work.editions.length - 1} other edition
            {work.editions.length - 1 !== 1 ? "s" : ""}
          </Text>
        ),
        panel: <EditionLinks work={work} />,
      });
    }
    return accordionData;
  };

  return (
    <Box
      id={`edition-${edition.edition_id}`}
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
                  query: { featured: edition.edition_id },
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
            id={`accordion-summary-${edition.edition_id}`}
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
            editionId={edition.edition_id}
          />
        </Flex>
      </Flex>
    </Box>
  );
};

export default ResultCard;
