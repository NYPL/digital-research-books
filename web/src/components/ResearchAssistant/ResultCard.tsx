import React from "react";
import {
  Card,
  CardContent,
  CardHeading,
  Box,
  CardActions,
  Flex,
} from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import { Agent, WorkEdition } from "~/src/types/DataModel";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { PLACEHOLDER_COVER_LINK } from "~/src/constants/editioncard";
import CardRequiredBadge from "../EditionCard/CardRequiredBadge";
import CopyrightLink from "../EditionCard/CopyrightLink";
import Ctas from "../EditionCard/Ctas";
import FeaturedEditionBadge from "../EditionCard/FeaturedEditionBadge";
import LanguageDisplayText from "../EditionCard/LanguageDisplayText";
import PhysicalEditionBadge from "../EditionCard/PhysicalEditionBadge";
import PublisherAndLocation from "../EditionCard/PublisherAndLocation";
import ScanAndDeliverBlurb from "../EditionCard/ScanAndDeliverBlurb";
import UpBlurb from "../EditionCard/UpBlurb";

export const ResultCard: React.FC<{
  authors: Agent[];
  edition: WorkEdition;
  title: string;
  isFeaturedEdition?: boolean;
}> = ({ authors, edition, title, isFeaturedEdition }) => {
  const previewItem = EditionCardUtils.getPreviewItem(edition.items);

  const editionYearElem = (edition: WorkEdition) => {
    const editionDisplay = EditionCardUtils.editionYearText(edition);

    const editionElem = edition ? (
      <Link
        to={{
          pathname: `/edition/${edition.edition_id}`,
          ...(previewItem
            ? { query: { featured: previewItem.item_id } }
            : null),
        }}
      >
        {editionDisplay}
      </Link>
    ) : (
      <>{editionDisplay}</>
    );
    return editionElem;
  };

  const coverUrl = EditionCardUtils.getCover(edition.links);
  const isPhysicalEdition = EditionCardUtils.isPhysicalEdition(previewItem);
  const isUniversityPress = EditionCardUtils.isUniversityPress(previewItem);
  const isLoginRequired = isPhysicalEdition || isUniversityPress;

  return (
    <Box
      border="1px"
      borderColor="ui.border.default"
      padding="s"
      paddingLeft={{ base: "l", md: null }}
      paddingBottom="l"
      paddingRight="l"
    >
      <Flex gap="xs" flexDirection={{ base: "column", lg: "row" }}>
        {isLoginRequired && <CardRequiredBadge />}
        {isFeaturedEdition && <FeaturedEditionBadge />}
      </Flex>
      <Card
        id={`card-${edition.edition_id}`}
        layout="column"
        imageProps={{
          src: coverUrl,
          alt:
            coverUrl === PLACEHOLDER_COVER_LINK
              ? "Placeholder Cover"
              : `Cover for ${EditionCardUtils.editionYearText(edition)}`,
          size: "xsmall",
          aspectRatio: "original",
        }}
        paddingTop="m"
        flexFlow={{ md: "column nowrap", lg: "row" }}
        justifyContent={{ md: "center", lg: "left" }}
        sx={{
          ".card-right": {
            maxWidth: { base: "100%", lg: "200px" },
            marginStart: { base: "0", lg: "m" },
            marginTop: { base: "xs", lg: 0 },
          },
        }}
      >
        <CardHeading
          level="h3"
          size="heading6"
          id="stack1-heading1"
          sx={{
            span: {
              fontSize: "18px",
              a: {
                textDecoration: "none",
              },
            },
          }}
        >
          <Flex alignItems="center" gap="xs">
            <span>{editionYearElem(edition)}</span>
            {isPhysicalEdition && <PhysicalEditionBadge />}
          </Flex>
        </CardHeading>
        <CardContent>
          <Box>
            <PublisherAndLocation
              pubPlace={edition.publication_place}
              publishers={edition.publishers}
            />
            <LanguageDisplayText edition={edition} />
            <CopyrightLink rights={previewItem?.rights} />
          </Box>
          {isPhysicalEdition && <ScanAndDeliverBlurb />}
          {isUniversityPress && <UpBlurb publishers={edition.publishers} />}
        </CardContent>
        <CardActions
          display="flex"
          flexDir="column"
          whiteSpace="nowrap"
          sx={{
            width: { base: "100%", lg: "200px" },
          }}
          gap="xs"
        >
          <Ctas authors={authors} item={previewItem} title={title} />
        </CardActions>
      </Card>
    </Box>
  );
};

export default ResultCard;
