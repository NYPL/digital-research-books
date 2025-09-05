import React from "react";
import {
    Box,
    Flex,
    Text,
    Accordion,
    Image,
    Heading,
} from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import { Agent, WorkEdition } from "~/src/types/DataModel";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import { PLACEHOLDER_COVER_LINK } from "~/src/constants/editioncard";
import CardRequiredBadge from "../EditionCard/CardRequiredBadge";
import Ctas from "../EditionCard/Ctas";
import FeaturedEditionBadge from "../EditionCard/FeaturedEditionBadge";
import PublisherAndLocation from "../EditionCard/PublisherAndLocation";
import ScanAndDeliverBlurb from "../EditionCard/ScanAndDeliverBlurb";
import UpBlurb from "../EditionCard/UpBlurb";
import PublicDomainBadge from "./PublicDomainBadge";
import AuthorsList from "../AuthorsList/AuthorsList";
import { ApiWork } from "~/src/types/WorkQuery";

export const ResultCard: React.FC<{
    authors: Agent[];
    edition: WorkEdition;
    work: ApiWork;
    isFeaturedEdition?: boolean;
}> = ({ authors, edition, work, isFeaturedEdition }) => {
    const previewItem = EditionCardUtils.getPreviewItem(edition.items);

    const editionYearElem = () => {
        const editionDisplay = EditionCardUtils.editionYearText(edition);
        const additionalEditions =
            work.edition_count > 1 ? ` + ${work.edition_count - 1} more` : "";

        return (
            <>
                {editionDisplay} <Link to={"/"}>{additionalEditions}</Link>
            </>
        );
    };

    const coverUrl = EditionCardUtils.getCover(edition.links);
    const isPhysicalEdition = EditionCardUtils.isPhysicalEdition(previewItem);
    const isUniversityPress = EditionCardUtils.isUniversityPress(previewItem);
    const isLoginRequired = isPhysicalEdition || isUniversityPress;

    return (
        <Box
            border="1px solid"
            borderColor="ui.border.default"
            padding="s"
            backgroundColor="ui.white"
            borderTop="2px solid"
            borderTopColor="section.research.primary"
        >
            <Flex gap="s" flexDirection="column">
                <Flex
                    gap="xs"
                    flexDirection="row"
                    alignItems="center"
                    marginBottom="xs"
                >
                    {isLoginRequired && <CardRequiredBadge />}
                    {isFeaturedEdition && <FeaturedEditionBadge />}
                    <PublicDomainBadge />
                </Flex>
                <Flex gap="s" flexDirection="row">
                    <Image
                        src={coverUrl}
                        alt={
                            coverUrl === PLACEHOLDER_COVER_LINK
                                ? "Placeholder Cover"
                                : `Cover for ${EditionCardUtils.editionYearText(edition)}`
                        }
                        size="xsmall"
                        aspectRatio="original"
                    />
                    <Box>
                        <Text size="caption" marginBottom="xxs">
                            E-BOOK
                        </Text>
                        <Heading size="heading7">
                            <Link
                                to={{
                                    pathname: `/work/${edition.edition_id}`,
                                    ...(previewItem
                                        ? { query: { featured: previewItem.item_id } }
                                        : null),
                                }}
                                isUnderlined={false}
                            >
                                {work.title}
                            </Link>
                        </Heading>
                        {authors.length > 0 && (
                            <Box>
                                By <AuthorsList authors={authors} />
                            </Box>
                        )}
                        <Box>
                            <PublisherAndLocation
                                pubPlace={edition.publication_place}
                                publishers={edition.publishers}
                            />
                        </Box>
                        <Box>{editionYearElem()}</Box>
                        {isPhysicalEdition && <ScanAndDeliverBlurb />}
                        {isUniversityPress && <UpBlurb publishers={edition.publishers} />}
                    </Box>
                </Flex>
                <Accordion
                    width="100%"
                    id={`accordion-summary-${edition.edition_id}`}
                    accordionData={[
                        {
                            label: "Read summary",
                            panel: <Text>{edition.summary || "No summary available."}</Text>,
                        },
                    ]}
                />
                <Flex
                    flexDir="row"
                    gap="xs"
                >
                    <Ctas authors={authors} item={previewItem} title={work.title} />
                </Flex>
            </Flex>
        </Box>
    );
};

export default ResultCard;
