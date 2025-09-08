import React from "react";
import {
    Box,
    Flex,
    Text,
    Accordion,
    Heading,
} from "@nypl/design-system-react-components";
import Link from "../Link/Link";
import { Agent, WorkEdition } from "~/src/types/DataModel";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import Ctas from "../EditionCard/Ctas";
import PublisherAndLocation from "../EditionCard/PublisherAndLocation";
import PublicDomainBadge from "./PublicDomainBadge";
import AuthorsList from "../AuthorsList/AuthorsList";
import { ApiWork } from "~/src/types/WorkQuery";
import ResearchAssistantIcon from "./ResearchAssistantIcon";

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
            isFeaturedEdition ? ` + ${work.edition_count - 1} more` : "";

        return (
            <>
                {editionDisplay} <Link to={"/"}>{additionalEditions}</Link>
            </>
        );
    };

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
                    <PublicDomainBadge />
                </Flex>
                <Flex gap="s" flexDirection="row">
                    <Box width="120px" bgColor="ui.gray.light-cool" flexShrink="0" />
                    <Box>
                        <Text size="caption" marginBottom="xxs">
                            E-BOOK
                        </Text>
                        <Heading size="heading7" noSpace marginBottom="xxs">
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
                        <Box marginTop="m">
                            <PublisherAndLocation
                                pubPlace={edition.publication_place}
                                publishers={edition.publishers}
                            />
                        </Box>
                        <Box>{editionYearElem()}</Box>
                    </Box>
                </Flex>
                <Accordion
                    width="100%"
                    id={`accordion-summary-${edition.edition_id}`}
                    accordionData={[
                        {
                            label: (
                                <Box
                                    display="flex"
                                    gap="xxs"
                                    alignItems="center"
                                    margin="0"
                                    __css={{ svg: { marginInlineStart: "0 !important" } }}
                                >
                                    <ResearchAssistantIcon />
                                    <Text noSpace>Read summary</Text>
                                </Box>
                            ),
                            panel: (
                                <Box>
                                    {edition.summary ||
                                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."}
                                    <Text
                                        size="caption"
                                        color="ui.gray.semi-dark"
                                        noSpace
                                        marginY="s"
                                    >
                                        AI-generated. Verify results.
                                    </Text>
                                </Box>
                            ),
                        },
                        {
                            label: (
                                <Box
                                    display="flex"
                                    gap="xxs"
                                    alignItems="center"
                                    margin="0"
                                    __css={{ svg: { marginInlineStart: "0 !important" } }}
                                >
                                    <ResearchAssistantIcon />
                                    <Text noSpace>Why is this result relevant?</Text>
                                </Box>
                            ),
                            panel: (
                                <Box>
                                    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed
                                    do eiusmod tempor incididunt ut labore et dolore magna aliqua.
                                    <Text
                                        size="caption"
                                        color="ui.gray.semi-dark"
                                        noSpace
                                        marginY="s"
                                    >
                                        AI-generated. Verify results.
                                    </Text>
                                </Box>
                            ),
                        },
                    ]}
                />
                <Flex flexDir="row" gap="xs">
                    <Ctas authors={authors} item={previewItem} title={work.title} />
                </Flex>
            </Flex>
        </Box>
    );
};

export default ResultCard;
