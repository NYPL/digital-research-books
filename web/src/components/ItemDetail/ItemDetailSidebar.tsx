import {
  Accordion,
  Box,
  Flex,
  Heading,
  Text,
  VStack,
} from "@nypl/design-system-react-components";
import React, { useMemo } from "react";
import { useCookies } from "react-cookie";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { ACCORDION_EXPANDED_BG } from "~/src/constants/colors";
import { GRID_PADDING_X } from "~/src/constants/researchAssistant";
import { ApiWork } from "~/src/types/WorkQuery";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import AuthorsList from "../AuthorsList/AuthorsList";
import Link from "../Link/Link";
import AccordionLabelWithIcon from "./AccordionLabelWithIcon";
import DetailsPanel from "./panels/DetailsPanel";
import DownloadOptionsPanel from "./panels/DownloadOptionsPanel";
import OtherEditionsPanel from "./panels/OtherEditionsPanel";
import RelatedBooksPanel from "./panels/RelatedBooksPanel";
import SearchPanel from "./panels/SearchPanel";
import SummaryPanel from "./panels/SummaryPanel";

interface ItemDetailSidebarProps {
  work: ApiWork;
  previewEdition: any;
  previewItem: any;
  backUrl?: string;
}

const ItemDetailSidebar: React.FC<ItemDetailSidebarProps> = React.memo(
  ({ work, previewEdition, previewItem, backUrl }) => {
    const [cookies] = useCookies([NYPL_SESSION_ID]);
    const loginCookie = cookies[NYPL_SESSION_ID];
    const isLoggedIn = !!loginCookie;

    const authorNames = useMemo(
      () => (work.authors ? work.authors.map((author) => author.name) : []),
      [work.authors]
    );

    const downloadLink = useMemo(
      () =>
        previewEdition
          ? EditionCardUtils.selectDownloadLink(previewEdition)
          : undefined,
      [previewEdition]
    );
    const publisherNames = (previewEdition?.publishers ?? []).map(
      (pubAgent) => pubAgent && pubAgent.name
    );
    return (
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
          <Box>
            {work.authors && work.authors.length > 0 && (
              <AuthorsList authors={work.authors} />
            )}
          </Box>
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
                ariaLabel: "Details",
                label: "Details",
                panel: (
                  <DetailsPanel
                    previewItem={previewItem}
                    previewEdition={previewEdition}
                    publisherNames={publisherNames}
                    work={work}
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
    );
  }
);

ItemDetailSidebar.displayName = "ItemDetailSidebar";

export default ItemDetailSidebar;
