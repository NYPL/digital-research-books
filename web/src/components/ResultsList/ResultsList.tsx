import React from "react";
import Link from "~/src/components/Link/Link";
import { Box, Heading, VStack } from "@nypl/design-system-react-components";
import { EditionCard } from "~/src/components/EditionCard/EditionCard";
import EditionCardUtils from "~/src/util/EditionCardUtils";
import EmptySearchSvg from "../Svgs/EmptySearchSvg";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import { MAX_TITLE_LENGTH } from "~/src/constants/editioncard";
import { ApiWork } from "~/src/types/WorkQuery";
import ViewEditionsLink from "../EditionCard/ViewEditionsLink";
import AuthorsList from "../AuthorsList/AuthorsList";

const ResultsList: React.FC<{ works: ApiWork[] }> = ({ works }) => {
  if (works.length === 0) {
    return (
      <div className="no-results">
        <EmptySearchSvg className="no-results-item" />
        <div className="no-results-item">
          No results were found. Please try a different keyword or fewer
          filters.
        </div>
      </div>
    );
  }
  return (
    <VStack align="left" spacing="m">
      {works.map((work) => {
        const previewEdition = work.editions && work.editions[0];

        return (
          <Box key={`search-result-${work.uuid}`} className="search-result">
            <Heading
              level="h2"
              size="heading4"
              marginBottom="xs"
              sx={{
                a: {
                  textDecoration: "none",
                },
              }}
            >
              <Link
                to={{
                  pathname: `/work/${work.uuid}`,
                  query: { featured: previewEdition.edition_id },
                }}
              >
                {truncateStringOnWhitespace(work.title, MAX_TITLE_LENGTH)}
              </Link>
            </Heading>
            <span>{EditionCardUtils.getSubtitle(work.sub_title)}</span>
            {work.authors && work.authors.length > 0 && (
              <Box marginBottom="xs">
                By <AuthorsList authors={work.authors} />
              </Box>
            )}
            <EditionCard
              authors={work.authors}
              edition={previewEdition}
              title={work.title}
              isFeaturedEdition={work.edition_count > 1}
            />
            <ViewEditionsLink work={work} />
          </Box>
        );
      })}
    </VStack>
  );
};

export default ResultsList;
