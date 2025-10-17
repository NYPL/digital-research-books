import React from "react";
import { Box, VStack } from "@nypl/design-system-react-components";
import EmptySearchSvg from "../Svgs/EmptySearchSvg";
import ResultCard from "../ResultCard/ResultCard";
import { ApiWork } from "~/src/types/WorkQuery";

// TODO: rename folder to ResultsList when we switch to VRA/Keyword search
const ResultsList: React.FC<{ works: ApiWork[] }> = ({ works }) => {
  if (works.length === 0) {
    return (
      <Box>
        <EmptySearchSvg />
        <Box>
          No results were found.
        </Box>
      </Box>
    );
  }
  return (
    <VStack align="left" spacing="s">
      {works.map((work) => {
        const previewEdition = work.editions && work.editions[0];

        return (
          <Box key={`search-result-${work.uuid}`} className="search-result">
            <ResultCard
              authors={work.authors}
              edition={previewEdition}
              work={work}
              isFeaturedEdition={work.edition_count > 1}
            />
          </Box>
        );
      })}
    </VStack>
  );
};

export default ResultsList;
