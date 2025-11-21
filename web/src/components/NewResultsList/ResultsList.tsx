import React from "react";
import {
  Box,
  Flex,
  Heading,
  Icon,
  VStack,
} from "@nypl/design-system-react-components";
import EmptySearchSvg from "../Svgs/EmptySearchSvg";
import ResultCard from "../ResultCard/ResultCard";
import { ApiWork } from "~/src/types/WorkQuery";

// TODO: rename folder to ResultsList when we switch to VRA/Keyword search
const ResultsList: React.FC<{ works: ApiWork[] }> = ({ works }) => {
  if (works.length === 0) {
    return (
      <Flex gap="s" bgColor="ui.bg.default" alignItems="center">
        <Flex
          alignItems="center"
          flex="1"
          flexDir="column"
          gap="xs"
          height="100%"
          margin="0 auto"
          maxWidth="1280px"
          padding="xl"
        >
          <Icon
            color="section.research.secondary"
            name="search"
            size="xlarge"
          />
          <Heading
            color="section.research.secondary"
            size="heading6"
            textAlign="center"
          >
            No results found. Try a different topic.
          </Heading>
        </Flex>
      </Flex>
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
