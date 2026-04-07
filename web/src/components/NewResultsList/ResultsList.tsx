import { Box, VStack } from "@nypl/design-system-react-components";
import React from "react";
import { RESULT_TEST_ID } from "~/src/constants/testIds";
import { Agent } from "~/src/types/DataModel";
import { CatalogEdition } from "~/src/types/ResearchAssistant";
import { ApiWork } from "~/src/types/WorkQuery";
import ResultCard from "../ResultCard/ResultCard";

// TODO: rename folder to ResultsList when we switch to VRA/Keyword search
const ResultsList: React.FC<{ works: ApiWork[] | CatalogEdition[] }> = ({
  works,
}) => {
  if (!works || works.length === 0) {
    return null;
  }

  const isCatalogEditions = (arr: any[]): arr is CatalogEdition[] =>
    arr.length > 0 && typeof arr[0] === "object" && "work_uuid" in arr[0];

  if (isCatalogEditions(works)) {
    const workMap = new Map<
      string,
      {
        uuid?: string;
        title?: string;
        authors?: Agent[];
        editions: CatalogEdition[];
      }
    >();

    for (const ed of works) {
      const key = ed.work_uuid;
      const wk = workMap.get(key) ?? {
        uuid: ed.work_uuid,
        title: ed.work_title ?? ed.title,
        authors: ed.work_authors ?? [],
        editions: [] as CatalogEdition[],
      };
      wk.editions.push(ed);
      workMap.set(key, wk);
    }

    const groupedWorks = Array.from(workMap.values());
    return (
      <VStack align="left" spacing="s">
        {groupedWorks.map((work) => {
          const previewEdition = work.editions && work.editions[0];

          return (
            <Box
              key={`search-result-${work.uuid}`}
              className="search-result"
              data-testid={RESULT_TEST_ID}
            >
              <ResultCard
                authors={work.authors}
                edition={previewEdition}
                work={{
                  uuid: work.uuid,
                  title: work.title,
                  editions: work.editions,
                  edition_count: work.editions.length,
                }}
                isFeaturedEdition={work.editions.length > 1}
              />
            </Box>
          );
        })}
      </VStack>
    );
  }

  const apiWorks = works as ApiWork[];
  return (
    <VStack align="left" spacing="s">
      {apiWorks.map((work) => {
        const previewEdition = work.editions && work.editions[0];

        return (
          <Box
            key={`search-result-${work.uuid}`}
            className="search-result"
            data-testid={RESULT_TEST_ID}
          >
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
