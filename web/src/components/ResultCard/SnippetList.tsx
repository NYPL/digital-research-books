import { Flex, Pagination, Text } from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useRef, useState } from "react";
import { SNIPPETS_PER_PAGE } from "~/src/constants/researchAssistant";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { Snippet } from "~/src/types/ResearchAssistant";
import Link from "../Link/Link";

interface SnippetListProps {
  snippets: Snippet[];
  workId?: string;
}

const SnippetList: React.FC<SnippetListProps> = ({ snippets, workId }) => {
  const [currentPage, setCurrentPage] = useState(1);
  const { handlePreview } = useResearchAssistant();
  const router = useRouter();

  const firstSnippetRef = useRef<HTMLAnchorElement>(null);
  const totalPages = Math.ceil(snippets.length / SNIPPETS_PER_PAGE);
  const startIndex = (currentPage - 1) * SNIPPETS_PER_PAGE;
  const endIndex = startIndex + SNIPPETS_PER_PAGE;
  const currentSnippets = snippets.slice(startIndex, endIndex);

  const handlePageChange = (page: number) => {
    setCurrentPage(page);
    firstSnippetRef.current?.focus();
  };

  const handlePageLinkClick = (
    e: React.MouseEvent<HTMLAnchorElement>,
    snippet: Snippet
  ) => {
    if (!workId) {
      e.preventDefault();
      const url = `/item/${snippet.item_id}/page/${String(
        snippet.start_page
      ).padStart(8, "0")}`;
      handlePreview(url);
    }
  };

  const createSnippetUrl = (snippet: Snippet) => {
    if (!workId) return "#";

    const query = {
      ...router.query,
      previewItemId: snippet.item_id,
      previewPage: String(snippet.start_page).padStart(8, "0"),
    };

    return {
      pathname: `/item/${workId}`,
      query: query,
    };
  };

  return (
    <Flex flexDir="column" gap="s" marginTop="xs">
      {currentSnippets.map((snippet, index) => (
        <Flex
          key={index}
          flexDir="column"
          gap="xs"
          borderTop="1px dotted"
          borderColor="ui.border.default"
        >
          <Link
            to={createSnippetUrl(snippet)}
            marginTop="xs"
            fontWeight="medium"
            width="fit-content"
            ref={index === 0 ? firstSnippetRef : null}
            onClick={(e) => handlePageLinkClick(e, snippet)}
          >
            Page {snippet.start_page}
          </Link>
          <Text isItalic>&quot;{snippet.text}&quot;</Text>
        </Flex>
      ))}
      {snippets.length > SNIPPETS_PER_PAGE && (
        <Pagination
          pageCount={totalPages}
          initialPage={currentPage}
          onPageChange={(page) => handlePageChange(page)}
        />
      )}
    </Flex>
  );
};

export default SnippetList;
