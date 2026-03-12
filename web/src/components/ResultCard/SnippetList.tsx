import { Flex, Pagination, Text } from "@nypl/design-system-react-components";
import React, { useRef, useState } from "react";
import { Snippet } from "~/src/types/ResearchAssistant";
import Link from "../Link/Link";

interface SnippetListProps {
  snippets: Snippet[];
  workId?: string;
}

const SnippetList: React.FC<SnippetListProps> = ({ snippets, workId }) => {
  const [currentPage, setCurrentPage] = useState(1);
  const snippetsPerPage = 6;
  const firstSnippetRef = useRef<HTMLAnchorElement>(null);

  const totalPages = Math.ceil(snippets.length / snippetsPerPage);

  const startIndex = (currentPage - 1) * snippetsPerPage;
  const endIndex = startIndex + snippetsPerPage;
  const currentSnippets = snippets.slice(startIndex, endIndex);

  const handlePageChange = (page: number) => {
    setCurrentPage(page);
    firstSnippetRef.current.focus();
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
            to={`/item/${workId}?previewItemId=${
              snippet.item_id
            }&previewPage=${String(snippet.start_page).padStart(8, "0")}`}
            marginTop="xs"
            fontWeight="medium"
            ref={index === 0 ? firstSnippetRef : null}
          >
            Page {snippet.start_page}
          </Link>
          <Text isItalic>&quot;{snippet.text}&quot;</Text>
        </Flex>
      ))}
      {snippets.length > snippetsPerPage && (
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
