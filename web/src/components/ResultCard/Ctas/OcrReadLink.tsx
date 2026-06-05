import { Box } from "@nypl/design-system-react-components";
import React from "react";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { ItemLink } from "~/src/types/DataModel";
import Link from "../../Link/Link";

interface OcrReadLinkProps {
  readLink: ItemLink;
  workId?: string;
  editionId?: number;
  title?: string;
}

const OcrReadLink: React.FC<OcrReadLinkProps> = ({
  readLink,
  workId,
  editionId,
  title,
}) => {
  const { page } = useResultPageContext();
  const isEnhancedSearch = page === "vra";

  const itemPageUrl = workId
    ? {
        pathname: `/item/${workId}`,
        query: editionId ? { featured: editionId } : undefined,
      }
    : "#";

  return (
    <>
      {isEnhancedSearch && readLink && workId ? (
        <Box>
          <Link
            to={itemPageUrl}
            variant="buttonPrimary"
            width="100%"
            aria-label={`${title} Read online`}
            bgColor="section.research.secondary"
            _hover={{ bgColor: "section.research.primary" }}
          >
            Read online
          </Link>
        </Box>
      ) : (
        <>Not yet available</>
      )}
    </>
  );
};

export default OcrReadLink;
