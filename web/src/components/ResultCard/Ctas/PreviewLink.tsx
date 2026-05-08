import { Box } from "@nypl/design-system-react-components";
import React from "react";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { trackEvent } from "~/src/lib/gtag/Analytics";
import { ItemLink } from "~/src/types/DataModel";
import Link from "../../Link/Link";

interface PreviewLinkProps {
  previewLink: ItemLink;
  workId?: string;
  editionId?: number;
}

const PreviewLink: React.FC<PreviewLinkProps> = ({
  previewLink,
  workId,
  editionId,
}) => {
  const { page } = useResultPageContext();
  const isResearchAssistant = page === "vra";

  const itemPageUrl = workId
    ? {
        pathname: `/item/${workId}`,
        query: editionId ? { featured: editionId } : undefined,
      }
    : "#";

  const handleClick = () => {
    // GTM Tagging: itemcard_read_click
    trackEvent({
      event: "itemcard_read_click",
      interaction: "Click",
      metadata_field: "Type",
      metadata_value: "Preview",
    });
  };

  return (
    <>
      {isResearchAssistant && previewLink && workId ? (
        <Box>
          <Link
            to={itemPageUrl}
            onClick={handleClick}
            variant="buttonPrimary"
            width="100%"
            aria-label="Preview item"
            bgColor="section.research.secondary"
            _hover={{ bgColor: "section.research.primary" }}
          >
            Preview
          </Link>
        </Box>
      ) : (
        <>Not yet available</>
      )}
    </>
  );
};

export default PreviewLink;
