import { Box, Button } from "@nypl/design-system-react-components";
import React from "react";
import { ItemLink } from "~/src/types/DataModel";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import Link from "../Link/Link";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { useRouter } from "next/router";

interface PreviewLinkProps {
  previewLink: ItemLink;
  workId?: string;
  editionId?: number;
}

const PreviewLink: React.FC<PreviewLinkProps> = ({
  previewLink,
  workId,
  editionId,
}) =>{
    const { page } = useResultPageContext();
    const isResearchAssistant = page === "vra";

    const itemPageUrl =
        workId
            ? {
                pathname: `/item/${workId}`,
                query: editionId ? { featured: editionId } : undefined,
            }
            : "#";

    return (
        <>
            {isResearchAssistant && previewLink && workId ? (
                <Box>
                    <Link
                        to={itemPageUrl}
                        variant="buttonPrimary"
                        width="100%"
                        aria-label="Preview item"
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
