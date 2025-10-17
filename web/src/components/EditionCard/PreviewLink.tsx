import { Box, Button } from "@nypl/design-system-react-components";
import React from "react";
import { ItemLink } from "~/src/types/DataModel";
import { useResultPageContext } from "~/src/context/ResultPageContext";

const PreviewLink: React.FC<{
    previewLink: ItemLink;
}> = ({ previewLink }) => {
    const { onPreview, page } = useResultPageContext();
    const isResearchAssistant = page === "vra";

    const linkText = "Preview";

    return (
        <>
            {isResearchAssistant && previewLink ? (
                <Box>
                    <Button
                        id={`preview-button-${previewLink.link_id}`}
                        onClick={() => onPreview(previewLink.url)}
                        width="100%"
                    >
                        {linkText}
                    </Button>
                </Box>
            ) : (
                <>Not yet available</>
            )}
        </>
    );
};

export default PreviewLink;
