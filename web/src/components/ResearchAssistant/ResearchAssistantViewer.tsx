import { Box } from "@nypl/design-system-react-components";
import React from "react";
import dynamic from "next/dynamic";
const WebReader = dynamic(() => import("@nypl/web-reader"), { ssr: false });

const ResearchAssistantViewer: React.FC<{
  itemId: string;
  pageId: string;
}> = ({ itemId, pageId }) => {
  const protocol =
    typeof window !== "undefined" && window.location.protocol
      ? window.location.protocol
      : "http:";
  const host =
    typeof window !== "undefined" ? window.location.host : "localhost:3000";
  const manifestApiUrl = `${protocol}//${host}/api/manifest/${itemId}?pageId=${pageId}`;

  if (!itemId || !pageId) {
    return (
      <Box height="90%" padding="m">
        <p>No PDF data available.</p>
      </Box>
    );
  }

  return (
    <Box>
      {manifestApiUrl && (
        <WebReader
          headerLeft={<></>}
          webpubManifestUrl={manifestApiUrl}
          pdfWorkerSrc="/pdf-worker/pdf.worker.min.js"
        />
      )}
    </Box>
  );
};

export default ResearchAssistantViewer;
