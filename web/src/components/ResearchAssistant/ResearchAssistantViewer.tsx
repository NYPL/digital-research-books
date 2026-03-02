import { Box } from "@nypl/design-system-react-components";
import dynamic from "next/dynamic";
import React from "react";
const WebReader = dynamic(() => import("@nypl/web-reader"), { ssr: false });

const origin =
  typeof window !== "undefined" && window.location?.origin
    ? window.location.origin
    : "";

const injectables = [
  {
    type: "style",
    url: `${origin}${"/ReadiumCSS/ReadiumCSS-before.css"}`,
  },
  {
    type: "style",
    url: `${origin}${"/ReadiumCSS/ReadiumCSS-default.css"}`,
  },
  {
    type: "style",
    url: `${origin}${"/ReadiumCSS/ReadiumCSS-after.css"}`,
  },
  {
    type: "style",
    url: `${origin}/fonts/opendyslexic/opendyslexic.css`,
    fontFamily: "opendyslexic",
  },
];

const ResearchAssistantViewer: React.FC<{
  itemId: string;
  pageId: string;
}> = ({ itemId, pageId }) => {
  const manifestApiUrl = `${origin}/api/manifest/${itemId}?pageId=${pageId}`;

  if (!itemId || !pageId || !manifestApiUrl) {
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
          webpubManifestUrl={manifestApiUrl}
          pdfWorkerSrc="/pdf-worker/pdf.worker.min.mjs"
          injectablesFixed={injectables}
        />
      )}
    </Box>
  );
};

export default ResearchAssistantViewer;
