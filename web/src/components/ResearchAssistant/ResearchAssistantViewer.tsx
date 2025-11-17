import { Box } from "@nypl/design-system-react-components";
import React from "react";
import dynamic from "next/dynamic";
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
  const protocol =
    typeof window !== "undefined" && window.location.protocol
      ? window.location.protocol
      : "http:";
  const host =
    typeof window !== "undefined" ? window.location.host : "localhost:3000";
  const manifestApiUrl = `${protocol}//${host}/api/manifest/${itemId}?pageId=${pageId}`;

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
          headerLeft={<></>}
          webpubManifestUrl={manifestApiUrl}
          pdfWorkerSrc="/pdf-worker/pdf.worker.min.js"
          injectablesFixed={injectables}
        />
      )}
    </Box>
  );
};

export default ResearchAssistantViewer;
