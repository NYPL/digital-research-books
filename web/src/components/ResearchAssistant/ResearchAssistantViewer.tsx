import { Box } from "@nypl/design-system-react-components";
import dynamic from "next/dynamic";
import React from "react";
import { trackEvent } from "~/src/lib/gtag/Analytics";
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
  const [isFullViewport, setIsFullViewport] = React.useState(false);
  const manifestApiUrl = `${origin}/api/manifest/${itemId}?pageId=${pageId}`;

  const toggleFullScreen = () => setIsFullViewport((v) => !v);

  if (!itemId || !pageId || !manifestApiUrl) {
    return (
      <Box height="90%" padding="m">
        <p>No PDF data available.</p>
      </Box>
    );
  }

  const handleReaderClick = (e: React.MouseEvent) => {
    const target = e.target as HTMLElement;
    const button = target.closest("button");

    if (button) {
      const actionName =
        button.getAttribute("aria-label") || button.title || button.innerText;
      // GTM Tagging: ereader_function_click
      trackEvent({
        event: "ereader_function_click",
        interaction: "Click",
        element_id: itemId,
        location: "Web Reader",
        metadata_field: "Button",
        metadata_value: actionName,
      });
    }
  };

  return (
    <Box
      margin="0 auto"
      width={isFullViewport ? "100vw" : "auto"}
      height={isFullViewport ? "100vh" : "auto"}
      position={isFullViewport ? "fixed" : "relative"}
      top={isFullViewport ? 0 : undefined}
      left={isFullViewport ? 0 : undefined}
      zIndex={isFullViewport ? 9999 : undefined}
      onClick={handleReaderClick}
    >
      {manifestApiUrl && (
        <WebReader
          key={manifestApiUrl}
          webpubManifestUrl={manifestApiUrl}
          pdfWorkerSrc="/pdf-worker/pdf.worker.min.mjs"
          injectablesFixed={injectables}
          height="70vh"
          toggleFullScreen={toggleFullScreen}
        />
      )}
    </Box>
  );
};

export default ResearchAssistantViewer;
