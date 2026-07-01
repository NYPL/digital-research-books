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
  const [isFullViewport, setIsFullViewport] = React.useState(false);
  const manifestApiUrl = `${origin}/api/manifest/${itemId}?pageId=${pageId}`;

  const toggleFullScreen = () => setIsFullViewport((v) => !v);
  const getContent = React.useCallback(async (href: string) => {
    const response = await fetch(href, {
      mode: "cors",
      // TODO: Once signed cookies are implemented, uncomment the following line:
      // credentials: 'include',
    });

    if (!response.ok) {
      throw new Error(
        `Failed to fetch content from ${href}: ${response.statusText}`
      );
    }

    return new Uint8Array(await response.arrayBuffer());
  }, []);

  if (!itemId || !pageId || !manifestApiUrl) {
    return (
      <Box height="90%" padding="m">
        <p>No PDF data available.</p>
      </Box>
    );
  }

  return (
    <Box
      margin="0 auto"
      width={isFullViewport ? "100vw" : "auto"}
      height={isFullViewport ? "100vh" : "auto"}
      position={isFullViewport ? "fixed" : "relative"}
      top={isFullViewport ? 0 : undefined}
      left={isFullViewport ? 0 : undefined}
      zIndex={isFullViewport ? 9999 : undefined}
    >
      {manifestApiUrl && (
        <WebReader
          key={manifestApiUrl}
          webpubManifestUrl={manifestApiUrl}
          pdfWorkerSrc="/pdf-worker/pdf.worker.min.mjs"
          injectablesFixed={injectables}
          height="70vh"
          toggleFullScreen={toggleFullScreen}
          getContent={getContent}
        />
      )}
    </Box>
  );
};

export default ResearchAssistantViewer;
