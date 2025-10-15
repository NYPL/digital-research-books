import { Box, Button } from "@nypl/design-system-react-components";
import React, { useEffect, useState } from "react";
import { itemsReadFetcher } from "~/src/lib/api/ResearchAssistantApi";
import { ApiItemsRead } from "~/src/types/ResearchAssistant";
import Loading from "../Loading/Loading";

const ResearchAssistantViewer: React.FC<{
  itemId: string;
  pdfData: ApiItemsRead;
}> = ({ itemId, pdfData }) => {
  const [currentPdfData, setCurrentPdfData] = useState<ApiItemsRead>(pdfData);
  const [pdfUrl, setPdfUrl] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  // Convert base64-encoded PDF data to blob and create object URL.
  // The <object> element requires a URL to display the PDF,
  // and browsers cannot directly render base64 strings as PDF sources.
  const base64ToBlob = (base64Data, contentType) => {
    const byteCharacters = atob(base64Data);

    const byteArrays = [];

    for (let offset = 0; offset < byteCharacters.length; offset += 512) {
      const slice = byteCharacters.slice(offset, offset + 512),
        byteNumbers = new Array(slice.length);
      for (let i = 0; i < slice.length; i++) {
        byteNumbers[i] = slice.charCodeAt(i);
      }
      const byteArray = new Uint8Array(byteNumbers);

      byteArrays.push(byteArray);
    }
    const blob = new Blob(byteArrays, { type: contentType });

    return blob;
  };

  useEffect(() => {
    const newPdfUrl =
      URL.createObjectURL(
        base64ToBlob(currentPdfData.pageData, "application/pdf")
      ) + "#toolbar=0&navpanes=0&scrollbar=0&view=fitH";
    setPdfUrl(newPdfUrl);
  }, [currentPdfData]);

  const fetchPage = async (pageId) => {
    setIsLoading(true);
    try {
      const nextItemReadResults = await itemsReadFetcher(itemId, pageId);
      setCurrentPdfData(nextItemReadResults.data);
    } catch (error) {
      console.error("Error fetching data:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const onPreviousPageClick = () => {
    fetchPage(
      currentPdfData.previousPages[currentPdfData.previousPages.length - 1]
    );
  };

  const onNextPageClick = () => {
    fetchPage(currentPdfData.nextPages[0]);
  };

  return (
    <Box height="90%" padding="m">
      {isLoading && <Loading />}
      <object width="100%" height="100%" type="application/pdf" data={pdfUrl}>
        <p>Insert your error message here, if the PDF cannot be displayed.</p>
      </object>
      <Box display="flex" flexDir="row" justifyContent="space-between" paddingTop="s">
        <Button id="previous-page-button" onClick={onPreviousPageClick}>
          Previous Page
        </Button>
        <Button id="next-page-button" onClick={onNextPageClick}>
          Next Page
        </Button>
      </Box>
    </Box>
  );
};

export default ResearchAssistantViewer;
