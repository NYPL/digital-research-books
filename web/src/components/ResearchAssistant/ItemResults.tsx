import {
  Box,
  Heading,
  Button,
  Text,
} from "@nypl/design-system-react-components";
import { useResearchAssistant } from "~/src/context/ResearchAssistantContext";
import { useResultPageContext } from "~/src/context/ResultPageContext";
import { ItemSearchResults } from "~/src/types/ResearchAssistant";
import ResearchAssistantViewer from "./ResearchAssistantViewer";

const ItemResults: React.FC<{
  results: ItemSearchResults[];
}> = ({ results }) => {
  const { onPreview } = useResultPageContext();
  const { itemId, pdfData, showWebReader } = useResearchAssistant();

  return (
    <Box
      padding="s"
      border="1px solid #e5e7eb"
      overflowY="auto"
      maxHeight="80vh"
      flex="1"
    >
      {showWebReader ? (
        <>
          {pdfData && (
            <ResearchAssistantViewer itemId={itemId} pdfData={pdfData} />
          )}
        </>
      ) : (
        <>
          <Heading level="h2" size="heading4">
            Results within the book
          </Heading>
          <Box display="flex" flexDir="column" gap="m">
            {results.map((item, index) => (
              <Box
                key={index}
                padding="s"
                border="1px solid #ccc"
                borderRadius="base"
              >
                <Text>{item.textPreview}</Text>
                <Box as="ul" paddingLeft="l">
                  {item.highlightedText && item.highlightedText.map((text, textIndex) => (
                    <Text key={textIndex} as="li">
                      {text}
                    </Text>
                  ))}
                </Box>
                <Button
                  onClick={() => onPreview(item.readLink)}
                  id={`read-preview-${index}`}
                >
                  Go to Page
                </Button>
              </Box>
            ))}
          </Box>
        </>
      )}
    </Box>
  );
};

export default ItemResults;
