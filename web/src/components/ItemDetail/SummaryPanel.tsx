import { Text, VStack } from "@nypl/design-system-react-components";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import { WorkEdition } from "~/src/types/DataModel";

interface SummaryPanelProps {
  previewEdition: WorkEdition;
}

const SummaryPanel: React.FC<SummaryPanelProps> = ({ previewEdition }) => {
  return (
  <VStack alignItems="left" gap="xs">
      <Text>{previewEdition.summary || "No summary available."}</Text>
      <AiGeneratedText />
    </VStack>
)
};

export default SummaryPanel;
