import { VStack } from "@nypl/design-system-react-components";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import { WorkEdition } from "~/src/types/DataModel";

interface SummaryPanelProps {
  previewEdition: WorkEdition;
}

const SummaryPanel: React.FC<SummaryPanelProps> = ({ previewEdition }) => (
  <VStack alignItems="left" gap="xs">
      {previewEdition.summary || "No summary available."}
      <AiGeneratedText />
    </VStack>
);

export default SummaryPanel;
