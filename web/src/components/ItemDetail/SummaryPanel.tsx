import { Flex, Text, VStack } from "@nypl/design-system-react-components";
import { WorkEdition } from "~/src/types/DataModel";
import AiGeneratedText from "../AiGeneratedText/AiGeneratedText";
import FeedbackButtons from "../ResearchAssistant/FeedbackButtons";

interface SummaryPanelProps {
  previewEdition: WorkEdition;
}

const SummaryPanel: React.FC<SummaryPanelProps> = ({ previewEdition }) => {
  return (
    <VStack alignItems="left" gap="xs">
      <Text>{previewEdition.summary || "No summary available."}</Text>
      <Flex alignItems="center" justifyContent="space-between">
        <AiGeneratedText />
        <FeedbackButtons />
      </Flex>
    </VStack>
  );
};

export default SummaryPanel;
