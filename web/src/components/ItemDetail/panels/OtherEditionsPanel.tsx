import { Box, Text } from "@nypl/design-system-react-components";
import { ApiWork } from "~/src/types/WorkQuery";
import EditionLinks from "../../ResultCard/EditionLinks";

interface OtherEditionsPanelProps {
  work: ApiWork;
}

const OtherEditionsPanel: React.FC<OtherEditionsPanelProps> = ({ work }) => (
  <Box>
    {work.editions.length > 1 ? (
      <EditionLinks work={work} />
    ) : (
      <Text>No other editions available.</Text>
    )}
  </Box>
);

export default OtherEditionsPanel;
