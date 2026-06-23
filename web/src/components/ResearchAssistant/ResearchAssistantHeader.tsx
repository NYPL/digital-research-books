import { Box } from "@nypl/design-system-react-components";
import {
  getPanelLayout,
  HEADER_HEIGHT,
  PADDING_COUNTER,
} from "~/src/constants/researchAssistant";

const ResearchAssistantHeader: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const { marginX, marginRight } = getPanelLayout();
  return (
    <Box
      bgColor="section.research.primary"
      display="flex"
      justifyContent="space-between"
      alignItems="center"
      borderBottom="1px white solid"
      borderRadius={{ base: "8px 8px 0 0", md: "0" }}
      marginLeft={marginX}
      marginRight={marginRight}
      paddingLeft="s"
      paddingRight={`calc(${PADDING_COUNTER} * 2)`}
      position="sticky"
      paddingY="s"
      top="0"
      zIndex="999"
      height={HEADER_HEIGHT}
    >
      {children}
    </Box>
  );
};

export default ResearchAssistantHeader;
