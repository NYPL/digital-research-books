import { Box, Heading } from "@nypl/design-system-react-components";
import React from "react";
import ResearchAssistantIcon from "../ResearchAssistant/icons/ResearchAssistantIcon";

interface AccordionLabelWithIconProps {
  text: string;
}

const AccordionLabelWithIcon: React.FC<AccordionLabelWithIconProps> = ({
  text,
}) => (
  <Box
    display="flex"
    alignItems="center"
    gap="xs"
    __css={{ svg: { marginInlineStart: "0 !important" } }}
  >
    <ResearchAssistantIcon inCircle />
    <Heading size="heading8" fontWeight="medium" level="h2">
      {text}
    </Heading>
  </Box>
);

export default AccordionLabelWithIcon;
