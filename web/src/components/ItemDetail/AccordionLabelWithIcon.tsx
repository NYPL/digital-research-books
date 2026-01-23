import { Box } from "@nypl/design-system-react-components";
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
    <span>{text}</span>
  </Box>
);

export default AccordionLabelWithIcon;
