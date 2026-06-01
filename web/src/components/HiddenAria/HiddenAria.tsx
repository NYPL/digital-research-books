import { Box } from "@nypl/design-system-react-components";
import React from "react";

type HiddenAriaProps = {
  children: React.ReactNode;
  ariaLive?: "polite" | "assertive" | "off";
  ariaAtomic?: boolean;
};

const HiddenAria: React.FC<HiddenAriaProps> = ({
  children,
  ariaLive = "polite",
  ariaAtomic = true,
}) => {
  return (
    <Box
      aria-live={ariaLive}
      aria-atomic={ariaAtomic}
      position="absolute"
      width="1px"
      height="1px"
      padding="0"
      margin="-1px"
      overflow="hidden"
      whiteSpace="nowrap"
      borderWidth={0}
      sx={{ clip: "rect(0, 0, 0, 0)" }}
    >
      {children}
    </Box>
  );
};

export default HiddenAria;
