import { Box } from "@nypl/design-system-react-components";
import React from "react";

interface ChevronIconProps {
  iconRotation: string;
}

const ChevronIcon: React.FC<ChevronIconProps> = ({ iconRotation }) => {
  return (
    <Box height="1rem" width="1rem" transform={iconRotation}>
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="16"
        height="16"
        viewBox="0 0 16 16"
        fill="none"
      >
        <path
          d="M11 8L7 4L3 8"
          stroke="#006166"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Box>
  );
};

export default ChevronIcon;
