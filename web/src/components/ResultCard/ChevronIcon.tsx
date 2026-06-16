import { Flex } from "@nypl/design-system-react-components";
import React from "react";

interface ChevronIconProps {
  iconRotation: string;
}

const ChevronIcon: React.FC<ChevronIconProps> = ({ iconRotation }) => {
  return (
    <Flex transform={iconRotation}>
      <svg
        width="16"
        height="16"
        viewBox="0 0 16 16"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <path
          d="M12 10L8 6L4 10"
          stroke="#006166"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Flex>
  );
};

export default ChevronIcon;
