import { Icon, type IconRotations, useNYPLBreakpoints } from "@nypl/design-system-react-components";
import React from "react";

export type ArrowDirection = "right" | "down" | "left" | "up";

type ResponsiveArrowDirection = {
  base: ArrowDirection;
  sm?: ArrowDirection;
  md?: ArrowDirection;
  lg?: ArrowDirection;
  xl?: ArrowDirection;
  "2xl"?: ArrowDirection;
};

const DIRECTION_TO_ROTATION: Record<ArrowDirection, IconRotations> = {
  right: "rotate0",
  down: "rotate90",
  left: "rotate180",
  up: "rotate270",
};

interface ArrowIconProps {
  direction?: ArrowDirection | ResponsiveArrowDirection;
  color?: string;
}

const ArrowIcon: React.FC<ArrowIconProps> = ({
  direction = "right",
  color = "ui.white",
}) => {
  const { isLargerThanMedium, isLargerThanLarge, isLargerThanXLarge } = useNYPLBreakpoints();
  const responsiveDirection =
    typeof direction === "string" ? { base: direction } : direction;
  
  let resolvedDirection = responsiveDirection.base;
  
  if (isLargerThanXLarge && responsiveDirection["2xl"]) {
    resolvedDirection = responsiveDirection["2xl"];
  } else if (isLargerThanLarge && responsiveDirection.xl) {
    resolvedDirection = responsiveDirection.xl;
  } else if (isLargerThanLarge && responsiveDirection.lg) {
    resolvedDirection = responsiveDirection.lg;
  } else if (isLargerThanMedium && responsiveDirection.md) {
    resolvedDirection = responsiveDirection.md;
  }

  return (
    <Icon
      size="medium"
      iconRotation={DIRECTION_TO_ROTATION[resolvedDirection]}
      fill={color}
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="18"
        height="18"
        viewBox="0 0 18 18"
        fill="none"
        style={{ display: "block" }}
      >
        <path
          d="M3.75 9H14.25M14.25 9L9 3.75M14.25 9L9 14.25"
          stroke={color}
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default ArrowIcon;
