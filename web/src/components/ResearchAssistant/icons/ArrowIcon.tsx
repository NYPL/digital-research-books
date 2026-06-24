import { Icon, type IconRotations } from "@nypl/design-system-react-components";
import React from "react";

export type ArrowDirection = "right" | "down" | "left" | "up";

const DIRECTION_TO_ROTATION: Record<ArrowDirection, IconRotations> = {
  right: "rotate0",
  down: "rotate90",
  left: "rotate180",
  up: "rotate270",
};

interface ArrowIconProps {
  direction?: ArrowDirection;
  color?: string;
}

const ArrowIcon: React.FC<ArrowIconProps> = ({
  direction = "right",
  color = "ui.white",
}) => {
  return (
    <Icon
      size="medium"
      iconRotation={DIRECTION_TO_ROTATION[direction]}
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
