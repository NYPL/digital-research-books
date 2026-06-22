import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const iconRotationsArray = [
  "rotate0",
  "rotate90",
  "rotate180",
  "rotate270",
] as const;

interface ArrowIconProps {
  iconRotation?: typeof iconRotationsArray[number];
  color?: string;
}

const ArrowIcon: React.FC<ArrowIconProps> = ({
  iconRotation = "rotate0",
  color = "ui.white",
}) => {
  return (
    <Icon size="medium" iconRotation={iconRotation} fill={color}>
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
