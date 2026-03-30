import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const BuildingIcon: React.FC = () => {
  return (
    <Icon
      size="2xlarge"
      // @ts-expect-error: Override color value type
      color="#F9E08E"
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="48"
        height="48"
        viewBox="0 0 48 48"
        fill="none"
      >
        <path
          d="M23.152 4.52995C23.4596 4.33768 23.6135 4.24154 23.7784 4.20406C23.9243 4.17091 24.0757 4.17091 24.2216 4.20406C24.3865 4.24154 24.5404 4.33768 24.848 4.52995L40 14H24H8L23.152 4.52995Z"
          fill="#F9E08E"
        />
        <path
          d="M6 42H42M12 36V20M20 36V20M28 36V20M36 36V20M40 14L24.848 4.52995C24.5404 4.33768 24.3865 4.24154 24.2216 4.20406C24.0757 4.17091 23.9243 4.17091 23.7784 4.20406C23.6135 4.24154 23.4596 4.33768 23.152 4.52995L8 14H24H40Z"
          stroke="black"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default BuildingIcon;
