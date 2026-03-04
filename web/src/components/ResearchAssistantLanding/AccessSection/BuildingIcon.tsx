import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const BuildingIcon: React.FC = () => {
  return (
    <Icon size="3xlarge" color="section.research.secondary">
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="64"
        height="64"
        viewBox="0 0 64 64"
        fill="none"
      >
        <path
          d="M8 55.9999H56M16 47.9999V26.6666M26.6667 47.9999V26.6666M37.3333 47.9999V26.6666M48 47.9999V26.6666M53.3333 18.6666L33.1307 6.03993C32.7205 5.78357 32.5154 5.65539 32.2954 5.60541C32.101 5.56122 31.899 5.56122 31.7046 5.60541C31.4846 5.65539 31.2795 5.78357 30.8693 6.03993L10.6667 18.6666H53.3333Z"
          stroke="#006166"
          strokeWidth="4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default BuildingIcon;
