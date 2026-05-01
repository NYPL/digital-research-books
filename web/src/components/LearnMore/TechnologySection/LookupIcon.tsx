import { Icon } from "@nypl/design-system-react-components";
import React from "react";

const LookupIcon: React.FC = () => {
  return (
    <Icon size="2xlarge" color="transparent">
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="38"
        height="42"
        viewBox="0 0 38 42"
        fill="none"
      >
        <path
          d="M29 40C32.866 40 36 36.866 36 33C36 29.134 32.866 26 29 26C25.134 26 22 29.134 22 33C22 36.866 25.134 40 29 40Z"
          fill="#F9E08E"
        />
        <path
          d="M21 19H9M13 27H9M25 11H9M33 18V10.6C33 7.23969 33 5.55953 32.346 4.27606C31.7708 3.14708 30.8529 2.2292 29.7239 1.65396C28.4405 1 26.7603 1 23.4 1H10.6C7.23969 1 5.55953 1 4.27606 1.65396C3.14708 2.2292 2.2292 3.14708 1.65396 4.27606C1 5.55953 1 7.23969 1 10.6V31.4C1 34.7603 1 36.4405 1.65396 37.7239C2.2292 38.8529 3.14708 39.7708 4.27606 40.346C5.55953 41 7.23969 41 10.6 41H16M37 41L34 38M36 33C36 36.866 32.866 40 29 40C25.134 40 22 36.866 22 33C22 29.134 25.134 26 29 26C32.866 26 36 29.134 36 33Z"
          stroke="black"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
    </Icon>
  );
};

export default LookupIcon;
